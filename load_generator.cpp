/*
 * CS3339 Fall 2026 — STUDENT STARTER (INTENTIONALLY HALF-WRITTEN)
 * ============================================================================
 * PURPOSE
 *   Generate simulated multiplayer traffic to benchmark the companion
 *   game_server.cpp. One simulated player = one nonblocking TCP connection.
 *   A single epoll event loop handles all clients; no thread per player.
 *
 * WHAT IS IMPLEMENTED
 *   CLI parsing and validation; 16-byte request encoding; DNS resolution;
 *   socket/epoll setup; nonblocking player connections; control-socket START;
 *   connection cleanup; basic counters, scheduling data types, percentile helper.
 *
 * WHAT YOU COMPLETE
 *   Nine marked TODO functions: flushing partial sends, scheduling requests,
 *   reassembling responses, validating acknowledgments, reading control replies,
 *   dispatching epoll events, rate pacing, draining ACKs, and writing CSV results.
 *
 * IMPORTANT
 *   This file COMPILES, but a full benchmark WILL NOT RUN until the TODOs are
 *   implemented. The TODO methods deliberately throw std::logic_error instead
 *   of silently producing incorrect experimental measurements.
 *
 * WIRE PROTOCOL (matches game_server.cpp)
 *   Request, 16 bytes:  player_id | sequence | x | y
 *   Response, 16 bytes: player_id | sequence | status | reserved
 *   Each field is a 4-byte unsigned integer in network (big-endian) order.
 *   status = 0 for processed; status = 1 for queue rejection.
 *   TCP bytes can arrive in arbitrary chunks: never assume recv() yields a
 *   complete 16-byte response, or that send() transmits all 16 bytes.
 *
 * BUILD: g++ -std=c++17 -O2 -Wall -Wextra -Wpedantic -pthread \
 *            load_generator.cpp -o load_generator
 * HELP:  ./load_generator --help
 *
 * Recommended order: TODO 6 (epoll dispatch), TODO 1–5 (network handling),
 * TODO 7 (pacing), TODO 8 (draining), and TODO 9 (results reporting).
 */

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <deque>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <fstream>
#include <limits>
#include <memory>
#include <netdb.h>
#include <netinet/tcp.h>
#include <queue>
#include <stdexcept>
#include <string>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>


 // sets up the clock and frame sizes used throughout the load generator
using Clock = std::chrono::steady_clock;
static constexpr size_t FRAME_SIZE = 16;
static constexpr int MAX_EVENTS = 2048;

// gets the current steady timestamp in nanoseconds for timing requests and responses
static int64_t ns_now() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        Clock::now().time_since_epoch()).count();
}

// reads a four byte number from a network response and converts it to the local byte order
static uint32_t get_u32(const uint8_t *p) {
    uint32_t net; std::memcpy(&net, p, 4); return ntohl(net);
}

// writes a four byte number into a network request in network byte order
static void put_u32(uint8_t *p, uint32_t value) {
    uint32_t net = htonl(value); std::memcpy(p, &net, 4);
}

// builds a player movement request using the player id sequence number and sample coordinates
[[maybe_unused]] static std::array<uint8_t, FRAME_SIZE> encode(uint32_t id, uint32_t seq) {
    std::array<uint8_t, FRAME_SIZE> a{};
    put_u32(a.data(), id); put_u32(a.data() + 4, seq);
    put_u32(a.data() + 8, seq % 10000); put_u32(a.data() + 12, (seq * 3U) % 10000);
    return a;
}

// allows a socket to be used without waiting for each read or write to finish
static void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
        throw std::runtime_error("fcntl failed");
}

// stores the connection settings number of players message rate and benchmark timings
struct Config {
    std::string host = "127.0.0.1";
    std::string token = "local-benchmark";
    std::string csv = "client.csv";
    int port = 9000, control_port = 9001, workers = 4;
    int players = 1000, max_inflight = 32;
    double rate = 60, warmup = 5, duration = 30, drain = 3, stop_wait = 60;
};







// OUTPUT

// prints the command line options that can be used to configure the load generator
static void help() {
    std::cout << "Usage: load_generator [options]\n"
              << "  --host HOST            Server IPv4 address or hostname\n"
              << "  --port N               Game TCP port (default 9000)\n"
              << "  --control-port N       Control TCP port (default port+1)\n"
              << "  --control-token S      Same token as game_server\n"
              << "  --workers N            Server worker count (CSV label only)\n"
              << "  --players N            Number of TCP player connections (default 1000)\n"
              << "  --rate HZ              Messages/second/player (default 60)\n"
              << "  --warmup SEC           Warm-up seconds (default 5)\n"
              << "  --duration SEC         Measurement seconds (default 30)\n"
              << "  --drain SEC            Initial acknowledgment drain (default 3)\n"
              << "  --stop-wait SEC        Extra seconds awaiting server report (default 60)\n"
              << "  --max-inflight N       Max outstanding messages/player (default 32)\n"
              << "  --csv FILE             One-row CSV output (default client.csv)\n";
}

// reads the command line settings and rejects missing or invalid values
static Config parse_args(int argc, char **argv) {
    Config c; bool explicit_control_port = false;
    for (int i = 1; i < argc; ++i) {
        std::string key = argv[i];
        if (key == "--help") { help(); std::exit(0); }
        if (i + 1 >= argc) throw std::runtime_error("Missing value for " + key);
        std::string v = argv[++i];
        if (key == "--host") c.host = v;
        else if (key == "--port") c.port = std::stoi(v);
        else if (key == "--control-port") {c.control_port = std::stoi(v); explicit_control_port = true;}
        else if (key == "--control-token") c.token = v;
        else if (key == "--workers") c.workers = std::stoi(v);
        else if (key == "--players") c.players = std::stoi(v);
        else if (key == "--rate") c.rate = std::stod(v);
        else if (key == "--warmup") c.warmup = std::stod(v);
        else if (key == "--duration") c.duration = std::stod(v);
        else if (key == "--drain") c.drain = std::stod(v);
        else if (key == "--stop-wait") c.stop_wait = std::stod(v);
        else if (key == "--max-inflight") c.max_inflight = std::stoi(v);
        else if (key == "--csv") c.csv = v;
        else throw std::runtime_error("Unknown argument: " + key);
    }
    if (!explicit_control_port) c.control_port = c.port + 1;
    if (c.players < 1 || c.players > 65535 || c.rate <= 0 || c.rate > 10000 ||
        c.duration <= 0 || c.warmup < 0 || c.drain < 0 || c.stop_wait < 0 || c.max_inflight < 1 ||
        c.port < 1 || c.port > 65535 || c.control_port < 1 || c.control_port > 65535 ||
        c.workers < 1 || c.token.empty()) throw std::runtime_error("Invalid options");
    return c;
}

// finds the ipv4 address for a server hostname and port so a socket can connect to it
static sockaddr_in resolve_ipv4(const std::string &hostname, int port) {
    addrinfo hints{}; hints.ai_family = AF_INET; hints.ai_socktype = SOCK_STREAM;
    addrinfo *result = nullptr;
    std::string portstr = std::to_string(port);
    int ret = getaddrinfo(hostname.c_str(), portstr.c_str(), &hints, &result);
    if (ret != 0) throw std::runtime_error("resolve " + hostname + ": " + gai_strerror(ret));
    sockaddr_in addr = *reinterpret_cast<sockaddr_in*>(result->ai_addr);
    freeaddrinfo(result);
    return addr;
}

// tracks a request that has been sent but has not received an acknowledgement
struct Pending { uint32_t seq; int64_t sent_ns; bool measured; };

// stores connection details buffered messages and unanswered requests for one simulated player
struct Client {
    int fd = -1;
    uint32_t id = 0, next_seq = 1;
    bool connected = false, failed = false, write_enabled = false;
    bool partial = false;
    std::array<uint8_t, FRAME_SIZE> pending_frame{};
    size_t pending_sent = 0;
    uint32_t pending_seq = 0;
    std::array<uint8_t, FRAME_SIZE> in{};
    size_t in_used = 0;
    std::unordered_map<uint32_t, Pending> outstanding;
};

// stores the next scheduled send time for a player
struct Due {
    int64_t ns;
    size_t index;

    // compares send times so the earliest scheduled update can be handled first
    bool operator>(const Due &other) const { return ns > other.ns; }
};

// manages the simulated players and coordinates network traffic for the benchmark
class Generator {
public:

    // prepares the server addresses event loop and storage for the simulated players
    explicit Generator(Config cfg) : cfg_(std::move(cfg)) {
        addr_ = resolve_ipv4(cfg_.host, cfg_.port);
        control_addr_ = resolve_ipv4(cfg_.host, cfg_.control_port);
        epfd_ = epoll_create1(EPOLL_CLOEXEC);
        if (epfd_ < 0) throw std::runtime_error("epoll_create1 failed");
        clients_.reserve(cfg_.players);
    }

    // closes the player connections and releases the control and event loop sockets
    ~Generator() {
        for (auto &p : clients_) if (p.fd >= 0) close(p.fd);
        if (controlfd_ >= 0) close(controlfd_);
        if (epfd_ >= 0) close(epfd_);
    }

    // connects the players runs warmup and measurement then gathers and saves the results
    void run() {
        connect_clients();
        connect_control();
        std::cerr << "Connected " << connected_ << " players; warmup=" << cfg_.warmup
                  << "s measure=" << cfg_.duration << "s, rate=" << cfg_.rate << " Hz\n";

        // runs the warmup before opening the measured benchmark window
        if (cfg_.warmup > 0) run_phase(cfg_.warmup, false);
        control_start();
        measurement_start_ns_ = ns_now();
        run_phase(cfg_.duration, true);
        measurement_stop_ns_ = ns_now();

        // stops the timed trial then continues collecting any responses still arriving
        send_control("STOP " + cfg_.token + "\n");
        drain_responses(cfg_.drain);

        // keeps servicing the sockets while waiting for the server to finish its report
        const int64_t stop_wait_deadline = ns_now() + static_cast<int64_t>(cfg_.stop_wait * 1e9);
        while (!control_done_ && ns_now() < stop_wait_deadline) process_events(50);
        if (!control_done_) std::cerr << "WARNING: timed out waiting for server's DONE.\n";
        save_results();
    }

private:
    // stores the active connections benchmark counters and timing information
    Config cfg_;
    sockaddr_in addr_{}, control_addr_{};
    int epfd_ = -1, controlfd_ = -1;
    size_t connected_ = 0;

    // keeps the simulated player states and a lookup from socket to player
    std::vector<Client> clients_;
    std::unordered_map<int, size_t> fd_to_index_;
    std::string control_in_;
    bool control_done_ = false;
    int64_t measurement_start_ns_ = 0, measurement_stop_ns_ = 0;

    // tracks how many updates were scheduled sent accepted or rejected during measurement
    uint64_t offered_ = 0, sent_ = 0, ack_ok_ = 0, ack_rejected_ = 0;
    uint64_t rate_limited_ = 0, protocol_errors_ = 0, socket_failures_ = 0;

    // stores successful request round trip times for the final latency statistics
    std::vector<double> latencies_ms_;

    // registers a socket with the event loop so the generator can monitor its activity
    void ep_add(int fd, uint32_t e) {
        epoll_event ev{}; ev.data.fd = fd; ev.events = e;
        if (epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) != 0)
            throw std::runtime_error(std::string("epoll_ctl add: ") + strerror(errno));
    }

    // changes which read write and disconnect events the generator watches for on a socket
    void ep_mod(int fd, uint32_t e) {
        epoll_event ev{}; ev.data.fd = fd; ev.events = e;
        if (epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ev) != 0)
            throw std::runtime_error(std::string("epoll_ctl mod: ") + strerror(errno));
    }

    // watches for writable events only when a connection or partial send still needs attention
    void update_interest(Client &c) {
        bool wants_write = !c.connected || c.partial;
        if (wants_write != c.write_enabled) {
            c.write_enabled = wants_write;
            ep_mod(c.fd, EPOLLIN | EPOLLRDHUP | (wants_write ? static_cast<uint32_t>(EPOLLOUT) : 0U));
        }
    }

    // opens a nonblocking tcp connection for each simulated player and waits for them to connect
    void connect_clients() {
        // creates one socket and player record for each requested simulated connection
        for (int i = 0; i < cfg_.players; ++i) {
            int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
            if (fd < 0) throw std::runtime_error("socket failed: increase ulimit -n");
            int one = 1; setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
            int rc = connect(fd, reinterpret_cast<sockaddr*>(&addr_), sizeof(addr_));
            if (rc < 0 && errno != EINPROGRESS) {
                std::string error = strerror(errno); close(fd);
                throw std::runtime_error("client connect: " + error);
            }
            Client c; c.fd = fd; c.id = static_cast<uint32_t>(i+1);
            c.connected = (rc == 0); c.write_enabled = !c.connected;
            clients_.push_back(c);
            fd_to_index_[fd] = clients_.size()-1;
            ep_add(fd, EPOLLIN | EPOLLRDHUP | (c.write_enabled ? static_cast<uint32_t>(EPOLLOUT) : 0U));
            if (c.connected) ++connected_;
        }

        // checks that every player connects before the connection deadline
        int64_t deadline = ns_now() + 30LL * 1000000000LL;
        while (connected_ < clients_.size() && ns_now() < deadline) {
            process_events(100);
            if (socket_failures_ > 0)
                throw std::runtime_error("One or more connections failed; see errors; check ulimit -n and server backlog");
        }
        if (connected_ != clients_.size())
            throw std::runtime_error("Timed out connecting all players; check server limit and network");
    }

    // opens a separate tcp connection for sending benchmark commands to the server
    void connect_control() {
        controlfd_ = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (controlfd_ < 0) throw std::runtime_error("control socket failed");
        timeval tv{}; tv.tv_sec = 10;
        setsockopt(controlfd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        setsockopt(controlfd_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        if (connect(controlfd_, reinterpret_cast<sockaddr*>(&control_addr_), sizeof(control_addr_)) != 0)
            throw std::runtime_error(std::string("control connect: ") + strerror(errno));
    }

    // sends a complete command over the benchmark control connection
    void send_control(const std::string &line) {
        size_t sent = 0;
        while (sent < line.size()) {
            ssize_t n = send(controlfd_, line.data()+sent, line.size()-sent, MSG_NOSIGNAL);
            if (n > 0) sent += static_cast<size_t>(n);
            else if (errno == EINTR) continue;
            else throw std::runtime_error("control send failed");
        }
    }

    // asks the server to start measuring and checks that it accepted the command
    void control_start() {
        send_control("START " + cfg_.token + "\n");
        std::string line;
        char ch;
        while (line.size() < 128) {
            ssize_t n = recv(controlfd_, &ch, 1, 0);
            if (n <= 0) throw std::runtime_error("START: no control reply");
            if (ch == '\n') break;
            line += ch;
        }
        if (line != "OK START") throw std::runtime_error("START rejected: " + line);
        set_nonblocking(controlfd_);
        ep_add(controlfd_, EPOLLIN | EPOLLRDHUP);
    }

    // marks a failed player connection and removes its socket from the event loop
    void close_client(Client &c) {
        if (c.failed) return;
        c.failed = true; ++socket_failures_;
        if (c.fd >= 0) {
            epoll_ctl(epfd_, EPOLL_CTL_DEL, c.fd, nullptr);
            fd_to_index_.erase(c.fd);
            close(c.fd); c.fd = -1;
        }
    }

    // placeholder for sending the remaining bytes of a player request and recording when it finishes
    void try_flush(Client &c) {
        (void)c;
        throw std::logic_error("TODO 1: implement Generator::try_flush");
    }

    // placeholder for scheduling a new player update while respecting the outstanding request limit
    void schedule_send(Client &c, bool measured) {
        // counts every scheduled measured attempt even when it cannot be sent
        if (measured) ++offered_;

        (void)c;
        throw std::logic_error("TODO 2: implement Generator::schedule_send");
    }

    // placeholder for receiving and assembling complete acknowledgements from a player socket
    void read_client(Client &c) {
        (void)c;
        throw std::logic_error("TODO 3: implement Generator::read_client");
    }

    // placeholder for checking an acknowledgement and recording the outcome and response time
    void record_ack(Client &c, const uint8_t *data) {
        // decodes the reply fields and looks for the matching outstanding player update
        const uint32_t id = get_u32(data);
        const uint32_t seq = get_u32(data + 4);
        const uint32_t status = get_u32(data + 8);
        const auto it = c.outstanding.find(seq);

        (void)id; (void)status; (void)it;
        throw std::logic_error("TODO 4: implement Generator::record_ack");
    }

    // placeholder for reading server control replies and detecting when the benchmark is complete
    void read_control() {
        throw std::logic_error("TODO 5: implement Generator::read_control");
    }

    // placeholder for handling connection activity incoming replies and pending sends through the event loop
    void process_events(int timeout_ms) {
        // waits for the operating system to report activity on monitored sockets
        std::array<epoll_event, MAX_EVENTS> events{};
        const int n = epoll_wait(epfd_, events.data(), MAX_EVENTS, timeout_ms);
        if (n < 0) {
            if (errno == EINTR) return;
            throw std::runtime_error("epoll_wait failed");
        }

        (void)events; (void)n;
        throw std::logic_error("TODO 6: implement Generator::process_events");
    }

    // placeholder for sending player updates at a steady rate during warmup or measurement
    void run_phase(double seconds, bool measured) {
        // sets the phase duration and time between planned updates for each player
        const int64_t start = ns_now();
        const int64_t end = start + static_cast<int64_t>(seconds * 1e9);
        const int64_t period = std::max<int64_t>(1, static_cast<int64_t>(1e9 / cfg_.rate));
        std::priority_queue<Due, std::vector<Due>, std::greater<Due>> due;

        // spreads the first update times across the interval rather than sending everyone at once
        for (size_t i = 0; i < clients_.size(); ++i) {
            int64_t offset = static_cast<int64_t>(
                (static_cast<long double>(i) * period) / clients_.size());
            due.push(Due{start + offset, i});
        }

        (void)end; (void)measured;
        throw std::logic_error("TODO 7: implement Generator::run_phase");
    }

    // counts measured player requests that have not received an acknowledgement yet
    uint64_t outstanding_count() const {
        uint64_t n = 0;
        for (const Client &c : clients_) for (const auto &kv : c.outstanding)
            if (kv.second.measured) ++n;
        return n;
    }

    // placeholder for collecting late acknowledgements after the timed measurement ends
    void drain_responses(double seconds) {
        const int64_t deadline = ns_now() + static_cast<int64_t>(seconds * 1e9);

        (void)deadline;
        throw std::logic_error("TODO 8: implement Generator::drain_responses");
    }

    // finds a requested response time percentile from the sorted latency samples
    static double percentile(const std::vector<double> &sorted, double p) {
        if (sorted.empty()) return 0;
        size_t i = static_cast<size_t>(std::ceil(p*sorted.size()))-1;
        return sorted[std::min(i, sorted.size()-1)];
    }

    // placeholder for calculating benchmark statistics and writing the results to a csv file
    void save_results() {
        // sorts measured response times before calculating summary statistics
        std::sort(latencies_ms_.begin(), latencies_ms_.end());
        const double seconds =
            static_cast<double>(measurement_stop_ns_ - measurement_start_ns_) / 1e9;
        const uint64_t unanswered = outstanding_count();

        (void)seconds; (void)unanswered;
        throw std::logic_error("TODO 9: implement Generator::save_results");
    }
};

// starts the load generator with the chosen settings and reports any errors
int main(int argc, char **argv) {
    try {
        Generator gen(parse_args(argc, argv));
        gen.run();
    } catch (const std::exception &e) {
        std::cerr << "ERROR: " << e.what() << '\n'; return 1;
    }
    return 0;
}
