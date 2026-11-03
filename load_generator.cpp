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

using Clock = std::chrono::steady_clock;
static constexpr size_t FRAME_SIZE = 16;
static constexpr int MAX_EVENTS = 2048;
/**
 * @brief Read a monotonic nanosecond clock for send/ack timing.
 * @note Inputs: none.
 * @return Nanoseconds since an unspecified steady-clock origin.
 */
static int64_t ns_now() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        Clock::now().time_since_epoch()).count();
}
/**
 * @brief Extract a 32-bit network-order integer from a response frame.
 * @param p Pointer to at least four readable protocol bytes.
 * @return Integer converted into host byte order.
 */
static uint32_t get_u32(const uint8_t *p) {
    uint32_t net; std::memcpy(&net, p, 4); return ntohl(net);
}
/**
 * @brief Write a 32-bit integer into a wire-protocol request frame.
 * @param p Pointer to at least four writable bytes.
 * @param value Host-order integer to write in network order.
 * @return Nothing; modifies the pointed-to bytes.
 */
static void put_u32(uint8_t *p, uint32_t value) {
    uint32_t net = htonl(value); std::memcpy(p, &net, 4);
}
/**
 * @brief Construct a reproducible 16-byte simulated movement request.
 * @param id Nonzero player ID (one ID per TCP connection).
 * @param seq Per-player sequence number used to match the later acknowledgement.
 * @return Four network-order fields: player ID, seq, x=seq%10000, y=(3*seq)%10000.
 * @note The coordinate math is intentionally cheap; this is an I/O benchmark.
 */
[[maybe_unused]] static std::array<uint8_t, FRAME_SIZE> encode(uint32_t id, uint32_t seq) {
    std::array<uint8_t, FRAME_SIZE> a{};
    put_u32(a.data(), id); put_u32(a.data() + 4, seq);
    put_u32(a.data() + 8, seq % 10000); put_u32(a.data() + 12, (seq * 3U) % 10000);
    return a;
}
/**
 * @brief Enable O_NONBLOCK on an existing socket descriptor.
 * @param fd Valid open file descriptor (here, the control socket).
 * @return Nothing; throws on fcntl failure.
 */
static void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
        throw std::runtime_error("fcntl failed");
}
// Generator CLI settings. Each simulated player owns one TCP connection;
// rate is per-player updates/second, not total global messages/second.
// warmup is excluded from measurement; drain and stop_wait are post-STOP phases.
struct Config {
    std::string host = "127.0.0.1";
    std::string token = "local-benchmark";
    std::string csv = "client.csv";
    int port = 9000, control_port = 9001, workers = 4;
    int players = 1000, max_inflight = 32;
    double rate = 60, warmup = 5, duration = 30, drain = 3, stop_wait = 60;
};
/**
 * @brief Print all load-generator CLI options and exit-path usage guidance.
 * @note Inputs: none.
 * @return Nothing; writes to stdout.
 */
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
/**
 * @brief Read and validate requested players, message rate, timings, and TCP settings.
 * @param argc Number of command-line arguments.
 * @param argv Option/value arguments supplied to the load generator.
 * @return Config with validated user overrides and defaults.
 * @throws std::runtime_error On malformed, unsupported, or out-of-range options.
 * @note The --workers flag labels output only; the server independently sets its pool.
 */
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
/**
 * @brief Resolve a server IPv4 hostname/address and TCP port for connect().
 * @param hostname DNS name or IPv4 address of the server machine.
 * @param port TCP data or control port to connect to.
 * @return Populated IPv4 socket address structure.
 * @throws std::runtime_error If getaddrinfo fails.
 */
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
// One request that was scheduled/sent but has not yet received its ACK.
// sent_ns is filled when the complete 16-byte frame has left the client socket;
// measured distinguishes timed requests from warm-up requests.
struct Pending { uint32_t seq; int64_t sent_ns; bool measured; };
// State machine for one simulated player TCP connection: input buffer for
// partial ACKs, output buffer for partial requests, and pending sequences for
// exact ACK matching even if workers complete requests out of order.
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
// Scheduled transmission deadline for one client. The min-heap orders these
// records by ns, rather than scanning all players at every scheduler tick.
struct Due {
    int64_t ns;
    size_t index;
    /**
     * @brief Compare two scheduled send events by next due timestamp.
     * @param other Another Due record to compare with this one.
     * @return True if this event should happen later than other.
     * @note std::greater<Due> gives the priority queue earliest-due-first behavior.
     */
    bool operator>(const Due &other) const { return ns > other.ns; }
};

// Drives every virtual player from a single epoll thread, keeping the client
// workload lightweight. Separating this process onto Capi avoids consuming
// Brooks CPU time while benchmarking the game server.
class Generator {
public:
    /**
     * @brief Resolve endpoints, allocate epoll, and prepare the player container.
     * @param cfg Validated load-generator configuration, moved into cfg_.
     * @return Generator ready to establish TCP client connections.
     * @throws std::runtime_error On hostname-resolution or epoll failure.
     */
    explicit Generator(Config cfg) : cfg_(std::move(cfg)) {
        addr_ = resolve_ipv4(cfg_.host, cfg_.port);
        control_addr_ = resolve_ipv4(cfg_.host, cfg_.control_port);
        epfd_ = epoll_create1(EPOLL_CLOEXEC);
        if (epfd_ < 0) throw std::runtime_error("epoll_create1 failed");
        clients_.reserve(cfg_.players);
    }
    /**
     * @brief Close all simulated player sockets and the control/epoll descriptors.
     * @note Inputs: none.
     * @return Nothing; releases owned file descriptors.
     */
    ~Generator() {
        for (auto &p : clients_) if (p.fd >= 0) close(p.fd);
        if (controlfd_ >= 0) close(controlfd_);
        if (epfd_ >= 0) close(epfd_);
    }
    /**
     * @brief Coordinate client setup, warm-up, timed load, STOP, and CSV output.
     * @note Inputs: none; operates using cfg_ for client count, rate, and intervals.
     * @return Nothing; saves results when the timed trial has finished.
     * @note Server START is acknowledged before the measurement phase begins.
     * @note Continues reading after STOP so acknowledgements can be drained.
     */
    void run() {
        connect_clients();
        connect_control();
        std::cerr << "Connected " << connected_ << " players; warmup=" << cfg_.warmup
                  << "s measure=" << cfg_.duration << "s, rate=" << cfg_.rate << " Hz\n";
        if (cfg_.warmup > 0) run_phase(cfg_.warmup, false);
        control_start();
        measurement_start_ns_ = ns_now();
        run_phase(cfg_.duration, true);
        measurement_stop_ns_ = ns_now();
        send_control("STOP " + cfg_.token + "\n");
        drain_responses(cfg_.drain);
        // A heavily overloaded server can have outstanding requests when
        // the drain interval expires; continue servicing sockets so that
        // it can flush its control reply and write the CSV safely.
        const int64_t stop_wait_deadline = ns_now() + static_cast<int64_t>(cfg_.stop_wait * 1e9);
        while (!control_done_ && ns_now() < stop_wait_deadline) process_events(50);
        if (!control_done_) std::cerr << "WARNING: timed out waiting for server's DONE.\n";
        save_results();
    }
private:
    Config cfg_;
    sockaddr_in addr_{}, control_addr_{};
    int epfd_ = -1, controlfd_ = -1;
    size_t connected_ = 0;
    std::vector<Client> clients_;
    std::unordered_map<int, size_t> fd_to_index_;
    std::string control_in_;
    bool control_done_ = false;
    int64_t measurement_start_ns_ = 0, measurement_stop_ns_ = 0;
    // offered_: scheduled attempts (even skipped); sent_: whole frames sent;
    // ack_ok_/ack_rejected_: server outcome for measured requests.
    uint64_t offered_ = 0, sent_ = 0, ack_ok_ = 0, ack_rejected_ = 0;
    uint64_t rate_limited_ = 0, protocol_errors_ = 0, socket_failures_ = 0;
    std::vector<double> latencies_ms_;

    /**
     * @brief Register a client/control fd with the load generator epoll instance.
     * @param fd Socket to monitor.
     * @param e epoll event flags for read, write, and/or connection closure.
     * @return Nothing; throws if epoll registration fails.
     */
    void ep_add(int fd, uint32_t e) {
        epoll_event ev{}; ev.data.fd = fd; ev.events = e;
        if (epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) != 0)
            throw std::runtime_error(std::string("epoll_ctl add: ") + strerror(errno));
    }
    /**
     * @brief Update which readiness events are watched for a known socket.
     * @param fd Registered socket descriptor.
     * @param e Replacement set of epoll event flags.
     * @return Nothing; throws on epoll_ctl failure.
     */
    void ep_mod(int fd, uint32_t e) {
        epoll_event ev{}; ev.data.fd = fd; ev.events = e;
        if (epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ev) != 0)
            throw std::runtime_error(std::string("epoll_ctl mod: ") + strerror(errno));
    }
    /**
     * @brief Track write readiness only for pending connect/partial-send operations.
     * @param c Mutable simulated-client state with connection and partial-send flags.
     * @return Nothing; updates c.write_enabled and its epoll registration.
     */
    void update_interest(Client &c) {
        bool wants_write = !c.connected || c.partial;
        if (wants_write != c.write_enabled) {
            c.write_enabled = wants_write;
            ep_mod(c.fd, EPOLLIN | EPOLLRDHUP | (wants_write ? static_cast<uint32_t>(EPOLLOUT) : 0U));
        }
    }
    /**
     * @brief Open one nonblocking TCP connection for every simulated player.
     * @note Inputs: none; loops over cfg_.players and connects to addr_.
     * @return Nothing; populates clients_, fd_to_index_, and connected_.
     * @throws std::runtime_error If connection setup fails or exceeds its deadline.
     * @note Uses epoll rather than creating a separate OS thread per simulated player.
     */
    void connect_clients() {
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
        int64_t deadline = ns_now() + 30LL * 1000000000LL;
        while (connected_ < clients_.size() && ns_now() < deadline) {
            process_events(100);
            if (socket_failures_ > 0)
                throw std::runtime_error("One or more connections failed; see errors; check ulimit -n and server backlog");
        }
        if (connected_ != clients_.size())
            throw std::runtime_error("Timed out connecting all players; check server limit and network");
    }
    /**
     * @brief Open the independent TCP control channel to the game server.
     * @note Inputs: none; uses control_addr_.
     * @return Nothing; stores the connected descriptor in controlfd_.
     * @note Establishes 10-second socket I/O timeouts for initial control messages.
     */
    void connect_control() {
        controlfd_ = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (controlfd_ < 0) throw std::runtime_error("control socket failed");
        timeval tv{}; tv.tv_sec = 10;
        setsockopt(controlfd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        setsockopt(controlfd_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        if (connect(controlfd_, reinterpret_cast<sockaddr*>(&control_addr_), sizeof(control_addr_)) != 0)
            throw std::runtime_error(std::string("control connect: ") + strerror(errno));
    }
    /**
     * @brief Send one text command in full over the control connection.
     * @param line Command including its final newline, e.g. "STOP token\n".
     * @return Nothing; retries interrupted writes or throws on failure.
     */
    void send_control(const std::string &line) {
        size_t sent = 0;
        while (sent < line.size()) {
            ssize_t n = send(controlfd_, line.data()+sent, line.size()-sent, MSG_NOSIGNAL);
            if (n > 0) sent += static_cast<size_t>(n);
            else if (errno == EINTR) continue;
            else throw std::runtime_error("control send failed");
        }
    }
    /**
     * @brief Start server-side measurement and confirm the START acknowledgement.
     * @note Inputs: none; sends configured token via controlfd_.
     * @return Nothing; switches the control socket to nonblocking epoll afterward.
     * @throws std::runtime_error On missing or rejected OK START response.
     */
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
    /**
     * @brief Mark one simulated client as failed and remove its TCP socket.
     * @param c Mutable client state whose fd/flags will be modified.
     * @return Nothing; increments socket_failures_ on the first failure.
     * @note Does not reinterpret missing acknowledgements as successful ones.
     */
    void close_client(Client &c) {
        if (c.failed) return;
        c.failed = true; ++socket_failures_;
        if (c.fd >= 0) {
            epoll_ctl(epfd_, EPOLL_CTL_DEL, c.fd, nullptr);
            fd_to_index_.erase(c.fd);
            close(c.fd); c.fd = -1;
        }
    }
    /**
     * @brief Progress a potentially partial 16-byte player-message send.
     * @param c Mutable client with pending_frame, pending_sent, and pending_seq.
     * @return Nothing; closes the client on fatal send errors.
     * @note When all 16 bytes have been sent, records the timing origin for RTT.
     * @note A blocked send remains pending until epoll reports EPOLLOUT.
     * @warning INCOMPLETE STUDENT EXERCISE: this method currently throws; implement its TODOs.
     */
    void try_flush(Client &c) {
        // The setup for sending already exists in Client:
        //   c.pending_frame = encoded 16-byte request
        //   c.pending_sent  = number of bytes already written
        //   c.pending_seq   = sequence number for this frame
        //   c.partial       = whether a frame still needs transmission
        //
        // TODO 1A: Return immediately if c.partial is false or c.failed is true.
        // TODO 1B: While pending_sent < FRAME_SIZE, call send() on the remaining
        //          bytes. Add positive return values to pending_sent.
        // TODO 1C: Retry if errno == EINTR. Stop the loop if the nonblocking
        //          socket returns EAGAIN/EWOULDBLOCK. Close on fatal errors.
        // TODO 1D: Once the ENTIRE frame has been sent:
        //           * set c.partial = false and c.pending_sent = 0;
        //           * locate c.pending_seq in c.outstanding;
        //           * record p.sent_ns = ns_now() (RTT starts here);
        //           * if p.measured, increment sent_.
        // TODO 1E: Call update_interest(c) unless the connection failed, so
        //          epoll stops polling for writable sockets with no work.
        //
        // Important: One send() is not guaranteed to transmit all 16 bytes.
        (void)c;
        throw std::logic_error("TODO 1: implement Generator::try_flush");
    }
    /**
     * @brief Attempt a scheduled player update while enforcing backpressure limits.
     * @param c Simulated client that is due to send its next sequence number.
     * @param measured True in the measurement phase; false for warm-up sends.
     * @return Nothing; may skip a request if disconnected, blocked, or at in-flight cap.
     * @note offered_ includes scheduled *attempts*, including skipped measured sends.
     * @note rate_limited_ counts attempts not dispatched due to client backpressure.
     * @warning INCOMPLETE STUDENT EXERCISE: this method currently throws; implement its TODOs.
     */
    void schedule_send(Client &c, bool measured) {
        // Called ONCE for each scheduled update attempt from run_phase().
        // `measured` is false during warm-up, true during the timed trial.
        //
        // This counter tracks DEMAND, not successful sends. Even an attempt
        // skipped due to backpressure must count toward offered_ during trial.
        if (measured) ++offered_;

        // TODO 2A: If the client is failed/disconnected, has a partial write,
        //          or already has max_inflight outstanding requests, do not
        //          enqueue more bytes. Increment rate_limited_ if measured.
        // TODO 2B: Otherwise reserve c.next_seq++ as the new request number.
        // TODO 2C: Call encode(c.id, seq) and store the resulting bytes in
        //          c.pending_frame; reset c.pending_sent, set c.partial=true,
        //          and remember c.pending_seq.
        // TODO 2D: Insert Pending{seq, 0, measured} into c.outstanding.
        // TODO 2E: Call try_flush(c) to attempt the first nonblocking send.
        //
        // Do not increment sent_ until try_flush has sent the *whole* frame.
        (void)c;
        throw std::logic_error("TODO 2: implement Generator::schedule_send");
    }
    /**
     * @brief Read and reassemble 16-byte acknowledgements from one TCP player socket.
     * @param c Mutable client including partial-input buffer and outstanding requests.
     * @return Nothing; calls record_ack for each complete frame.
     * @note TCP recv can return any chunk size; frames may span multiple calls.
     * @warning INCOMPLETE STUDENT EXERCISE: this method currently throws; implement its TODOs.
     */
    void read_client(Client &c) {
        // TCP is a stream, not a message transport: recv() can return half
        // an ACK, several ACKs, or an ACK split across multiple reads.
        // State available to reassemble replies:
        //   c.in       = 16-byte partial-response buffer
        //   c.in_used  = bytes currently stored in that buffer
        //
        // TODO 3A: Repeatedly recv(c.fd, ...) into a temporary byte buffer.
        // TODO 3B: On positive n, consume every received byte. Copy only as
        //          much as fits into the remaining portion of c.in.
        // TODO 3C: When c.in_used reaches FRAME_SIZE, call
        //          record_ack(c, c.in.data()) and reset c.in_used = 0.
        // TODO 3D: If recv returns 0, the peer closed; call close_client(c).
        // TODO 3E: On EINTR retry; on EAGAIN/EWOULDBLOCK stop this read pass;
        //          on any other socket error call close_client(c).
        //
        // Hint: std::min(FRAME_SIZE - c.in_used, available_bytes) is useful.
        (void)c;
        throw std::logic_error("TODO 3: implement Generator::read_client");
    }
    /**
     * @brief Validate a reply and account for success, rejection, and response latency.
     * @param c Client whose outstanding sequence numbers should contain this reply.
     * @param data Pointer to a complete 16-byte acknowledgement frame.
     * @return Nothing; updates ack counters and latency samples when measured.
     * @note Successful RTT begins after the request finishes sending, not when scheduled.
     * @note Unknown sequence/player/status values increment protocol_errors_.
     * @warning INCOMPLETE STUDENT EXERCISE: this method currently throws; implement its TODOs.
     */
    void record_ack(Client &c, const uint8_t *data) {
        // Each reply is 16 bytes: player_id, sequence, status, reserved.
        // All four fields are big-endian uint32_t values.
        // status == 0 means processed; status == 1 means server rejected it.
        const uint32_t id = get_u32(data);
        const uint32_t seq = get_u32(data + 4);
        const uint32_t status = get_u32(data + 8);
        const auto it = c.outstanding.find(seq);

        // TODO 4A: Validate id == c.id, it != c.outstanding.end(), and
        //          status is either 0 or 1. Count protocol_errors_ on failure.
        // TODO 4B: Copy the matching Pending record, then erase the sequence
        //          from c.outstanding so it cannot be acknowledged twice.
        // TODO 4C: Ignore warm-up ACKs in measured counters (but still erase).
        // TODO 4D: For a measured ACK: status 0 -> ack_ok_++; status 1 ->
        //          ack_rejected_++.
        // TODO 4E: For successful measured ACKs, calculate the round-trip
        //          latency in ms = (ns_now() - p.sent_ns) / 1e6 and append to
        //          latencies_ms_. If p.sent_ns is zero, count protocol error.
        //
        // Do not assume ACKs are always delivered in sequence order.
        (void)id; (void)status; (void)it;
        throw std::logic_error("TODO 4: implement Generator::record_ack");
    }
    /**
     * @brief Drain nonblocking control replies and detect server completion.
     * @note Inputs: none; reads newline-delimited strings from controlfd_.
     * @return Nothing; sets control_done_ upon receiving DONE.
     * @throws std::runtime_error If the server responds with an ERR line.
     * @warning INCOMPLETE STUDENT EXERCISE: this method currently throws; implement its TODOs.
     */
    void read_control() {
        // The control socket receives newline-delimited text. The server
        // replies "OK STOP" first, and later "DONE" when reporting finishes.
        // `control_in_` holds an unfinished line across multiple recv() calls.
        //
        // TODO 5A: Read from controlfd_ until recv() would block.
        // TODO 5B: Append bytes to control_in_ (do not assume one recv = line).
        // TODO 5C: While control_in_ contains '\n', extract complete lines.
        //          Remove each processed line from the buffer.
        // TODO 5D: Set control_done_ = true if a complete line is "DONE".
        //          If a line starts with "ERR", throw std::runtime_error.
        // TODO 5E: Correctly handle EINTR, EAGAIN, and EOF.
        //
        // Hint: std::string::find('\n'), substr(), and erase() are handy.
        throw std::logic_error("TODO 5: implement Generator::read_control");
    }
    /**
     * @brief Service nonblocking network activity for all simulated clients.
     * @param timeout_ms Maximum milliseconds to block waiting for epoll events.
     * @return Nothing; advances connect/send/receive state and handles disconnects.
     * @note Called repeatedly while connecting, generating traffic, and draining replies.
     * @warning INCOMPLETE STUDENT EXERCISE: this method currently throws; implement its TODOs.
     */
    void process_events(int timeout_ms) {
        // This is the central single-threaded event loop. Epoll allows one
        // operating-system thread to monitor thousands of TCP sockets.
        std::array<epoll_event, MAX_EVENTS> events{};
        const int n = epoll_wait(epfd_, events.data(), MAX_EVENTS, timeout_ms);
        if (n < 0) {
            if (errno == EINTR) return; // Interrupted wait is not an I/O error.
            throw std::runtime_error("epoll_wait failed");
        }

        // TODO 6A: For each event among events[0..n), read fd and event flags.
        // TODO 6B: If fd == controlfd_, call read_control() for EPOLLIN.
        // TODO 6C: Look up other descriptors in fd_to_index_; ignore stale fds.
        // TODO 6D: For a not-yet-connected client with EPOLLOUT, call
        //          getsockopt(fd, SOL_SOCKET, SO_ERROR, ...) to confirm the
        //          nonblocking connect actually succeeded. On success mark
        //          c.connected=true, increment connected_, update_interest(c).
        //          On failure log the reason and close_client(c).
        // TODO 6E: Handle EPOLLIN with read_client(c), writable partial
        //          frames with try_flush(c), and hangups/errors with
        //          close_client(c). Check c.failed between operations.
        //
        // Warning: EPOLLOUT alone does NOT prove a connect() succeeded.
        (void)events; (void)n;
        throw std::logic_error("TODO 6: implement Generator::process_events");
    }
    /**
     * @brief Run fixed-rate, staggered, open-loop send scheduling for all players.
     * @param seconds Duration of this phase, in wall-clock seconds.
     * @param measured Whether due attempts and acknowledgements count toward the trial.
     * @return Nothing; schedules at 1/rate seconds per client until the phase ends.
     * @note Uses an earliest-due priority queue and services sockets between batches.
     * @note Overload causes attempted sends to be skipped, not silently rescheduled.
     * @warning INCOMPLETE STUDENT EXERCISE: this method currently throws; implement its TODOs.
     */
    void run_phase(double seconds, bool measured) {
        // A measurement phase generates traffic at a FIXED OFFERED RATE.
        // For N players at R messages/second/player, total offered demand is
        // N * R messages/second, even if some requests are rate-limited.
        const int64_t start = ns_now();
        const int64_t end = start + static_cast<int64_t>(seconds * 1e9);
        const int64_t period = std::max<int64_t>(1, static_cast<int64_t>(1e9 / cfg_.rate));
        std::priority_queue<Due, std::vector<Due>, std::greater<Due>> due;

        // Initial deadlines are distributed across the update period so all
        // 5,000 players don't send on exactly the same CPU scheduler tick.
        for (size_t i = 0; i < clients_.size(); ++i) {
            int64_t offset = static_cast<int64_t>(
                (static_cast<long double>(i) * period) / clients_.size());
            due.push(Due{start + offset, i});
        }

        // TODO 7A: Loop until ns_now() >= end.
        // TODO 7B: Pop all records whose due.ns <= now, up to a batch of 4096;
        //          call schedule_send(clients_[due.index], measured) for each.
        // TODO 7C: Reschedule each due event at *its original deadline*
        //          plus period (NOT current time plus period). This preserves
        //          open-loop demand even when the server becomes overloaded.
        // TODO 7D: Compute the time until the next event or phase end.
        //          Convert that to an epoll timeout (at most 10 ms).
        // TODO 7E: Call process_events(timeout_ms) every iteration, so ACKs
        //          and partially sent frames continue to make progress.
        //
        // Hint: due.top() provides the earliest scheduled deadline.
        (void)end; (void)measured;
        throw std::logic_error("TODO 7: implement Generator::run_phase");
    }
    /**
     * @brief Count measured requests still waiting for an acknowledgement.
     * @note Inputs: none; walks each client's outstanding request map.
     * @return Number of measured, not-yet-acknowledged requests.
     */
    uint64_t outstanding_count() const {
        uint64_t n = 0;
        for (const Client &c : clients_) for (const auto &kv : c.outstanding)
            if (kv.second.measured) ++n;
        return n;
    }
    /**
     * @brief Continue receiving ACKs for a limited period after the STOP command.
     * @param seconds Maximum drain time in seconds.
     * @return Nothing; stops early if all measured requests are answered and DONE arrived.
     * @note This can add late ACKs to client latency statistics after the measured window.
     * @warning INCOMPLETE STUDENT EXERCISE: this method currently throws; implement its TODOs.
     */
    void drain_responses(double seconds) {
        // After STOP, do NOT send new player updates. We only want to collect
        // ACKs that correspond to requests scheduled during the timed phase.
        const int64_t deadline = ns_now() + static_cast<int64_t>(seconds * 1e9);

        // TODO 8A: Repeatedly call process_events(10) until deadline.
        // TODO 8B: You may stop early when outstanding_count() == 0 AND
        //          control_done_ is true (the server finished its report).
        // TODO 8C: Do not treat timed-out requests as successful ACKs.
        //
        // Server completion can arrive later than the initial drain interval;
        // Generator::run handles that separately using cfg_.stop_wait.
        (void)deadline;
        throw std::logic_error("TODO 8: implement Generator::drain_responses");
    }
    /**
     * @brief Extract a nearest-rank latency percentile from sorted samples.
     * @param sorted Ascending sequence of response latencies in milliseconds.
     * @param p Quantile expressed as fraction in (0,1], e.g. 0.95 for P95.
     * @return Latency at ceil(p * sample_count) (clamped), or 0 with no samples.
     * @note Caller must sort the samples first; no interpolation is performed.
     */
    static double percentile(const std::vector<double> &sorted, double p) {
        if (sorted.empty()) return 0;
        size_t i = static_cast<size_t>(std::ceil(p*sorted.size()))-1;
        return sorted[std::min(i, sorted.size()-1)];
    }
    /**
     * @brief Summarize measured load-generation and acknowledgment results in a CSV.
     * @note Inputs: none; reads cfg_, counters, measurement timestamps, and latency samples.
     * @return Nothing; writes a single row and prints a human-readable summary.
     * @note Throughput = successful acknowledgements / measured wall-clock seconds.
     * @note Reports mean, P50, P95, and P99 of successful-request RTT samples.
     * @warning INCOMPLETE STUDENT EXERCISE: this method currently throws; implement its TODOs.
     */
    void save_results() {
        // Called after measurement and the response-drain period have ended.
        // All counters here are for the measured phase unless noted otherwise.
        std::sort(latencies_ms_.begin(), latencies_ms_.end());
        const double seconds =
            static_cast<double>(measurement_stop_ns_ - measurement_start_ns_) / 1e9;
        const uint64_t unanswered = outstanding_count();

        // TODO 9A: Compute the average of successful RTT samples in ms; use
        //          0 when latencies_ms_ is empty to avoid division by zero.
        // TODO 9B: Use percentile(latencies_ms_, 0.50 / 0.95 / 0.99) to
        //          calculate P50, P95, and P99. Samples are already sorted.
        // TODO 9C: Open cfg_.csv using std::ofstream; check it succeeded.
        // TODO 9D: Write the exact header below so analyze_results.py can
        //          read the file without requiring any script changes:
        // workers,players,rate_hz,warmup_s,duration_s,connected,offered,sent,
        // ack_ok,ack_rejected,unanswered,rate_limited,throughput_msg_s,offered_msg_s,
        // avg_latency_ms,p50_latency_ms,p95_latency_ms,p99_latency_ms,protocol_errors,socket_failures
        // TODO 9E: Write one data row with these values. Useful formulas:
        //          throughput_msg_s = ack_ok_ / seconds
        //          offered_msg_s    = offered_ / seconds
        // TODO 9F: Print a compact summary and warn if rate_limited_,
        //          unanswered, socket_failures_, or protocol_errors_ is nonzero.
        //
        // Crucial: the denominator is the MEASUREMENT duration, not the
        // measurement duration plus warm-up or acknowledgment-drain time.
        (void)seconds; (void)unanswered;
        throw std::logic_error("TODO 9: implement Generator::save_results");
    }
};

/**
 * @brief Entry point: parse arguments, construct the generator, and run it.
 * @param argc Number of CLI arguments.
 * @param argv Array of CLI strings.
 * @return Exit status 0 for success, 1 when an exception occurs.
 */
int main(int argc, char **argv) {
    try {
        Generator gen(parse_args(argc, argv));
        gen.run();
    } catch (const std::exception &e) {
        std::cerr << "ERROR: " << e.what() << '\n'; return 1;
    }
    return 0;
}
