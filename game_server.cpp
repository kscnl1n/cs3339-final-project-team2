// CS3339 Fall 2026 - Multicore game-server benchmark (Linux, C++17)
//
// Architecture: ONE epoll TCP I/O thread -> mutex-protected shared work queue
// -> configurable worker threads -> completion queue -> same TCP I/O thread.
// Message size is fixed at 16 bytes; all four fields are 32-bit network order.
// Request: {player_id, sequence, x, y}; response: {player_id, sequence, status, 0}
// status 0 = processed, 1 = rejected because the work queue was full.
//
// A separate control port accepts "START <token>" and "STOP <token>".
// The load generator uses these to delimit the *exact* measurement interval.


/*
PROBLEM

Imagine we have a server with 1000 players moving about in a digital world.
Each player would periodically send their coordinates to the server:

player_id = 42;
sequence = 100;
x = 150;
y = 275;

The server then needs to send updates to:

- Receive the player's movement update packet
- Assign the update to a worker thread
- Find the player's current state in memory
- Update their position without introducing race conditions
- Send a response confirming that the update was processed

NOTES
This follows producer-consumer architecture, with a customizable number
of workers. You can set the number of workers like so:

./game_server --workers 1
./game_server --workers 2
./game_server --workers 4
./game_server --workers 8
./game_server --workers 16

We can plan to include this in our benchmarks later. For now let's verify
the code actually works.

It also needs load_generator.cpp to work, which simulates the actual player load.

*/


// misc headers
#include <cerrno> // HEADER - for error codes from the OS
#include <csignal> // header to handle signals SIGint, SIGterm

// thread headers
#include <pthread.h> // POSIX threads and thread-specific system functionality
#include <sched.h> // cpu affinity, scheduling operations

// networking/linux headers
#include <netinet/in.h> // IP addresses - maybe we won't use this but let's see
#include <arpa/inet.h> // converting IP addresses & byte order
#include <linux/perf_event.h> // linux hardware & performance counters (ex: perf)
#include <sys/epoll.h> // epolling
#include <sys/ioctl.h> // device/file descriptor controls
#include <sys/syscall.h> // linux call numbers
#include <sys/eventfd.h> // event notifier file descriptor
#include <sys/resource.h> // processing resource limits
#include <sys/socket.h> // sockets - we probably also won't use this either, will maybe delete later - kay
#include <unistd.h> // .read(), .write(), .close()

#include <netinet/tcp.h>
#include <fcntl.h> 

// libs
#include <algorithm> //std sort, max, find etc.
#include <deque> // data types
#include <array>
#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint> // fixed sized integers
#include <cstring>
#include <filesystem> // filesystem paths & directories
#include <fstream> // reading + writing files using streams
#include <iomanip> // control output formatting and precision
#include <iostream> // std out/in, console input/output
#include <atomic> // thread-safe atomic operations sans mutexes
#include <thread>
#include <chrono> // system clocks
#include <limits>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <set>
#include <sstream>
#include <stdexcept>


// HELPFUL CONSTANTS
// for the purposes of this assignment we assume the clock is monotonic (forward progression only)
using Clock = std::chrono::steady_clock;
static constexpr size_t FRAME_SIZE = 16; // every request sends exactly 16 bytes
static constexpr uint32_t MAX_PLAYER_ID = 65535;
static constexpr int MAX_EVENTS = 1000;
static std::atomic<bool> g_exit_requested{false};


/**
 * @brief Read a monotonic timestamp for benchmark timing :>
 * @note Inputs: none; uses std::chrono::steady_clock, not wall-clock time
 * @return Nanoseconds since an arbitrary steady-clock epoch
 * @note Differences of two timestamps measure elapsed time; absolute values are not dates
 */
// gets a steady timestamp in nanoseconds so the server can measure how long work takes
static int64_t ns_now() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        Clock::now().time_since_epoch()).count();
}

// marks the server for shutdown when it receives a termination signal
static void signal_handler(int) { g_exit_requested.store(true); }

// reads a four byte value from a network message and converts it to the system byte order
static uint32_t get_u32(const uint8_t *p) {
    uint32_t net; std::memcpy(&net, p, 4); return ntohl(net);
}

// writes a four byte value into a network message using network byte order
static void put_u32(uint8_t *p, uint32_t value) {
    uint32_t net = htonl(value); std::memcpy(p, &net, 4);
}

// builds the response sent back after a player update is accepted or rejected
static std::array<uint8_t, FRAME_SIZE> ack_frame(uint32_t player, uint32_t seq, uint32_t status) {
    std::array<uint8_t, FRAME_SIZE> a{};
    put_u32(a.data(), player); put_u32(a.data() + 4, seq);
    put_u32(a.data() + 8, status); put_u32(a.data() + 12, 0);
    return a;
}

// creates a nonblocking tcp socket and starts listening for incoming connections
static int listen_socket(const std::string &bind_ip, int port) {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) throw std::runtime_error("socket failed");
    int one = 1; setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(port);
    if (inet_pton(AF_INET, bind_ip.c_str(), &addr.sin_addr) != 1) {
        close(fd); throw std::runtime_error("--bind must be an IPv4 address");
    }
    if (bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
        listen(fd, 8192) != 0) {
        const std::string err = strerror(errno); close(fd);
        throw std::runtime_error("bind/listen port " + std::to_string(port) + ": " + err);
    }
    return fd;
}

// converts a time value into seconds for reporting and calculations
static double time_seconds(const timeval &tv) { return tv.tv_sec + tv.tv_usec / 1e6; }

// adds together the cpu time spent in user code and the operating system
static double cpu_seconds(const rusage &ru) {
    return time_seconds(ru.ru_utime) + time_seconds(ru.ru_stime);
}

// reads a number from a system file to learn about the available cpu hardware
static int read_int_file(const std::string &path) {
    std::ifstream f(path); int x = -1; if (f) f >> x; return x;
}

// finds the cpus this process can use and puts different physical cores ahead of sibling threads
static std::vector<int> available_physical_first() {
    cpu_set_t allowed; CPU_ZERO(&allowed);
    if (sched_getaffinity(0, sizeof(allowed), &allowed) != 0)
        throw std::runtime_error("sched_getaffinity failed");
    std::vector<int> primary, siblings;
    std::set<std::pair<int,int>> seen;
    for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
        if (!CPU_ISSET(cpu, &allowed)) continue;
        std::string base = "/sys/devices/system/cpu/cpu" + std::to_string(cpu) + "/topology/";
        int socket_id = read_int_file(base + "physical_package_id");
        int core_id = read_int_file(base + "core_id");
        if (socket_id < 0 || core_id < 0 || seen.insert({socket_id, core_id}).second)
            primary.push_back(cpu);
        else siblings.push_back(cpu);
    }
    primary.insert(primary.end(), siblings.begin(), siblings.end());
    return primary;
}

// manages optional linux performance counters for cpu activity during a benchmark
class PerfCounters {
public:

    // closes any performance counter handles that are still open
    ~PerfCounters() { close_all(); }

    // starts collecting hardware and scheduling statistics for the server threads
    void start(const std::vector<pid_t> &tids) {
        close_all(); available_ = false; counts_.fill(0);

        struct Event {uint32_t type; uint64_t config;};
        constexpr Event ev[4] = {
            {PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES},
            {PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS},
            {PERF_TYPE_HARDWARE, PERF_COUNT_HW_CACHE_MISSES},
            {PERF_TYPE_SOFTWARE, PERF_COUNT_SW_CONTEXT_SWITCHES}
        };
        // opens a separate set of counters for each server thread
        for (pid_t tid : tids) {
            if (tid <= 0) { close_all(); return; }
            for (int i = 0; i < 4; ++i) {
                perf_event_attr attr{};
                attr.size = sizeof(attr);
                attr.type = ev[i].type;
                attr.config = ev[i].config;
                attr.disabled = 1;
                attr.exclude_kernel = 1;
                attr.exclude_hv = 1;
                attr.read_format = PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING;
                int fd = static_cast<int>(syscall(SYS_perf_event_open, &attr, tid, -1, -1, 0));
                if (fd < 0) {
                    if (!warned_) {
                        std::cerr << "NOTE: perf_event_open not permitted/supported ("
                                  << strerror(errno) << "); hardware counters will be blank.\n";
                        warned_ = true;
                    }
                    close_all(); return;
                }
                fds_.push_back({fd,i});
            }
        }
        // resets and enables the counters once they have been opened
        for (const auto &f : fds_) {
            ioctl(f.fd, PERF_EVENT_IOC_RESET, 0);
            ioctl(f.fd, PERF_EVENT_IOC_ENABLE, 0);
        }
        available_ = true;
    }

    // stops the counters and adds up the measurements collected from each thread
    void stop() {
        if (!available_) return;
        struct Reading {uint64_t count, enabled, running;};
        for (const auto &f : fds_) {
            ioctl(f.fd, PERF_EVENT_IOC_DISABLE, 0);
            Reading r{};
            if (read(f.fd, &r, sizeof(r)) != static_cast<ssize_t>(sizeof(r))) {
                available_ = false; break;
            }
            if (r.running == 0) { available_ = false; break; }
            const long double scaled = static_cast<long double>(r.count) * r.enabled / r.running;
            counts_[f.index] += static_cast<uint64_t>(scaled);
        }
        close_all();
    }

    // reports whether performance counter data was collected successfully
    bool available() const { return available_; }

    // returns the total number of cpu cycles counted during the measurement
    uint64_t cycles() const { return counts_[0]; }

    // returns the total number of instructions counted during the measurement
    uint64_t instructions() const { return counts_[1]; }

    // returns the number of cache misses counted during the measurement
    uint64_t cache_misses() const { return counts_[2]; }

    // returns the number of context switches counted during the measurement
    uint64_t context_switches() const { return counts_[3]; }

private:
    // stores the open performance counter handles and their collected values
    struct Opened {int fd, index;};
    std::vector<Opened> fds_;
    std::array<uint64_t,4> counts_{};
    bool available_ = false, warned_ = false;

    // closes all open performance counter handles and clears the list
    void close_all() {
        for (const auto &f : fds_) close(f.fd);
        fds_.clear();
    }
};

// holds the settings used to configure the server and its benchmark behavior
struct Config {
    std::string bind = "127.0.0.1";
    std::string output = "server.csv";
    std::string token = "local-benchmark";
    int port = 9000;
    int control_port = 9001;
    int workers = 4;
    size_t max_queue = 200000;
    bool pin_workers = true;
};

// prints the available command line options and their default values
static void help() {
    std::cout << "Usage: game_server [options]\n"
              << "  --bind IP           Bind IPv4 address (default 127.0.0.1)\n"
              << "  --port N            Game TCP port (default 9000)\n"
              << "  --control-port N    Control TCP port (default port+1)\n"
              << "  --workers N         Worker thread count (default 4)\n"
              << "  --max-queue N       Maximum queued requests (default 200000)\n"
              << "  --pin-workers 0|1   Pin workers to distinct physical CPUs first (default 1)\n"
              << "  --control-token S   Control port token (use a random value remotely)\n"
              << "  --output FILE       One-row CSV file (default server.csv)\n";
}

// reads the command line options and checks that the requested settings are valid
static Config parse_args(int argc, char **argv) {
    Config c; bool has_control_port = false;
    for (int i = 1; i < argc; ++i) {
        std::string key = argv[i];
        if (key == "--help") { help(); std::exit(0); }
        if (i + 1 >= argc) throw std::runtime_error("Missing value for " + key);
        std::string v = argv[++i];
        if (key == "--bind") c.bind = v;
        else if (key == "--port") c.port = std::stoi(v);
        else if (key == "--control-port") {c.control_port = std::stoi(v); has_control_port = true;}
        else if (key == "--workers") c.workers = std::stoi(v);
        else if (key == "--max-queue") c.max_queue = std::stoull(v);
        else if (key == "--pin-workers") c.pin_workers = (v == "1");
        else if (key == "--control-token") c.token = v;
        else if (key == "--output") c.output = v;
        else throw std::runtime_error("Unknown argument: " + key);
    }
    if (!has_control_port) c.control_port = c.port + 1;
    if (c.workers < 1 || c.workers > 512 || c.port < 1 || c.port > 65534 ||
        c.control_port < 1 || c.control_port > 65535 || c.control_port == c.port ||
        c.max_queue < 1 || c.token.empty()) throw std::runtime_error("Invalid options");
    return c;
}

// represents one player update waiting to be processed by a worker thread
struct Work {
    int fd;
    uint64_t connection_id;
    uint32_t player, seq;
    int32_t x, y;
    int64_t queued_ns;
    bool measured;
};

// holds a finished worker response until the network thread can send it
struct Completion {
    int fd;
    uint64_t connection_id;
    std::array<uint8_t, FRAME_SIZE> bytes;
};

// stores a player's latest position and update sequence number
struct PlayerState { int32_t x = 0, y = 0; uint32_t last_seq = 0; };

// keeps track of the buffered input and output for an individual client connection
struct Connection {
    uint64_t id;
    std::array<uint8_t, FRAME_SIZE> in{};
    size_t in_used = 0;
    std::vector<uint8_t> out;
    size_t out_sent = 0;
    bool write_enabled = false;
    bool failed_write = false;

    // creates a new connection record and reserves space for outgoing responses
    explicit Connection(uint64_t cid) : id(cid) { out.reserve(256); }
};

// stores request counts and timing measurements shared between the server threads
struct Counters {
    std::atomic<uint64_t> received{0}, rejected{0}, completed{0};
    std::atomic<uint64_t> queue_wait_ns{0}, queue_lock_ns{0}, state_lock_ns{0}, processing_ns{0};
    std::atomic<uint64_t> queue_peak{0};

    // clears the recorded statistics before starting a new benchmark
    void reset() {
        received.store(0); rejected.store(0); completed.store(0);
        queue_wait_ns.store(0); queue_lock_ns.store(0);
        state_lock_ns.store(0); processing_ns.store(0); queue_peak.store(0);
    }
};

// SERVER CLASS


// runs the player connections worker threads and benchmark control interface
class Server {
public:

    // sets up the sockets event loop and worker pool needed to run the server
    explicit Server(Config cfg) : cfg_(std::move(cfg)), players_(MAX_PLAYER_ID+1),
        state_locks_(256), cpu_order_(available_physical_first()),
        worker_tids_(cfg_.workers) {
        if (cpu_order_.empty()) throw std::runtime_error("No allowed CPUs");
        epfd_ = epoll_create1(EPOLL_CLOEXEC);
        eventfd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
        listenfd_ = listen_socket(cfg_.bind, cfg_.port);
        control_listenfd_ = listen_socket(cfg_.bind, cfg_.control_port);
        if (epfd_ < 0 || eventfd_ < 0) throw std::runtime_error("epoll/eventfd failed");
        add_epoll(listenfd_, EPOLLIN);
        add_epoll(control_listenfd_, EPOLLIN);
        add_epoll(eventfd_, EPOLLIN);
        for (int i = 0; i < cfg_.workers; ++i) {
            worker_threads_.emplace_back(&Server::worker_loop, this, i);
        }
    }

    // shuts down the worker pool and closes the server connections and event handles
    ~Server() {
        {
            std::lock_guard<std::mutex> lock(work_mutex_); quitting_ = true;
        }
        work_cv_.notify_all();
        for (auto &thread : worker_threads_) if (thread.joinable()) thread.join();
        for (auto &pair : clients_) close(pair.first);
        if (controlfd_ >= 0) close(controlfd_);
        if (control_listenfd_ >= 0) close(control_listenfd_);
        if (listenfd_ >= 0) close(listenfd_);
        if (eventfd_ >= 0) close(eventfd_);
        if (epfd_ >= 0) close(epfd_);
    }

    // runs the main event loop and passes network activity to the appropriate handlers
    void run() {
        std::cerr << "Listening on " << cfg_.bind << ':' << cfg_.port
                  << " (control " << cfg_.control_port << "), workers=" << cfg_.workers
                  << ", allowed_logical_CPUs=" << cpu_order_.size() << '\n';
        if (cfg_.pin_workers && static_cast<size_t>(cfg_.workers) > cpu_order_.size())
            std::cerr << "WARNING: more workers than available logical CPUs; CPU pins wrap.\n";
        std::array<epoll_event, MAX_EVENTS> events{};
        while (!g_exit_requested.load()) {
            const int n = epoll_wait(epfd_, events.data(), MAX_EVENTS, 100);
            if (n < 0) { if (errno == EINTR) continue; throw std::runtime_error("epoll_wait failed"); }
            for (int i = 0; i < n; ++i) {
                const int fd = events[i].data.fd;
                const uint32_t e = events[i].events;
                if (fd == listenfd_) accept_clients();
                else if (fd == control_listenfd_) accept_control();
                else if (fd == eventfd_) handle_completions();
                else if (fd == controlfd_) handle_control(e);
                else if (clients_.find(fd) != clients_.end()) handle_client(fd, e);
            }
            if (stop_requested_ && pending_measured_.load(std::memory_order_acquire) == 0)
                finish_measurement();
        }
        std::cerr << "Server stopping.\n";
    }

private:
    // holds the server configuration socket handles and benchmark state
    Config cfg_;
    int epfd_ = -1, eventfd_ = -1, listenfd_ = -1, control_listenfd_ = -1;
    int controlfd_ = -1;
    uint64_t next_conn_id_ = 1;
    size_t connection_peak_ = 0;
    std::string control_in_, control_out_;
    bool control_write_enabled_ = false;
    bool stop_requested_ = false;
    bool measurement_active_ = false;
    std::atomic<int64_t> stop_ns_{INT64_MAX};
    int64_t start_ns_ = 0, end_ns_ = 0;
    rusage ru_start_{}, ru_stop_{};

    std::atomic<uint64_t> pending_measured_{0};
    Counters stats_;

    // holds player data synchronization objects and queues shared by the worker threads
    std::vector<PlayerState> players_;

    std::vector<std::mutex> state_locks_;
    std::vector<int> cpu_order_;
    std::vector<std::atomic<pid_t>> worker_tids_;
    PerfCounters perf_;
    std::unordered_map<int, std::unique_ptr<Connection>> clients_;
    std::mutex work_mutex_, completion_mutex_;
    std::condition_variable work_cv_;
    std::deque<Work> work_queue_;
    std::deque<Completion> completion_queue_;
    std::vector<std::thread> worker_threads_;
    bool quitting_ = false;

    // registers a socket or event handle with the main event loop
    void add_epoll(int fd, uint32_t events) {
        epoll_event ev{}; ev.events = events; ev.data.fd = fd;
        if (epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) != 0)
            throw std::runtime_error(std::string("epoll_ctl ADD: ") + strerror(errno));
    }

    // changes which events the server watches for on an existing connection
    void mod_epoll(int fd, uint32_t events) {
        epoll_event ev{}; ev.events = events; ev.data.fd = fd;
        if (epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ev) != 0 && errno != ENOENT)
            std::cerr << "epoll_ctl MOD fd=" << fd << ": " << strerror(errno) << '\n';
    }

    // disconnects a player and removes its connection from the event loop
    void close_client(int fd) {
        epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr); close(fd); clients_.erase(fd);
    }

    // accepts new player connections and prepares them for nonblocking communication
    void accept_clients() {
        while (true) {
            int fd = accept4(listenfd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (fd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                if (errno != EINTR) std::cerr << "accept failed: " << strerror(errno) << '\n';
                break;
            }
            int one = 1; setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
            clients_.emplace(fd, std::make_unique<Connection>(next_conn_id_++));
            add_epoll(fd, EPOLLIN | EPOLLRDHUP);
            connection_peak_ = std::max(connection_peak_, clients_.size());
        }
    }

    // accepts a connection used to send benchmark commands to the server
    void accept_control() {
        while (true) {
            int fd = accept4(control_listenfd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (fd < 0) break;
            if (controlfd_ >= 0) { close(fd); continue; }
            controlfd_ = fd; control_in_.clear(); control_out_.clear();
            control_write_enabled_ = false;
            add_epoll(fd, EPOLLIN | EPOLLRDHUP);
        }
    }

    // updates whether the control connection needs to be watched for writable events
    void update_control_interest() {
        if (controlfd_ >= 0)
            mod_epoll(controlfd_, EPOLLIN | EPOLLRDHUP |
                        (control_write_enabled_ ? static_cast<uint32_t>(EPOLLOUT) : 0U));
    }

    // adds a benchmark control reply to the outgoing buffer and tries to send it
    void control_reply(const std::string &reply) {
        if (controlfd_ < 0) return;
        control_out_ += reply;
        flush_control();
    }

    // sends any pending control replies and saves the remainder if the socket is busy
    void flush_control() {
        if (controlfd_ < 0) return;
        while (!control_out_.empty()) {
            ssize_t n = send(controlfd_, control_out_.data(), control_out_.size(), MSG_NOSIGNAL);
            if (n > 0) control_out_.erase(0, static_cast<size_t>(n));
            else if (n < 0 && errno == EINTR) continue;
            else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
            else { close_control(); return; }
        }
        bool want_write = !control_out_.empty();
        if (want_write != control_write_enabled_) {
            control_write_enabled_ = want_write; update_control_interest();
        }
    }

    // closes the benchmark control connection and clears its saved messages
    void close_control() {
        if (controlfd_ < 0) return;
        epoll_ctl(epfd_, EPOLL_CTL_DEL, controlfd_, nullptr);
        close(controlfd_); controlfd_ = -1;
        control_in_.clear(); control_out_.clear();
    }

    // receives benchmark commands and handles responses or disconnections on the control socket
    void handle_control(uint32_t e) {
        if (e & EPOLLIN) {
            char buf[512];
            while (true) {
                ssize_t n = recv(controlfd_, buf, sizeof(buf), 0);
                if (n > 0) {
                    control_in_.append(buf, static_cast<size_t>(n));
                    if (control_in_.size() > 4096) { close_control(); return; }
                    while (true) {
                        auto pos = control_in_.find('\n');
                        if (pos == std::string::npos) break;
                        std::string cmd = control_in_.substr(0, pos);
                        control_in_.erase(0, pos + 1);
                        dispatch_command(cmd);
                    }
                } else if (n == 0) { close_control(); return; }
                else if (errno == EINTR) continue;
                else if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                else { close_control(); return; }
            }
        }
        if (controlfd_ >= 0 && (e & EPOLLOUT)) flush_control();
        if (controlfd_ >= 0 && (e & (EPOLLERR | EPOLLHUP | EPOLLRDHUP))) close_control();
    }

    // checks start and stop commands and manages the benchmark measurement window
    void dispatch_command(const std::string &line) {
        // starts or stops a measurement when a valid control command arrives
        if (line == "START " + cfg_.token) {
            if (measurement_active_ || stop_requested_) { control_reply("ERR already running\n"); return; }
            if (pending_measured_.load() != 0) { control_reply("ERR unfinished prior run\n"); return; }
            stats_.reset();
            stop_ns_.store(INT64_MAX, std::memory_order_release);
            std::vector<pid_t> tids;
            tids.push_back(static_cast<pid_t>(syscall(SYS_gettid)));
            for (const auto &tid : worker_tids_) tids.push_back(tid.load());
            perf_.start(tids);
            getrusage(RUSAGE_SELF, &ru_start_);
            start_ns_ = ns_now();
            measurement_active_ = true;
            control_reply("OK START\n");
            std::cerr << "MEASUREMENT START\n";
        } else if (line == "STOP " + cfg_.token) {
            if (!measurement_active_ || stop_requested_) { control_reply("ERR not measuring\n"); return; }
            end_ns_ = ns_now();
            stop_ns_.store(end_ns_, std::memory_order_release);
            measurement_active_ = false;
            getrusage(RUSAGE_SELF, &ru_stop_);
            perf_.stop();
            stop_requested_ = true;
            control_reply("OK STOP\n");
            std::cerr << "MEASUREMENT STOP\n";
        } else control_reply("ERR bad command or token\n");
    }

    // reads complete player messages and sends queued replies over a client connection
    void handle_client(int fd, uint32_t e) {
        auto it = clients_.find(fd);
        if (it == clients_.end()) return;
        Connection &c = *it->second;
        bool closed = false;
        if (e & EPOLLIN) {
            uint8_t buf[8192];
            while (true) {
                ssize_t n = recv(fd, buf, sizeof(buf), 0);
                if (n > 0) {
                    size_t pos = 0;
                    while (pos < static_cast<size_t>(n)) {
                        size_t take = std::min(FRAME_SIZE - c.in_used, static_cast<size_t>(n) - pos);
                        std::memcpy(c.in.data() + c.in_used, buf + pos, take);
                        c.in_used += take; pos += take;
                        if (c.in_used == FRAME_SIZE) {
                            accept_message(fd, c, c.in.data());
                            c.in_used = 0;
                        }
                    }
                } else if (n == 0) { closed = true; break; }
                else if (errno == EINTR) continue;
                else if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                else { closed = true; break; }
            }
        }
        if (!closed && (e & EPOLLOUT)) closed = !flush_client(fd, c);
        if (c.failed_write || (e & (EPOLLERR | EPOLLHUP | EPOLLRDHUP))) closed = true;
        if (closed) close_client(fd);
    }

    // queues an acknowledgement for a player and attempts to send it immediately
    void send_response(int fd, Connection &c, std::array<uint8_t, FRAME_SIZE> response) {
        if (c.out_sent == c.out.size()) { c.out.clear(); c.out_sent = 0; }
        c.out.insert(c.out.end(), response.begin(), response.end());
        if (!flush_client(fd, c)) c.failed_write = true;
    }

    // sends buffered player responses and keeps unsent bytes for a later retry
    bool flush_client(int fd, Connection &c) {
        while (c.out_sent < c.out.size()) {
            ssize_t n = send(fd, c.out.data() + c.out_sent, c.out.size() - c.out_sent, MSG_NOSIGNAL);
            if (n > 0) c.out_sent += static_cast<size_t>(n);
            else if (n < 0 && errno == EINTR) continue;
            else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
            else return false;
        }
        if (c.out_sent == c.out.size()) { c.out.clear(); c.out_sent = 0; }
        bool want_write = !c.out.empty();
        if (want_write != c.write_enabled) {
            c.write_enabled = want_write;
            mod_epoll(fd, EPOLLIN | EPOLLRDHUP | (want_write ? static_cast<uint32_t>(EPOLLOUT) : 0U));
        }
        return true;
    }

    // validates a player update and adds it to the worker queue when there is room
    void accept_message(int fd, Connection &c, const uint8_t *data) {
        // decodes the update and checks whether it can be queued for processing
        const uint32_t player = get_u32(data), seq = get_u32(data + 4);
        const int32_t x = static_cast<int32_t>(get_u32(data + 8));
        const int32_t y = static_cast<int32_t>(get_u32(data + 12));
        const bool measured = measurement_active_;
        if (measured) stats_.received.fetch_add(1, std::memory_order_relaxed);
        if (player == 0 || player > MAX_PLAYER_ID) {
            if (measured) stats_.rejected.fetch_add(1, std::memory_order_relaxed);
            send_response(fd, c, ack_frame(player, seq, 1)); return;
        }
        const int64_t t1 = ns_now();
        std::unique_lock<std::mutex> lock(work_mutex_);
        const int64_t t2 = ns_now();
        if (work_queue_.size() >= cfg_.max_queue) {
            lock.unlock();
            if (measured) stats_.rejected.fetch_add(1, std::memory_order_relaxed);
            send_response(fd, c, ack_frame(player, seq, 1));
            return;
        }
        if (measured) {
            pending_measured_.fetch_add(1, std::memory_order_release);
            stats_.queue_lock_ns.fetch_add(static_cast<uint64_t>(t2 - t1), std::memory_order_relaxed);
        }
        work_queue_.push_back(Work{fd, c.id, player, seq, x, y, ns_now(), measured});
        if (measured) {
            uint64_t size = work_queue_.size();
            uint64_t p = stats_.queue_peak.load(std::memory_order_relaxed);
            if (size > p) stats_.queue_peak.store(size, std::memory_order_relaxed);
        }
        lock.unlock(); work_cv_.notify_one();
    }

    // processes queued player updates and passes completed replies back to the network thread
    void worker_loop(int worker_index) {
        // records the worker thread and optionally assigns it to a specific cpu
        worker_tids_[worker_index].store(static_cast<pid_t>(syscall(SYS_gettid)));
        if (cfg_.pin_workers) {
            cpu_set_t mask; CPU_ZERO(&mask);
            CPU_SET(cpu_order_[worker_index % cpu_order_.size()], &mask);
            if (pthread_setaffinity_np(pthread_self(), sizeof(mask), &mask) != 0)
                std::cerr << "WARNING: pthread_setaffinity_np failed for worker " << worker_index << '\n';
        }
        while (true) {
            // takes the next update from the shared queue and measures how long it waited
            const int64_t lock_start = ns_now();
            std::unique_lock<std::mutex> lock(work_mutex_);
            const int64_t lock_finish = ns_now();

            work_cv_.wait(lock, [&] { return quitting_ || !work_queue_.empty(); });
            if (quitting_ && work_queue_.empty()) return;
            Work w = work_queue_.front(); work_queue_.pop_front();
            const int64_t dequeued = ns_now();
            lock.unlock();
            const int64_t process_start = ns_now();
            const int64_t state_lock_start = ns_now();
            // locks the player state so simultaneous updates cannot overwrite each other
            std::mutex &state_mutex = state_locks_[w.player % state_locks_.size()];
            int64_t state_locked;
            {
                std::lock_guard<std::mutex> sl(state_mutex);
                state_locked = ns_now();

                PlayerState &s = players_[w.player];
                if (static_cast<int32_t>(w.seq - s.last_seq) > 0) {
                    s.x = w.x; s.y = w.y; s.last_seq = w.seq;
                }
            }
            const int64_t process_done = ns_now();

            // records processing and waiting times for work completed inside the measurement window
            if (w.measured && process_done <= stop_ns_.load(std::memory_order_acquire)) {
                stats_.completed.fetch_add(1, std::memory_order_relaxed);
                stats_.queue_wait_ns.fetch_add(static_cast<uint64_t>(dequeued - w.queued_ns), std::memory_order_relaxed);
                stats_.queue_lock_ns.fetch_add(static_cast<uint64_t>(lock_finish - lock_start), std::memory_order_relaxed);
                stats_.state_lock_ns.fetch_add(static_cast<uint64_t>(state_locked - state_lock_start), std::memory_order_relaxed);
                stats_.processing_ns.fetch_add(static_cast<uint64_t>(process_done - process_start), std::memory_order_relaxed);
            }
            // passes the finished response to the network thread and wakes it if needed
            Completion completed{w.fd, w.connection_id, ack_frame(w.player, w.seq, 0)};
            bool should_wake = false;
            {
                std::lock_guard<std::mutex> completion_lock(completion_mutex_);

                should_wake = completion_queue_.empty();
                completion_queue_.push_back(std::move(completed));
            }
            if (should_wake) {
                uint64_t one = 1;
                (void)write(eventfd_, &one, sizeof(one));
            }
            if (w.measured) pending_measured_.fetch_sub(1, std::memory_order_release);
        }
    }

    // collects worker results and sends replies only to the connections they came from
    void handle_completions() {
        uint64_t v;
        while (read(eventfd_, &v, sizeof(v)) > 0) {}
        // moves completed work out of the shared queue before sending replies
        std::deque<Completion> ready;
        {
            std::lock_guard<std::mutex> lock(completion_mutex_);
            ready.swap(completion_queue_);
        }
        for (auto &done : ready) {
            auto it = clients_.find(done.fd);
            if (it == clients_.end() || it->second->id != done.connection_id) continue;
            send_response(done.fd, *it->second, done.bytes);
            if (it->second->failed_write) close_client(done.fd);
        }
    }

    // calculates the final benchmark results and writes them to a csv file
    void finish_measurement() {
        stop_requested_ = false;
        // calculates throughput cpu use and average time spent in each processing stage
        const double seconds = static_cast<double>(end_ns_ - start_ns_) / 1e9;
        const uint64_t n = stats_.completed.load();
        const uint64_t incoming = stats_.received.load();
        const uint64_t rejected = stats_.rejected.load();
        const double used_cpu = cpu_seconds(ru_stop_) - cpu_seconds(ru_start_);

        const double cores = (seconds > 0 ? used_cpu / seconds : 0.0);
        const double avg_queue = n ? stats_.queue_wait_ns.load() / (1000.0*n) : 0.0;
        const double avg_queue_lock = n ? stats_.queue_lock_ns.load() / (1000.0*n) : 0.0;
        const double avg_state_lock = n ? stats_.state_lock_ns.load() / (1000.0*n) : 0.0;
        const double avg_processing = n ? stats_.processing_ns.load() / (1000.0*n) : 0.0;
        // writes the benchmark metrics to the configured csv output file
        std::ofstream file(cfg_.output);
        if (!file) {
            std::cerr << "ERROR: cannot open output file " << cfg_.output << '\n';
        } else {
            file << "workers,window_s,received,completed,rejected,throughput_msg_s,"
                 << "cpu_cores_used,cpu_percent_one_core,cpu_percent_allowed_cpus,"
                 << "avg_queue_wait_us,avg_queue_lock_us,avg_state_lock_us,avg_processing_us,"
                 << "voluntary_ctx_switches,involuntary_ctx_switches,peak_queue_depth,peak_clients,allowed_logical_cpus,"
                 << "perf_available,cycles,instructions,cache_misses,perf_context_switches,cycles_per_message,ipc\n";
            file << std::fixed << std::setprecision(6)
                 << cfg_.workers << ',' << seconds << ',' << incoming << ',' << n << ',' << rejected
                 << ',' << (seconds > 0 ? n/seconds : 0.0)
                 << ',' << cores << ',' << cores*100
                 << ',' << cores*100/cpu_order_.size()
                 << ',' << avg_queue << ',' << avg_queue_lock
                 << ',' << avg_state_lock << ',' << avg_processing
                 << ',' << (ru_stop_.ru_nvcsw - ru_start_.ru_nvcsw)
                 << ',' << (ru_stop_.ru_nivcsw - ru_start_.ru_nivcsw)
                 << ',' << stats_.queue_peak.load() << ',' << connection_peak_
                 << ',' << cpu_order_.size() << ',' << (perf_.available() ? 1 : 0);
            if (perf_.available()) {
                file << ',' << perf_.cycles() << ',' << perf_.instructions()
                     << ',' << perf_.cache_misses() << ',' << perf_.context_switches()
                     << ',' << (n ? static_cast<double>(perf_.cycles()) / n : 0.0)
                     << ',' << (perf_.cycles() ? static_cast<double>(perf_.instructions()) / perf_.cycles() : 0.0);
            } else file << ",,,,,,";
            file << '\n';
        }
        // prints a short benchmark summary and tells the controller the run is finished
        std::cerr << std::fixed << std::setprecision(2)
                  << "RESULT workers=" << cfg_.workers << " completed=" << n
                  << " throughput=" << (seconds > 0 ? n/seconds : 0.0)
                  << " msg/s CPU_cores=" << cores
                  << " queue_wait=" << avg_queue << " us\n";
        control_reply("DONE\n");
    }
};

// starts the server with the chosen options and reports any startup or runtime errors
int main(int argc, char **argv) {
    try {
        std::signal(SIGINT, signal_handler);
        std::signal(SIGTERM, signal_handler);
        Config cfg = parse_args(argc, argv);
        Server server(std::move(cfg));
        server.run();
    } catch (const std::exception &e) {
        std::cerr << "ERROR: " << e.what() << '\n'; return 1;
    }
    return 0;
}