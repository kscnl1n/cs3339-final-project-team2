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


// get timestamp
static int64_t ns_now() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        Clock::now().time_since_epoch()).count();
}

// 
static void signal_handler(int) { g_exit_requested.store(true); }
static uint32_t get_u32(const uint8_t *p) {
    uint32_t net; std::memcpy(&net, p, 4); return ntohl(net);
}

// convert between 32 bit format our computer expects and the network format
static void put_u32(uint8_t *p, uint32_t value) {
    uint32_t net = htonl(value); std::memcpy(p, &net, 4);
}

// this doesn't work - I will fix it later, this is meant to convert our packets
// to the format the network needs but I was running this earlier and 
// I think I found a bug here - Kay
static std::array<uint8_t, FRAME_SIZE> ack_frame(uint32_t player, uint32_t seq, uint32_t status) {
    std::array<uint8_t, FRAME_SIZE> a{};
    put_u32(a.data(), player); put_u32(a.data() + 4, seq);
    put_u32(a.data() + 8, status); put_u32(a.data() + 12, 0);
    return a;
}

// socket listener to build the response returned to the client
/* THIS DOES THE FOLLOWING:
lls socket() to create an IPv4 TCP socket
Uses SOCK_NONBLOCK so socket operations do not ordinarily block the I/O thread
Uses SOCK_CLOEXEC so the descriptor will be closed across exec
Enables SO_REUSEADDR, useful when restarting the server
Converts the supplied IP address with inet_pton()
Calls bind() to attach the socket to an IP address and port
Calls listen() with a backlog argument of 8192
Returns the socket's file descriptor
*/

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


static double time_seconds(const timeval &tv) { return tv.tv_sec + tv.tv_usec / 1e6; }
static double cpu_seconds(const rusage &ru) {
    return time_seconds(ru.ru_utime) + time_seconds(ru.ru_stime);
}


static int read_int_file(const std::string &path) {
    std::ifstream f(path); int x = -1; if (f) f >> x; return x;
}


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

// Hardware counters are optional: kernel perf_event_paranoid or container
// permissions may prevent opening them. The core benchmark remains usable.
class PerfCounters {
public:
private:
};

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

// Game server inputs
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

// Basic argument parser
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

// Structs to contain the player states as they move throughout the world
struct Work {
    int fd;
    uint64_t connection_id;
    uint32_t player, seq;
    int32_t x, y;
    int64_t queued_ns;
    bool measured;
};
struct Completion {
    int fd;
    uint64_t connection_id;
    std::array<uint8_t, FRAME_SIZE> bytes;
};
struct PlayerState { int32_t x = 0, y = 0; uint32_t last_seq = 0; };
struct Connection {
    uint64_t id;
    std::array<uint8_t, FRAME_SIZE> in{};
    size_t in_used = 0;
    std::vector<uint8_t> out;
    size_t out_sent = 0;
    bool write_enabled = false;
    bool failed_write = false;
    explicit Connection(uint64_t cid) : id(cid) { out.reserve(256); }
};

// Struct to store counters
struct Counters {
    std::atomic<uint64_t> received{0}, rejected{0}, completed{0};
    std::atomic<uint64_t> queue_wait_ns{0}, queue_lock_ns{0}, state_lock_ns{0}, processing_ns{0};
    std::atomic<uint64_t> queue_peak{0};
    void reset() {
        received.store(0); rejected.store(0); completed.store(0);
        queue_wait_ns.store(0); queue_lock_ns.store(0);
        state_lock_ns.store(0); processing_ns.store(0); queue_peak.store(0);
    }
};








class Server {
public:

    void run() {
    }
private:
};






int main(int argc, char **argv) {
    try {
        server.run();
    } catch (const std::exception &e) {
        std::cerr << "ERROR: " << e.what() << '\n'; return 1;
    }
    return 0;
}
