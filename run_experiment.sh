#!/usr/bin/env bash
# Run the CS3339 game-server benchmark locally with a fresh server per trial.
set -Eeuo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

# Override any parameter by prefixing the command with KEY=value.
WORKERS="${WORKERS:-1 2 4 8 16 32}"
PLAYERS="${PLAYERS:-10 100 500 1000}"
REPEATS="${REPEATS:-1}"
RATE="${RATE:-60}"
WARMUP="${WARMUP:-5}"
DURATION="${DURATION:-30}"
DRAIN="${DRAIN:-3}"
STOP_WAIT="${STOP_WAIT:-60}"
MAX_INFLIGHT="${MAX_INFLIGHT:-32}"
MAX_QUEUE="${MAX_QUEUE:-200000}"
PIN_WORKERS="${PIN_WORKERS:-1}"
PORT="${PORT:-9000}"
CONTROL_PORT="${CONTROL_PORT:-$((PORT + 1))}"
RESULTS_ROOT="${RESULTS_ROOT:-$ROOT/results}"

for exe in g++ timeout awk; do
    command -v "$exe" >/dev/null || { echo "Missing required tool: $exe" >&2; exit 1; }
done
for file in game_server.cpp load_generator.cpp; do
    [[ -f "$ROOT/$file" ]] || { echo "Missing source file: $ROOT/$file" >&2; exit 1; }
done
[[ "$REPEATS" =~ ^[1-9][0-9]*$ ]] || { echo 'REPEATS must be a positive integer.' >&2; exit 1; }
[[ "$PORT" =~ ^[0-9]+$ && "$CONTROL_PORT" =~ ^[0-9]+$ ]] || { echo 'Invalid port number.' >&2; exit 1; }

# Running both client and server on one host needs ~2 file descriptors/player.
ulimit -n 65535 2>/dev/null || true
max_players=0
for p in $PLAYERS; do
    [[ "$p" =~ ^[1-9][0-9]*$ ]] || { echo "Invalid player count: $p" >&2; exit 1; }
    if (( p > max_players )); then max_players="$p"; fi
done
open_limit="$(ulimit -n)"
needed_fds=$((max_players * 2 + 128))
if [[ "$open_limit" != 'unlimited' ]] && (( open_limit < needed_fds )); then
    echo "ERROR: per-process file descriptor limit is $open_limit; need roughly $needed_fds." >&2
    echo 'Try: ulimit -n 8192 (or ask your administrator to raise the limit).' >&2
    exit 1
fi

RUN_DIR="$RESULTS_ROOT/$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$RUN_DIR/bin"
server_pid=''
stop_server() {
    if [[ -n "$server_pid" ]]; then
        kill "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
        server_pid=''
    fi
}
trap stop_server EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

{
    echo "Start UTC: $(date -u -Is)"
    echo "Hostname: $(hostname)"
    echo "Allowed logical CPUs: $(nproc)"
    echo "Open file limit: $open_limit"
    echo "Workers: $WORKERS"
    echo "Players: $PLAYERS"
    echo "Repeats: $REPEATS"
    echo "Rate: $RATE; warmup: $WARMUP; duration: $DURATION"
    echo "Worker affinity enabled: $PIN_WORKERS"
    echo "Kernel: $(uname -a)"
    echo 'Compiler:'
    g++ --version | head -n 1
    echo 'CPU:'
    if command -v lscpu >/dev/null; then lscpu; fi
    echo 'Perf restrictions:'
    if [[ -r /proc/sys/kernel/perf_event_paranoid ]]; then
        cat /proc/sys/kernel/perf_event_paranoid
    fi
} > "$RUN_DIR/system.txt"

printf 'Building C++17 binaries...\n'
g++ -std=c++17 -O2 -Wall -Wextra -Wpedantic -pthread "$ROOT/game_server.cpp" -o "$RUN_DIR/bin/game_server"
g++ -std=c++17 -O2 -Wall -Wextra -Wpedantic -pthread "$ROOT/load_generator.cpp" -o "$RUN_DIR/bin/load_generator"

printf 'workers,players,repeat,status,exit_code,folder\n' > "$RUN_DIR/trial_index.csv"
client_header=0
server_header=0
failures=0
trial_no=0
# Include a generous bound in case connections or the server control reply stall.
run_timeout="$(awk -v a="$WARMUP" -v b="$DURATION" -v c="$DRAIN" -v d="$STOP_WAIT" 'BEGIN {printf "%.0f", a+b+c+d+75}')"

for workers in $WORKERS; do
    [[ "$workers" =~ ^[1-9][0-9]*$ ]] || { echo "Invalid worker count: $workers" >&2; exit 1; }
    for players in $PLAYERS; do
        for ((repeat=1; repeat<=REPEATS; repeat++)); do
            trial_no=$((trial_no + 1))
            tag="w${workers}_p${players}_r${repeat}"
            folder="$RUN_DIR/$tag"
            mkdir -p "$folder"
            token="$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
            printf '\n[%d] Workers=%s Players=%s Repeat=%s\n' "$trial_no" "$workers" "$players" "$repeat"

            "$RUN_DIR/bin/game_server" \
                --bind 127.0.0.1 --port "$PORT" --control-port "$CONTROL_PORT" \
                --workers "$workers" --max-queue "$MAX_QUEUE" \
                --pin-workers "$PIN_WORKERS" --control-token "$token" \
                --output "$folder/server.csv" \
                > "$folder/server.stdout.log" 2> "$folder/server.log" &
            server_pid=$!

            ready=0
            for ((attempt=1; attempt<=50; attempt++)); do
                if grep -q '^Listening on ' "$folder/server.log"; then
                    ready=1
                    break
                fi
                if ! kill -0 "$server_pid" 2>/dev/null; then break; fi
                sleep 0.1
            done
            if (( ready == 0 )); then
                echo "Server failed to start. See $folder/server.log" >&2
                rc=90
            else
                set +e
                timeout --signal=TERM --kill-after=5s "$run_timeout" \
                    "$RUN_DIR/bin/load_generator" \
                    --host 127.0.0.1 --port "$PORT" --control-port "$CONTROL_PORT" \
                    --control-token "$token" --workers "$workers" \
                    --players "$players" --rate "$RATE" \
                    --warmup "$WARMUP" --duration "$DURATION" \
                    --drain "$DRAIN" --stop-wait "$STOP_WAIT" \
                    --max-inflight "$MAX_INFLIGHT" \
                    --csv "$folder/client.csv" \
                    > "$folder/client.stdout.log" 2> "$folder/client.log"
                rc=$?
                set -e
            fi

            stop_server
            status='ok'
            if (( rc != 0 )) || [[ ! -s "$folder/server.csv" || ! -s "$folder/client.csv" ]] || \
               ! awk -F, 'NR==2 {good=($20==1 && $11==0 && $12==0 && $7>0)} END {exit !(NR==2 && good)}' "$folder/client.csv" || \
               ! awk -F, 'NR==2 {good=($2>0 && $3>0 && $4>0)} END {exit !(NR==2 && good)}' "$folder/server.csv"; then
                status='failed'
                failures=$((failures + 1))
                echo "FAILED (exit=$rc). Check $folder/client.log and server.log" >&2
            else
                if ((client_header == 0)); then
                    { printf 'repeat,'; head -n 1 "$folder/client.csv"; } > "$RUN_DIR/client_summary.csv"
                    client_header=1
                fi
                if ((server_header == 0)); then
                    { printf 'players,repeat,'; head -n 1 "$folder/server.csv"; } > "$RUN_DIR/server_summary.csv"
                    server_header=1
                fi
                tail -n +2 "$folder/client.csv" | while IFS= read -r line; do
                    printf '%s,%s\n' "$repeat" "$line"
                done >> "$RUN_DIR/client_summary.csv"
                tail -n +2 "$folder/server.csv" | while IFS= read -r line; do
                    printf '%s,%s,%s\n' "$players" "$repeat" "$line"
                done >> "$RUN_DIR/server_summary.csv"
                echo "OK: $folder/client.csv and server.csv"
            fi
            printf '%s,%s,%s,%s,%s,%s\n' "$workers" "$players" "$repeat" "$status" "$rc" "$tag" >> "$RUN_DIR/trial_index.csv"
        done
    done
done

printf '\nExperiment complete. Results: %s\n' "$RUN_DIR"
echo 'Summary files: trial_index.csv, client_summary.csv, server_summary.csv, system.txt'
if (( failures > 0 )); then
    echo "WARNING: $failures trial(s) failed; inspect their log files." >&2
    exit 1
fi
