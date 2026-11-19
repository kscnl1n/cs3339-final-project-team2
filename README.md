## AMD EPYC Game Server Load Simulation Project

This repository provides a minimal TCP multiplayer server and open-loop load generator to study how **worker-thread count**
and **concurrent player population** affect message throughput, CPU use, queue delay,
mutex contention, and acknowledgment latency.

All code runs on **Linux** with **C++17**. 

## How to Run
All team members participated in the writing of functions and methods for the benchmarks, which are measured on Brooks. to run each code on its own, run:


There is a shell script (run_experiment.sh) which runs the entire experiment autonomously.

Flags:
These flags can be used to run specific parts of the benchmark with the shell script. If you 
