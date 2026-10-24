# fleet

A Borg-style cluster scheduler in C++20 that runs stateful jobs: a control
plane places tasks on nodes by CPU and memory, preempts lower-priority
work, and when a node dies reschedules its tasks elsewhere, resuming
streaming jobs from their last checkpoint.

Status: m1 done (core model and scheduler as a pure library, `fleetsim`
simulator, placement-strategy comparison in [NOTES.md](NOTES.md)). m2
(gRPC master, agents, CLI) is next. See [docs/PLAN.md](docs/PLAN.md).

## Try the simulator

```
./build/bin/fleetsim --nodes 100 --tasks 5000 --compare --seeds 10
```

## Build

```
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build
ctest --test-dir build
```
