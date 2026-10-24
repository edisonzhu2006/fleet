# fleet

A Borg-style cluster scheduler in C++20 that runs stateful jobs: a control
plane places tasks on nodes by CPU and memory, preempts lower-priority
work, and when a node dies reschedules its tasks elsewhere, resuming
streaming jobs from their last checkpoint.

Status: under construction. See [docs/PLAN.md](docs/PLAN.md).

## Build

```
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build
ctest --test-dir build
```
