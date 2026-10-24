# fleet

A Borg-style cluster scheduler in C++20 that runs stateful jobs: a control
plane places tasks on nodes by CPU and memory, preempts lower-priority
work, and when a node dies reschedules its tasks elsewhere, resuming
streaming jobs from their last checkpoint.

Status: m2 done. m1: core model and scheduler as a pure library,
`fleetsim` simulator, placement-strategy comparison. m2: gRPC control
plane (`proto/fleet.proto`), `fleetmaster`, `fleetlet` agents with a
fork/exec `ProcessRuntime`, the `fleetctl` CLI, and a local 3-agent
cluster script. Decisions and measurements are in [NOTES.md](NOTES.md).
m2b (Docker runtime with cgroup limits) is next. See
[docs/PLAN.md](docs/PLAN.md).

## Run a local cluster

```
bench/local_cluster.sh      # master + 3 agents, a mix of real tasks, clean teardown
```

Or by hand:

```
./build/bin/fleetmaster --listen 127.0.0.1:7070 &
./build/bin/fleetlet --name n1 --cpu 4000 --mem 4096 &
./build/bin/fleetctl submit --job hello --cpu 500 --mem 64 --count 3 -- /bin/sh -c 'echo hi'
./build/bin/fleetctl list
./build/bin/fleetctl kill --job hello
./build/bin/fleetctl drain n1
```

CPU is in milli-CPU (1000 = one core), memory in MB. Task logs go to
`/private/tmp/fleet/<node>/<job>.t<task>.a<attempt>.log`.

## Try the simulator

```
./build/bin/fleetsim --nodes 100 --tasks 5000 --compare --seeds 10
```

## Build

Needs gRPC and Protocol Buffers with their CMake package files. If
`~/anaconda3` has them, the build uses it automatically; otherwise pass
`-DCMAKE_PREFIX_PATH=...`.

```
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build
ctest --test-dir build
```
