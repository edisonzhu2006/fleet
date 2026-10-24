# fleet

A Borg-style cluster scheduler in C++20 that runs stateful jobs: a control
plane places tasks on nodes by CPU and memory, preempts lower-priority
work, and when a node dies reschedules its tasks elsewhere, resuming
streaming jobs from their last checkpoint.

Status: m2b done. m1: core model and scheduler as a pure library,
`fleetsim` simulator, placement-strategy comparison. m2: gRPC control
plane (`proto/fleet.proto`), `fleetmaster`, `fleetlet` agents with a
fork/exec `ProcessRuntime`, the `fleetctl` CLI, and a local 3-agent
cluster script. m2b: `DockerRuntime` (`fleetlet --runtime=docker`), which
runs each task as a container with cgroup CPU and memory limits and
reports OOM kills; measured with `bench/docker_limits.sh` (a 0.5-core task
held to 50.0% of a core, an over-limit task reported as `oom`, container
start 118 to 129 ms median). Decisions and measurements are in
[NOTES.md](NOTES.md). m3 (failure detection, rescheduling, fencing) is
next. See [docs/PLAN.md](docs/PLAN.md).

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

## Run tasks as containers

Needs Docker (Docker Desktop or OrbStack on macOS). fleetlet checks that
the daemon answers at startup and exits if it does not.

```
./build/bin/fleetlet --name d1 --cpu 4000 --mem 4096 --runtime=docker &
./build/bin/fleetctl submit --job hog --cpu 500 --mem 32 --image busybox:1.36 -- \
    /bin/sh -c 'head -c 256m /dev/zero | tail'
./build/bin/fleetctl list          # EXIT column shows "oom"
bench/docker_limits.sh             # memory, CPU and start-latency checks; skips without Docker
```

Each task runs as `docker run -d --name fleet-<task>-<attempt>-<incarnation>
--init --cpus=<milli/1000> --memory=<mb>m --memory-swap=<mb>m <image> <cmd>`;
the default image is `busybox:1.36` (`--docker-image` changes it). The
container's output is copied to the task's log file when it exits.

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
