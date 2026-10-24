# fleet

A Borg-style cluster scheduler in C++20 that runs stateful jobs: a control
plane places tasks on nodes by CPU and memory, preempts lower-priority
work, and when a node dies reschedules its tasks elsewhere, resuming
streaming jobs from their last checkpoint.

Status: m4 done. m1: core model and scheduler as a pure library,
`fleetsim` simulator, placement-strategy comparison. m2: gRPC control
plane (`proto/fleet.proto`), `fleetmaster`, `fleetlet` agents with a
fork/exec `ProcessRuntime`, the `fleetctl` CLI, and a local 3-agent
cluster script. m2b: `DockerRuntime` (`fleetlet --runtime=docker`), which
runs each task as a container with cgroup CPU and memory limits and
reports OOM kills; measured with `bench/docker_limits.sh` (a 0.5-core task
held to 50.0% of a core, an over-limit task reported as `oom`, container
start 118 to 129 ms median). m3: failure detection (a node is suspect
after 3 missed heartbeats and dead after 6; its tasks are rescheduled),
fencing by a monotonically increasing per-registration epoch, a reconciler
that carries out restarts, rescheduling and drain through one
desired-vs-reported diff, per-task restart policies (never / on-failure /
always) with capped exponential backoff, graceful drain (stop, then move),
and cleanup of tasks a crashed fleetlet left behind; measured with
`bench/node_failure.sh` (SIGKILL an agent, 10 trials: declared dead in
a median 2,771 ms, max 2,950 ms; all four of its tasks running elsewhere
in a median 3,107 ms, max 3,440 ms). m4: the master survives its own
crash. Every state change is a Protobuf record (`proto/wal.proto`) in a
length-prefixed, CRC-32C-checksummed write-ahead log, synced with
F_FULLFSYNC before any reply that depends on it leaves the process, one
sync per scheduler iteration or RPC (group commit: 1,000 placements commit
in about 3 ms instead of about 3.1 s with a sync per record); snapshots
with log rotation; recovery is snapshot plus replay with a torn tail
discarded, then report-before-schedule and reconciliation against the
agents' first reports. `bench/master_failure.sh` SIGKILLs the master 20
times mid-workload: no acknowledged task lost, no task ever running twice
at once, every agent keeps its registration; the master serves RPCs again
about 30 ms after the kill and is scheduling again, once every agent has
reported, a median of 241 ms and 340 ms after it in two runs of 20 (max
525 ms; NOTES.md).
Decisions and measurements are in [NOTES.md](NOTES.md). m4b (Raft
replication of the same log) is next. See [docs/PLAN.md](docs/PLAN.md).

## Run a local cluster

```
bench/local_cluster.sh      # master + 3 agents, a mix of real tasks, clean teardown
bench/node_failure.sh       # SIGKILL an agent 10 times: time to dead, time to running elsewhere
bench/master_failure.sh     # SIGKILL the master 20 times mid-workload: nothing lost, nothing doubled
bench/wal_commit.sh         # sync latency, group commit vs per-record sync, replay vs snapshot
```

Or by hand:

```
./build/bin/fleetmaster --listen 127.0.0.1:7070 &
./build/bin/fleetlet --name n1 --cpu 4000 --mem 4096 &
./build/bin/fleetctl submit --job hello --cpu 500 --mem 64 --count 3 -- /bin/sh -c 'echo hi'
./build/bin/fleetctl submit --job web --cpu 500 --mem 64 --restart always -- /bin/sh -c 'sleep 5'
./build/bin/fleetctl list
./build/bin/fleetctl kill --job hello
./build/bin/fleetctl drain n1     # no new tasks on n1; each of its tasks is stopped, then moved
```

To keep the master's state across restarts, give it a state directory:

```
./build/bin/fleetmaster --listen 127.0.0.1:7070 --state-dir /private/tmp/fleet/state &
```

It writes `wal` (the log) and `snapshot` there, and on start rebuilds its
state from them before it opens the port. Agents need no change: their
next heartbeat to the restarted master is accepted under the same epoch.
`--wal-sync full|fsync|none` (default full: F_FULLFSYNC on macOS),
`--wal-per-record` (one sync per record, for comparison),
`--snapshot-every N` (records between snapshots, default 10000) and
`--recovery-window-ms N` (longest wait for agents to report after a
restart, default 1000) tune it. Without `--state-dir` the master keeps
state in memory only, as before m4.

Failure handling is set on the master: `--heartbeat-ms 500 --suspect-after 3
--dead-after 6 --tick-ms 100` (the defaults) and, for restart policies,
`--backoff-base-ms 1000 --backoff-max-ms 60000 --backoff-reset-ms 60000`.
`fleetctl list` shows a silent node as `suspect`, then `dead`, and a task
waiting out its backoff as `backoff Ns` with its restart count.

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

Each task runs as `docker run -d --name fleet-<task>-<attempt>-<epoch>
--init --label=fleet.owner=<node> --cpus=<milli/1000> --memory=<mb>m
--memory-swap=<mb>m <image> <cmd>`;
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
