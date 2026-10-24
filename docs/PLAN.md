# fleet build plan

Created: 2026-10-24 15:18 EDT

## Objective

A Borg-style cluster scheduler in C++20: a central control plane decides
which node runs each task, node agents run tasks as real processes, and
when a node dies its tasks are rescheduled elsewhere. Its distinguishing
feature is that it runs **stateful** jobs: it schedules adstack's
streamkit pipelines and, when a node fails, restarts them on another node
from their last checkpoint, so the job's output is still exactly-once.

```
            fleetctl (CLI)
                 |  gRPC: submit / list / kill / drain
                 v
        +-------------------+      write-ahead log + snapshot
        |  fleetmaster      | ---> (state survives master restart)
        |  scheduler loop   |
        |  reconciler       |
        +-------------------+
          ^ gRPC heartbeats| gRPC assign / stop
          |   + task state v
   +-----------+  +-----------+  +-----------+
   | fleetlet  |  | fleetlet  |  | fleetlet  |   one per node
   | docker run|  | docker run|  | docker run|   (containers, cgroup limits)
   +-----------+  +-----------+  +-----------+
```

Target reader: a Google infra interviewer (Borg, cluster management) who
will ask "how does placement work", "what happens when a node dies", "what
if the master dies", and "how did you test that".

## How it differs from a typical intern scheduler

- **Stateful recovery, end to end.** Rescheduling a stateless task is
  easy. fleet reschedules a streamkit job whose checkpoint lives in shared
  storage, and the resumed job's output is checked against adstack's batch
  oracle. The headline test kills nodes mid-job and still gets identical
  output.
- **Replicated control plane with its own Raft, not etcd.** fleetmaster
  runs as 3 replicas that agree on every state change through Raft (leader
  election, log replication, snapshots), the way Borg's Borgmaster runs 5
  Paxos-replicated replicas. Losing any one master machine loses nothing.
  The single-node write-ahead log (m4) becomes the Raft log (m4b).
- **gRPC + Protocol Buffers** for every interface (agent to master,
  CLI to master), the same stack Google uses internally. Services and
  messages are defined in `proto/fleet.proto`.
- **Docker as the task runtime.** The agent starts each task with
  `docker run --cpus --memory`, so CPU and memory limits are enforced by
  Linux cgroups (inside Docker's VM on macOS). A plain-process runtime is
  kept for unit tests and CI, behind the same `Runtime` interface.

## Components

| component | what it is |
|---|---|
| `proto/` | `fleet.proto`: gRPC services (`Master`, `Agent`) and Protobuf messages; CMake generates C++ stubs |
| `master/` | fleetmaster: cluster state, scheduler, reconciler, failure detector, WAL + snapshot, TCP server |
| `agent/` | fleetlet: registers with the master, heartbeats, runs tasks through a `Runtime` interface: `DockerRuntime` (docker run with --cpus/--memory, cgroup-enforced) or `ProcessRuntime` (fork/exec + rlimits, for tests and CI), reports exits |
| `ctl/` | fleetctl: submit, list, kill, drain node, show events |
| `sim/` | in-process simulated cluster (hundreds of fake nodes) for placement and scale tests without real processes |
| `bench/` | scripts that produce every number in the docs |

## Scheduling model

- Node: id, capacity (milli-CPU, memory MB), labels, state (ready, draining, dead).
- Task: id, job, priority (0 to 3: batch, normal, production, critical), request (milli-CPU, memory), command line, restart policy, optional checkpoint dir.
- Placement: feasibility filter (enough free resources, labels match, node ready), then score by **best-fit** on the dominant resource (pack tightly, leave large holes for large tasks). Ties broken deterministically. NOTES.md compares best-fit, worst-fit (spreading) and first-fit, with measurements from `sim/`.
- Quotas: per-job caps on total CPU and memory; submissions over quota are rejected at admission, not queued forever.
- Preemption: if a task cannot fit, find the cheapest set of strictly lower-priority victims on one node whose eviction makes it fit (fewest victims, then lowest priority); evicted tasks go back to pending. Never preempt equal or higher priority.
- No overcommit: the sum of requests on a node never exceeds capacity. Checked as an invariant after every scheduling decision in tests.

## Failure handling

- Heartbeats every 500 ms; a node missing 3 consecutive heartbeats is suspect, 6 is dead (configurable). Dead node's tasks go back to pending and are rescheduled.
- A node that comes back after being declared dead is told to kill whatever it is running (fencing by node epoch), so a task never runs twice.
- Reconciler: every tick, compare desired state (master's view) with actual state (agents' reports) and issue start/stop to converge. This is the single mechanism behind restarts, rescheduling and drain.
- Master restart: replay snapshot + WAL, then wait one heartbeat window for agents to report what they actually run before scheduling, so nothing is double-started.

## Milestones

| m | what | done when |
|---|---|---|
| m1 | core model + scheduler as a pure library: nodes, tasks, best-fit placement, quotas, preemption, invariant checker; `sim/` driver | unit tests; sim places 5,000 tasks on 100 nodes with zero overcommit; placement-strategy comparison table |
| m2 | gRPC services + fleetmaster + fleetlet + fleetctl; ProcessRuntime; heartbeats | local 3-agent cluster runs real tasks end to end over gRPC; integration tests |
| m2b | DockerRuntime: tasks as containers with cgroup CPU/memory limits; OOM-kill and CPU throttling detected and reported | with Docker installed: a task over its memory limit is OOM-killed and reported, a CPU hog is throttled to its share (measured); without Docker the runtime is skipped cleanly |
| m3 | failure detection, rescheduling, fencing, drain, restart policies with backoff | `bench/node_failure.sh`: kill an agent with SIGKILL, measure time to detect and to running elsewhere |
| m4 | WAL + snapshot; master crash recovery with the report-before-schedule rule | `bench/master_failure.sh`: SIGKILL the master mid-workload 20 times; no task lost, none running twice |
| m4b | Raft-replicated control plane: 3 fleetmaster replicas; leader election, log replication of the m4 WAL records, snapshots/log compaction; agents and fleetctl discover the leader (gRPC redirect); only the leader schedules | `bench/leader_failover.sh`: SIGKILL the leader mid-workload 20 times; a new leader is elected and scheduling resumes, no committed task lost, none running twice; election time median/max; partition test (isolate the leader) shows no split brain |
| m5 | stateful jobs: run adstack streamkit tasks with checkpoint dirs; on node death the task resumes elsewhere with `--resume` | `bench/stateful_failover.sh`: kill nodes during streamkit jobs; every job's output identical to adstack's oracle |
| m6 | scale and benchmarks: scheduling latency and throughput in `sim/` at 100 / 1,000 / 10,000 nodes; failover times; BENCHMARKS.md | numbers reproducible from bench scripts |

## Ground rules (same as adstack)

1. Every milestone appends to `NOTES.md`: decision, alternatives, why, cost, verification.
2. Every number comes from a script in `bench/`, with machine and load average recorded.
3. Zero warnings under `-Wall -Wextra -Wpedantic -Wshadow -Wconversion`; ASan clean; TSan clean for anything threaded.
4. GoogleTest unit tests per milestone.
5. C++20, gRPC and Protocol Buffers (found via CMake `find_package`; on Edison's Mac they come from `~/anaconda3`), Docker CLI at runtime for DockerRuntime. GoogleTest for tests. adstack is used as an external binary in m5, not linked.
6. Long-running commands print progress at least every 30 seconds or run in the background, so automated sessions do not stall.

## Resume target (numbers filled in from BENCHMARKS.md at the end)

**fleet: Distributed Cluster Scheduler** | C++20, gRPC, Protocol Buffers, Docker, Raft
- Built a Borg-style scheduler that places jobs across machines by CPU and memory, preempts low-priority work, and reschedules jobs when a machine fails
- Replicates the scheduler across 3 nodes with Raft so it survives losing its leader, and restarts streaming jobs from their last checkpoint after machine failures with output verified identical across N forced crashes
