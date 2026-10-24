# Design notes

Running log of decisions. Every entry answers a "why X instead of Y"
question, names the alternative, the cost, and how it was verified.

## 2026-10-24: scope

**Raft-replicated master, implemented here, instead of etcd (revised
2026-10-24).** First built as a single master with a write-ahead log (m4),
then replicated across 3 replicas with Raft (m4b). A single master survives
a process crash but not losing its machine or disk; replication with a
majority quorum survives any one replica failing, which is why Borgmaster
runs 5 Paxos replicas. Writing Raft here instead of storing state in etcd
keeps the scheduler's state machine and its replicated log in one process
(no extra network hop per decision) and makes the consensus layer
something to explain rather than a black box. Cost: Raft is easy to get
subtly wrong, so m4b carries the heaviest test burden (election safety,
log matching, a partition test for split brain).

**gRPC and Protocol Buffers instead of a hand-rolled protocol (revised
2026-10-24).** The first plan was a custom length-prefixed TCP protocol to
avoid the dependency. Revised because the interfaces are exactly what gRPC
is for: typed request/response between services, deadlines on every call
(a heartbeat that cannot reach the master must fail fast, not hang),
schema evolution through Protobuf field numbers when master and agents run
different versions during a rollout, and generated clients so the CLI and
tests share one definition. It is also the stack Borg's own components
use. Cost: a heavy build dependency (gRPC, Protobuf, Abseil); on this Mac it
comes prebuilt from anaconda, found with `find_package(gRPC CONFIG)`.

**Docker as the task runtime.** macOS has no cgroups, so a plain
fork/exec runtime can only approximate limits with setrlimit. Running each
task as `docker run --cpus=X --memory=Y` puts real cgroup enforcement under
it (Docker runs a Linux VM on macOS), which is what Borg's Borglet does with
containers. A ProcessRuntime with the same interface stays for unit tests
and CI runners without Docker. Cost: container start latency (measured in
m2b) and a dependency on the Docker daemon being up.

**Stateful jobs as the differentiator.** Rescheduling a stateless task is
"start it again somewhere". Rescheduling a job with state means the new
copy must pick up from the old one's checkpoint and must never run at the
same time as the old one (fencing). That is where cluster schedulers get
hard, and adstack's streamkit gives a real job with a real correctness
check (its batch oracle).

## 2026-10-24: m1, core model and scheduler (fleet_core) and fleetsim

What was built: `core/model.h` (Resources, Node, Task, TaskSpec, JobQuota),
`core/cluster_state.{h,cpp}` (the only code that mutates state, plus
`check_invariants()`), `core/scheduler.{h,cpp}` (feasibility, three
placement strategies, preemption search, the scheduling pass) and
`sim/fleetsim.cpp`. Pure, single-threaded, no I/O.

**Integer milli-CPU and MB instead of doubles.** "Does it fit" and "is the
node over capacity" must be exact. With doubles, placing and evicting the
same request many times can leave `allocated` at 1e-13 over capacity and the
invariant checker would need a tolerance. Scores are fractions compared by
cross-multiplication (a/b < c/d as a*d < c*b), not divided into doubles, so
two nodes with truly equal scores compare equal and the tie-break decides,
not rounding. Cost: a ceiling of 2^31 per dimension per node
(`kMaxCapacity`) so the products fit in int64; `add_node` rejects larger.

**Dense integer ids into vectors instead of hash maps keyed by string.**
Node and task lookup is an array index, O(1) with no hashing, and iterating
in id order is free, which is what makes the tie-break (lowest node id)
deterministic without sorting. Cost: tasks are never erased in m1 (terminal
tasks stay as history so ids stay valid). Compaction comes when the master
runs for days.

**All mutation through ClusterState methods, each returning false on an
illegal transition.** `place`, `evict`, `finish`, `kill`, `set_node_state`
update every derived structure (node.allocated, the node's task set, the
pending queue, job quota usage) in one place. `check_invariants()`
recomputes all of it from the task list in O(nodes + tasks) and compares:
no node over capacity, every running task on a ready node and in that
node's set, every pending task in the queue, allocated equal to the sum of
placed requests, quota usage equal to the sum of live requests. Tests call
it after every operation. Alternative: let the scheduler write fields
directly and trust it. Rejected because one forgotten line drifts silently;
here a buggy caller fails a test.

**Pending queue as a std::set ordered by (-priority, id).** Highest band
first, then oldest. Insert, erase and in-order visit are O(log n). The pass
walks it by key (`upper_bound(previous key)`) instead of by iterator,
because placing erases and preemption inserts. Why one pass terminates and
visits each task at most once: a victim has strictly lower priority than its
preemptor, so its key sorts after the cursor; it is retried later in the
same pass, and everything after it has priority <= its own, so nothing can
evict it again in that pass.

**Best-fit on the dominant resource as the default.** The dominant resource
is the dimension where the task asks for the larger share of that node
(a 4c/4G task is CPU-dominant on a 16c/128G node). Best-fit picks the node
leaving the least of it free, to keep large holes open for large tasks.
Worst-fit (most left, spreading) and first-fit (lowest feasible id) sit
behind `Strategy` for comparison. Alternatives: a weighted sum of both
dimensions, or a dot-product "alignment" score (Tetris-style). The single
dominant dimension was chosen because it is one sentence to explain and
matches DRF's notion of dominant share. What it costs is visible in the
measurements below: it ignores the non-dominant dimension.

**Admission control charges quota at submit, not at placement.** A job
over quota is told immediately with a reason ("job web over quota: used X +
request Y > limit Z") instead of sitting in the pending queue forever.
Eviction and node loss keep the quota charged (the task is still alive);
only finish and kill release it. Lowering a quota below usage does not kill
anything, it only blocks new submissions (Borg behaves the same way).
Not done in m1: rejecting a request larger than every node, since nodes can
be added later; such a task simply stays pending.

**Preemption: exact search with a budget instead of pure greedy.** If no
node is feasible, every ready node with matching labels and enough total
capacity is searched for the cheapest set of strictly lower-priority victims
whose eviction makes the task fit. Cost is lexicographic: fewest victims,
then lowest top victim priority (two batch victims beat one batch plus one
production), then lowest priority sum, then lowest node id. Strictly lower
only, because equal priority would let two tasks evict each other forever.
Minimum-count covering in two dimensions is NP-hard in general, so the
search is a depth-first branch and bound over candidates sorted cheapest
first; its first complete descent is exactly the greedy answer ("evict
cheapest first until it fits"), so a plan always exists once any is
feasible, and a step budget (200,000 per decision) bounds only the hunt for
a better one. Alternative: greedy alone. Rejected because greedy can evict
three small tasks when one large one would do (test
`Preemption.PrefersFewestVictims`). Verified optimal against brute force
over all subsets on 400 random small clusters
(`Preemption.MatchesBruteForceOnRandomClusters`). In the m1 sims the budget
was hit 0 times for best-fit and worst-fit and 11 times for first-fit (seed
42), where first-fit leaves many small tasks per node.

**Drain and death evict immediately.** In m1 there are no processes, so
setting a node draining or dead returns its tasks to pending at once,
which keeps "every running task is on a ready node" exact. Node loss does
not bump `preempted_count`, since it is not priority churn. Graceful drain
arrives with the reconciler in m3.

**fleetsim workload.** splitmix64 instead of `<random>` distributions,
because those are implementation-defined and the stream must be identical
on macOS and Linux CI. 100 nodes in three shapes (32c/128G, 16c/64G,
16c/256G; 25% with disk=ssd), tasks in six size classes with +/-25% jitter,
priorities 40/35/20/5% across bands 0..3, 5% requiring ssd, lifetimes
10..50 steps, 50 tasks arriving per step. Arrival over time is what makes
preemption happen (a production task arriving at step 80 finds the cluster
full of batch work). Fragmentation follows Weng et al., "Beware of
Fragmentation" (USENIX ATC '23): the share of free CPU sitting on nodes
where a task drawn from the workload's own size mix does not fit, weighted
by that mix. Reported both as a share of free CPU and of cluster CPU,
because a strategy that packs tighter has less free CPU and its few crumbs
look bad as a share of free. check_invariants() runs after every step.

### Measurements (Apple M5 Pro, 18 threads, Release build)

Command: `./build/bin/fleetsim --nodes 100 --tasks 5000 --compare --seeds 10`
(mean over seeds 42..51; uptime load average 4.81 4.17 3.54). This
workload is saturated: arrivals exceed capacity, CPU ends near full.

| strategy | running | pending | preemptions | cpu util end / mean | mem util end / mean | fragmented CPU end (% of free / % of cluster) | 8c/32G slots | decision mean / p99 (us) |
|---|---|---|---|---|---|---|---|---|
| best-fit | 1474 | 182 | 150 | 98.0% / 83.7% | 71.8% / 58.4% | 75.4% / 1.5% | 0 | 2.36 / 12.45 |
| worst-fit | 1453 | 206 | 226 | 93.3% / 80.2% | 69.3% / 58.2% | 33.0% / 1.8% | 0 | 2.53 / 27.91 |
| first-fit | 1481 | 158 | 206 | 96.9% / 83.7% | 70.3% / 59.4% | 48.5% / 1.4% | 0 | 3.07 / 19.02 |

Same workload with lifetimes 5..30 (`--life 5 30 --compare --seeds 10`,
same load average), where the cluster is not saturated:

| strategy | running | pending | preemptions | cpu util end / mean | mem util end / mean | fragmented CPU end (% of free / % of cluster) | 8c/32G slots | decision mean / p99 (us) |
|---|---|---|---|---|---|---|---|---|
| best-fit | 884 | 0 | 4 | 68.1% / 59.0% | 43.9% / 39.2% | 17.2% / 5.4% | 64 | 0.51 / 1.30 |
| worst-fit | 883 | 0 | 0 | 67.9% / 58.9% | 43.7% / 39.1% | 7.2% / 2.2% | 32 | 0.45 / 1.19 |
| first-fit | 883 | 0 | 0 | 67.9% / 58.9% | 43.7% / 39.1% | 4.3% / 1.4% | 81 | 0.14 / 0.30 |

Single seed 42 (`--compare`, load 4.95 4.18 3.53): best-fit 232 pending,
151 preemptions; worst-fit 260 / 345; first-fit 211 / 250; zero invariant
violations across all 100 steps of every run. With preemption off
(`--no-preempt --compare --seeds 10`): pending best-fit 134, worst-fit 174,
first-fit 120. With per-job quotas (`--job-quota-mcpu 20000`, best-fit,
seed 42): 1,977 of 5,000 submissions rejected at admission, 0 pending.

What the numbers say, honestly:
- Under saturation, best-fit has the highest end CPU utilization and the
  fewest preemptions (150 vs 206 and 226): tight packing leaves more
  whole-node room for the high-priority arrivals, so they need to evict
  less. Worst-fit is worst on every placement metric, as expected of
  spreading, and its spread costs 4.7 points of CPU utilization.
- First-fit, not best-fit, leaves the fewest tasks pending (158 vs 182)
  and, at moderate load, the least fragmentation (1.4% vs 5.4% of cluster
  CPU) and the most 8c/32G slots (81 vs 64). First-fit packs low-id nodes
  on both dimensions and leaves high-id nodes empty, which is exactly a
  large hole. Best-fit scores only the dominant dimension, so it can fill
  a node's CPU while stranding its memory (or the reverse). Classical bin
  packing also finds first-fit close to best-fit. The plan keeps best-fit
  as the default for its preemption count and utilization; a two-dimension
  score (or first-fit as the default) is a fair alternative to revisit in
  m6 at 1,000 and 10,000 nodes.
- At moderate load best-fit preempted 4 times where the others needed 0,
  for the same reason: a fragmented node forced a production task to evict.
- Decision latency is microseconds (mean 2.4 to 3.1 us saturated); each
  decision is an O(nodes) scan. Preemption searches dominate the p99 and
  max (max 893.8 us for first-fit at seed 42). Under ASan
  (`./build-asan/bin/fleetsim --compare --seeds 2`) all invariants held.

Tests (32 in fleet_tests): placement per strategy, dominant-resource
scoring, tie-breaking and same-seed-same-placements determinism, quota
rejection with reason and release only on finish/kill, preemption picks the
fewest then cheapest victims, never touches equal or higher priority,
cascades within one pass, matches brute force, and a truncated search still
returns a valid plan. Property test: 3 seeds x 5,000 random operations
(submit, schedule with a random strategy, finish, kill, evict, possibly
illegal direct place, node drain/death/recovery, new nodes, quota changes)
with check_invariants() after every one; seed 1 alone did 750 scheduling
passes, 1,773 placements, 387 preemptions and 694 quota rejections.
`ctest` passes in build (0.13 s), build-asan (0.46 s) and build-tsan
(2.53 s; m1 has no threads, run for completeness). Zero warnings in
project code; the one warning in the sanitizer builds is inside
GoogleTest's own gtest-all.cc (`-Wcharacter-conversion`), not compiled with
fleet_flags.

Build note: the root CMakeLists now sets CMAKE_PREFIX_PATH and
CMAKE_BUILD_RPATH to ~/anaconda3 when it contains gRPC and the caller did
not set them, so m2's find_package(gRPC CONFIG) works with a plain
`cmake -S . -B build`.

## 2026-10-24: m2, gRPC control plane, fleetmaster, fleetlet, fleetctl, ProcessRuntime

What was built:
- `proto/fleet.proto` (library `fleet_proto`, stubs generated at build
  time): one service, `Master`, with RegisterNode, Heartbeat, Submit,
  List, Kill, Drain.
- `master/`: `MasterCore` (all master logic, no threads, no network,
  unit-tested call by call), `MasterServer` (gRPC sync server, one mutex,
  scheduler thread), `fleetmaster` (flags and signals only).
- `agent/`: the `Runtime` interface, `ProcessRuntime`, `Agent` (register,
  heartbeat, apply the reply), `fleetlet`.
- `ctl/fleetctl`: submit, list, kill, drain.
- `common/`: shutdown-signal handling and timestamped log lines.
- `bench/local_cluster.sh`, output in `bench/results/local_cluster.txt`.

**Pull-based heartbeats instead of the master pushing to an Agent
service.** Every 500 ms each fleetlet calls `Master.Heartbeat` with
everything it runs, and the reply says what to start, what to stop and
which exits the master has recorded. There is no `Agent` service; the
master never dials an agent. Alternatives considered:
- *Master pushes* (`Agent.StartTask`, `Agent.StopTask`). The master needs
  a reachable address for every agent (breaks behind NAT or a firewall),
  must remember which pushes were delivered and retry the rest, and an
  agent that restarts loses whatever was in flight. Each of those is a
  bookkeeping problem the pull model does not have.
- *A long-lived bidirectional stream per agent.* Lower latency (the
  master can send a start the moment it places), but after any reconnect
  the two sides still have to resynchronize full state, which is exactly
  what a heartbeat is; the stream adds lifecycle and flow-control
  questions on top.
Borg's Borglet and Kubernetes' kubelet both pull. Cost: a placement waits
for the node's next heartbeat, so placement-to-start latency is up to one
interval. Measured below; one mitigation was added (next decision).

**A heartbeat that reports an exit also runs a scheduling pass before
the reply is built.** First version: exits set a flag and the scheduler
thread placed the next task a moment later, but by then the node had just
heartbeated, so the start waited almost a full interval. With the pass
inline, work placed into room an exit just freed starts in the same round
trip. Measured with `bench/local_cluster.sh`, 3 runs each, Release:

| version | placement-to-start min / median / max (ms) | settled after (ms) | load average |
|---|---|---|---|
| exit sets a flag | 316/455/507, 262/438/504, 328/464/505 | 7035, 6037, 7181 | 9.50, 8.57, 8.53 |
| pass inline on exit | 1/414/468, 2/410/467, 1/411/465 | 6046, 5611, 6029 | 6.45, 6.15, 6.05 |

The minimum drops to 1 to 2 ms (exit-triggered placements), the maximum
stays bounded by the 500 ms interval, and the whole run settles about
1 s sooner. The median stays high for a reason specific to the script:
it submits right after the agents register, so the first placements land
just after a heartbeat and wait nearly a full interval. Cost of the inline
pass: a heartbeat with an exit holds the lock for one pass over the whole
cluster (microseconds at this size; worth re-measuring at 10,000 nodes in
m6).

**Level-triggered replies, full reports instead of deltas.** The reply
is a pure function of current state: start every task placed here that
the agent did not report, stop every reported instance that is not
current, forget every reported exit. Nothing depends on what an earlier
reply said, so a lost request or reply is repaired by the next heartbeat
with no retry logic anywhere. Starts are re-sent until reported, so the
agent ignores a start for an instance it already has. Alternative: deltas
with sequence numbers and acks, smaller messages but a protocol to get
right. Cost: each heartbeat carries O(tasks on the node); at ~100 tasks
per node that is a few KB every 500 ms. This same comparison of desired
and actual state is the reconciler that m3 builds restarts and
rescheduling on.

**Instance identity is (task id, attempt, incarnation), not task id.**
- *attempt* is bumped on every placement. A task preempted and later
  placed on the same node again is a new attempt, so the exit report of
  the old copy cannot finish the new one
  (`MasterCore.StopsKilledStaleAttemptAndUnknownInstances`).
- *incarnation* is a fresh random 64-bit value per registration. Tasks are
  tagged with the incarnation they started under, and anything reported
  under another one is stopped. Random rather than a counter because a
  restarted master starts its counters and task ids from zero again; a
  counter would make "task 0, attempt 1, incarnation 1" from before the
  restart look current.
Re-registration of a known node name issues a new incarnation and
requeues the tasks of the old one at new attempts. A changed capacity or
label set is refused, because ClusterState cannot resize a node.

**Exits are delivered at least once and applied at most once.** The agent
reports an exit on every heartbeat until a reply lists it in `forget`.
The master applies an exit only if the instance is current, then marks
the task terminal, so a repeated report is acknowledged and ignored
(`MasterCore.ExitIsAppliedOnceAndAcknowledged`). Alternative: report each
exit once; one lost heartbeat would then leave a finished task "running"
forever.

**One mutex around MasterCore, not a single scheduler thread fed by a
queue.** Both serialize access to ClusterState. With the queue, each RPC
handler posts a request object and waits on a promise for its reply, and
the one thread owns the state with no locks. With the mutex, each handler
is a straight-line function: lock, call MasterCore, unlock, return. Every
handler is a few microseconds of in-memory work and a scheduling pass is
microseconds at this size (m1: 2.4 us mean per decision on 100 nodes), so
contention does not matter yet, and the mutex version is less code to
read and defend. The queue design wins once a pass is long enough to
stall heartbeats (m6 measures this at 10,000 nodes); the logic in
MasterCore would not change, only who calls it. The scheduler thread
waits on a condition variable with a 200 ms timeout: woken at once by any
handler that sets `needs_schedule()` (submit, kill, drain, new node), and
the timer is the backstop that m3 will also use for failure detection.

**Agent: one loop thread, deadlines on every call.** Register, heartbeat,
reaping exits and applying replies all run on one thread, so the instance
table and the Runtime need no locks; only the stop flag and a stats
snapshot are shared, under a mutex. Every RPC has a 400 ms deadline,
shorter than the 500 ms interval, so a hung call cannot silently swallow
beats, and tasks keep running while the master is unreachable. gRPC's
reconnect backoff defaults to growing up to 120 s, which would leave an
agent silent for minutes after a master restart; it is capped at 1 s.
fleetctl uses a 3 s deadline (`--timeout-ms`), so a dead master fails the
command in bounded time (`ClusterTest.DeadlinesBoundCallsToADeadMaster`).

**ProcessRuntime: fork/exec, made safe in a multithreaded process.**
fleetlet has gRPC threads, and after fork() the child may only call
async-signal-safe functions until exec. So:
- Everything the child needs (char* argv, the resolved path, open fds,
  rlimit values, the warning text) is prepared in the parent; the child
  only makes system calls. PATH is searched in the parent so the child
  can call execve, which is async-signal-safe, instead of execvp, which
  is not guaranteed to be.
- Exec failure comes back through a pipe marked close-on-exec: the child
  writes errno into it only if execve fails, so start() returns "exec:
  Permission denied" instead of the task "exiting 127" a heartbeat later.
- Each task gets its own process group (setpgid in both parent and
  child, so the group exists before start() returns whichever runs
  first). Stop signals the group, which takes a shell script's children
  too (`ProcessRuntimeTest.StopKillsWholeProcessGroup`).
- The child clears its signal mask and resets dispositions. fleetlet
  blocks SIGTERM in all threads (to sigwait for it), and both the mask and
  ignored dispositions survive exec, so without this a task could never
  be stopped gracefully (`ProcessRuntimeTest.TaskSeesDefaultSignalState`).
- Exits are detected with `waitid(WNOWAIT)`, which leaves the child a
  zombie; the group is then killed and only then reaped. While the leader
  is a zombie its pid, which is the group id, cannot be reused, so the
  group kill cannot hit an unrelated process. Reaping first and killing
  `-pid` after would race with pid reuse.
- `waitpid(pid)` per task, never `waitpid(-1)`, which would steal exits
  from other runtimes in the same process (the integration tests run
  three agents in one process).
- Stop is SIGTERM to the group, then SIGKILL after a grace period (2 s
  default, `--stop-grace-ms`). Logs: one file per attempt,
  `<work-dir>/<job>.t<task>.a<attempt>.log`; job names are validated
  (`[A-Za-z0-9._-]{1,64}`, not `.` or `..`) by the master and again by
  the agent because they become file names.
Alternative: posix_spawn, which avoids the fork hazards and supports
process groups and fd actions directly, but cannot set rlimits in the
child.

**What macOS enforces, measured.** A probe program
(`/private/tmp/fleet/rl.cpp`: setrlimit, then malloc and touch 64 MB
chunks up to 4 GB; load 7.38 5.99 4.40) found:
- `RLIMIT_AS` (on macOS the same constant as `RLIMIT_RSS`) and
  `RLIMIT_DATA`: setrlimit fails with EINVAL at 256 MB, 1 GB, 8 GB and
  64 GB, and succeeds at 500,000 MB, far above any real limit. Every
  macOS process already reserves a very large virtual address space, and
  the kernel refuses a limit below it. So **memory is not enforced per
  task on macOS**. The child still tries (it works on Linux) and, when
  refused, writes one line to the task's log saying so.
- `RLIMIT_CORE = 0`: works; set so a crashing task cannot fill the disk.
- CPU: no rlimit limits CPU *rate*. `RLIMIT_CPU` caps total CPU seconds
  and would kill a long-running service, so it is not used. CPU is
  enforced only by the scheduler's accounting: no node is over-committed
  on paper.
Real enforcement is DockerRuntime's job in m2b (cgroups in Docker's
Linux VM). A memory watchdog polling each task's RSS and killing over
limit was considered and not built: it is a different mechanism from
what m2b provides, and it would only approximate it. On Linux,
`RLIMIT_AS` limits virtual, not resident, memory, so runtimes that
reserve big heaps (JVM, Go) need headroom over their request.

**Known gaps, on purpose, for m3.**
- A preemption victim gets SIGTERM and its preemptor starts in the same
  reply, so for up to the stop grace both run: the ledger is never over
  capacity but the machine briefly is. Alternative: start the preemptor
  only once the victim's exit is reported (slower preemption, exact).
- An agent that crashes leaves its tasks running as orphans; the new
  agent process does not know them, so the incarnation fence cannot reach
  them. A clean shutdown kills them (`Runtime::kill_all`). Fix in m3: the
  agent records its process groups in its work dir and kills them on
  startup.
- No failure detection yet: a dead agent's tasks stay "running" until it
  comes back. That is m3's heartbeat timeout.
- Plaintext gRPC on localhost; TLS is a change to the three places that
  create credentials.

**Testing against prebuilt gRPC under sanitizers.** gRPC and Protobuf come
prebuilt from anaconda, so they are not instrumented.
- ASan reported a container-overflow on the first read of a repeated
  field in a parsed reply. Protobuf's headers (compiled into our code,
  instrumented) annotate RepeatedField's spare capacity as poisoned, but
  the uninstrumented library that fills the field during parsing never
  updates the annotation: the documented mixed-instrumentation false
  positive. Only that check is turned off, through the ctest environment
  (`ASAN_OPTIONS=detect_container_overflow=0`) in the ASan build; every
  other ASan and UBSan check stays on.
- TSan sees every access in our code and the pthread mutexes and
  condition variables we use, so races between our threads (handlers,
  scheduler thread, agent loop, test thread) are checked. It cannot see
  races inside gRPC itself.

**Build.** Stubs are generated by an explicit `add_custom_command`
(protoc with `--cpp_out`, `--grpc_out` and the grpc_cpp_plugin) into the
build tree. Generated code is not compiled with `fleet_flags`, and its
include directory is SYSTEM, so protoc's output does not trip
`-Wconversion` in our code. Apple's linker warns about static libraries
repeated on the link line, which CMake does on purpose for libraries that
depend on each other; that one warning is turned off on Apple. CI now
installs gRPC and Protobuf from apt and Homebrew; Protobuf is found in
CONFIG mode with the FindProtobuf module as fallback for distributions
that ship no CMake config. Not yet run on GitHub's runners.

**A C++20 bug the tests caught.** `for (const auto& t : list().tasks())`
segfaulted: the temporary `ListResponse` is destroyed before the loop body
runs (lifetime extension of range-for temporaries only arrives in C++23).
The tests keep the response in a named variable.

### Verification

Tests: 56 in fleet_tests (24 new). `MasterCore` (11, no network): ids and
incarnations, starts re-sent until reported, exits applied once and
acknowledged, an exit's freed room started in the same reply, stops for
killed / stale-attempt / unknown / stale-incarnation instances, unknown
node and stale incarnation rejected, re-registration requeues at a new
attempt and refuses a resize, submit validation (path-like job names,
priority, empty command, count), drain moves a task with a new attempt,
preemption stops exactly one victim, kill by job. `ProcessRuntime` (7,
real processes): exit codes reported once, stdout/stderr captured and
stdin is /dev/null, missing and non-executable commands fail at start(),
stop kills a grandchild, SIGTERM escalates to SIGKILL after the grace,
kill_all leaves nothing, tasks do not inherit a blocked SIGTERM.
Integration (6, in-process master and agents over loopback gRPC, 100 ms
heartbeats): six tasks run to completion across three nodes with output
in their logs plus a failing and a missing command; kill stops the real
process; drain moves a task to the other node at attempt 2 with one
process left; preemption over the wire, and the victim reruns at attempt
2 after the preemptor finishes; agent restart under a new incarnation;
deadlines against a dead address. Every integration test ends with
`MasterCore::check_invariants()`.

| build | command | result | load average |
|---|---|---|---|
| Release | `ctest --test-dir build` | 56/56, 4.00 s | 5.04 5.73 5.69 |
| ASan + UBSan | `ctest --test-dir build-asan` | 56/56, 9.96 s | 5.06 5.70 5.67 |
| TSan | `ctest --test-dir build-tsan` | 56/56, 51.71 s, no reports | 5.59 5.78 5.70 |
| Release, repeated | `fleet_tests --gtest_filter='ClusterTest.*:ProcessRuntimeTest.*:MasterCore.*' --gtest_repeat=20` | 20/20 | 2.70 4.80 5.33 |

Before the inline-pass change the suite also passed `ctest -j 18
--repeat until-fail:5` (Release) and `-j 18 --repeat until-fail:3` (TSan,
new tests), which runs the integration tests concurrently on one machine.

`bench/local_cluster.sh` (Release, Apple M5 Pro, load 2.70 4.80 5.33;
full output in `bench/results/local_cluster.txt`): master plus three
agents (n1 4 cores / 4 GB, n2 4 cores / 8 GB with `disk=ssd`, n3 2 cores /
2 GB), 13 tasks: six 1-core shell-arithmetic batch tasks, three 1.5-core
priority-2 services that must preempt batch work to fit, one
`disk=ssd` task, one that exits 2, one asking for 16 cores (stays
pending), one `sleep 600` that is killed; n3 is drained mid-run. Result:
settled in 6,028 ms, every outcome as expected (checked by the script),
preemptions and the drain visible in the master's event log, batch tasks
rerun at attempts 2 and 3 after being preempted, placement-to-start 1 /
277 / 331 ms (min / median / max over 14 placements, 3 more undone before
they started), and no task process left after teardown. The same script
with the sanitizer binaries: ASan settled in 6,041 ms (load 2.53 4.66
5.27) and TSan in 26,103 ms (load 2.52 4.59 5.24; fork under TSan costs
hundreds of ms), both with every outcome as expected, no sanitizer report
in any master or agent log, and nothing left behind.

Zero warnings in project code (clean Release rebuild: the only warning is
`-Wcharacter-conversion` inside GoogleTest's own gtest-all.cc).

## 2026-10-24: m2b, DockerRuntime

What was built:
- `agent/docker_runtime.{h,cpp}`: `DockerRuntime`, a second implementation
  of the m2 `Runtime` interface. Each task is one container:
  `docker run -d --name fleet-<task>-<attempt>-<inc8> --init --cpus=<milli/1000>
  --memory=<mb>m --memory-swap=<mb>m <image> <cmd...>`; state, exit code and
  OOM kill from `docker inspect`; output copied to the task's log file
  with `docker logs`; `docker stop -t <grace>` then `docker rm`.
- `agent/subprocess.{h,cpp}`: `Subprocess`, a non-blocking runner for the
  docker CLI (fork/exec of an argv array, non-blocking output pipes, a
  deadline, polled by the caller). `resolve_command` and the child's
  signal reset moved here from ProcessRuntime so both runtimes share them.
- Task spec gains an image: `SubmitRequest.image`, `TaskLaunch.image`,
  `TaskInfo.image` (empty means the node runtime's default, `busybox:1.36`);
  `fleetctl submit --image`. Exits gain an OOM flag: `ExitInfo.oom_killed`,
  `TaskStatus.oom_killed`, `TaskInfo.oom_killed`; `fleetctl list` shows
  `oom` in the EXIT column.
- `fleetlet --runtime=docker|process` (default process), `--docker-image`,
  `--docker`, `--docker-poll-ms`; all flags also accept `--flag=value`.
  With docker, fleetlet runs `docker version` (5 s deadline) at startup
  and exits with the reason if no daemon answers.
- `bench/docker_limits.sh`, output in `bench/results/docker_limits.txt`.

Docker on this machine: OrbStack, Docker Engine 29.8.2, Linux kernel
7.2.12 in OrbStack's VM, cgroup v2, 18 CPUs and 12.6 GB visible to
containers. Docker *is* installed here, so the real-Docker tests and the
bench ran; nothing was skipped.

**The docker CLI, not the Engine API.** The Engine API is HTTP/1.1 over
the unix socket (`/var/run/docker.sock`, or OrbStack's own socket through
the active docker context). Using it directly saves a fork/exec of the Go
CLI per call (`docker inspect` measured at a 30 to 34 ms median, below)
and would allow the `/events` stream instead of polling. It needs an
HTTP client and a JSON parser, and fleet's rule is no dependencies beyond
gRPC and Protobuf; writing both by hand is more code to defend than the
whole runtime. The CLI already handles contexts, TLS and API version
negotiation, and a Go template makes it print exactly the four fields we
need. Cost: one process per call and parsing human-facing text (kept
small by the template). The socket API, with `/events`, is the next step
if the per-call cost matters, e.g. at hundreds of tasks per node.

**Every CLI call asynchronous, not a blocking `docker run` inside
start().** The m2 contract says start() does not block, because the agent
calls the runtime on the same thread that heartbeats. `docker run -d`
measured 105 to 1,617 ms (median 118 to 129 ms over 30 runs), and an
image pull makes it seconds; a node starting ten tasks in one reply would
miss several 500 ms heartbeats, which after m3 means being declared dead.
So start() only spawns `docker run` and returns a handle; reap() polls
the Subprocess. Alternatives: a worker thread per container or a pool
that runs blocking CLI calls (threads plus a queue plus locks, for what
is a state machine anyway); `posix_spawn` (no deadline or pipe handling
saved, same polling). Cost: Docker's own rejections (unknown image, pull
denied, a command missing from the image, a limit below Docker's minimum)
arrive later, through reap(), as an exit with `start_failed`, which the
agent reports as the same `PHASE_START_FAILED` a synchronous failure
gets. Until then the master sees the task as running for one or two
heartbeats.

**Poll with one batched `docker inspect` every 250 ms, not `docker events`
or `docker wait`.** One call covers every running container on the node
(`docker inspect --format '{{.Name}} {{.State.Status}} {{.State.ExitCode}}
{{.State.OOMKilled}}' n1 n2 ...`), so the cost is one fork per poll
interval per node, whatever the task count. Alternatives:
- `docker wait` per container: event-driven and exact, but a blocked CLI
  process per task (100 tasks = 100 Go processes), and it does not say
  whether the exit was an OOM kill, so an inspect is still needed.
- `docker events`: one long-running stream, lowest latency. But a stream
  can drop (daemon restart, VM sleep) and after any gap we must re-list
  everything anyway, which is the poll. Level-triggered polling is the
  same choice m2 made for heartbeats: a lost answer is repaired by the
  next one.
Cost: exit detection waits up to one poll interval plus an inspect.
Measured (Docker's `die` event to fleetlet's log line, three bench runs):
124, 89 and 40 ms.

**Docker does the field extraction (a Go template), not a JSON parser.**
`{{json .State}}` would need JSON parsing; the template yields four
space-free tokens per line, and container names and status words cannot
contain spaces. inspect prints the containers it found even when it fails
on others (exit 1, `no such object` on stderr), so stdout is parsed
whatever the exit code. A container is declared gone only when stderr
names it with `no such object`; any other failure (daemon restarting,
timeout) means "learned nothing this round", never an exit
(`FakeDockerTest.DaemonOutageIsNotAnExit`).

**OOM from Docker's `State.OOMKilled`, not from exit code 137.** 137 is
128 + SIGKILL: an OOM kill, but also docker stop's escalation, `kill -9`
or `exit 137`. The cgroup's OOM flag is the ground truth
(`FakeDockerTest.Exit137WithoutOomIsNotAnOom`). Exit codes are passed
through as Docker reports them, with `signal` left 0: Docker gives one
integer, and reinterpreting 129..159 as signals would mislabel a program
that exits with such a code. The message says which limit was exceeded
("OOM-killed: exceeded its 32 MB memory limit").

**Flags beyond the plain `docker run --cpus --memory`, each measured or
tested:**
- `--init` (tini as PID 1). Linux does not deliver a default-action
  SIGTERM to PID 1 of a PID namespace, so `sh -c 'sleep 600'` would
  ignore `docker stop` and die only at the SIGKILL after the grace. With
  tini the stop is prompt and exits 143 (`RealDockerTest.StopIsPromptBecauseOfInit`,
  well under its 10 s grace; a manual `docker stop -t 2` of three such
  containers took 185 ms). This matches ProcessRuntime, which stops the
  whole process group with SIGTERM.
- `--memory-swap` equal to `--memory`. Docker's default lets a container
  use as much swap again as its memory limit, so on a host with swap an
  over-limit task slows down instead of being killed, and the limit is
  really twice what the scheduler accounted.
- No `--rm`. The exit code and the OOM flag live on the stopped
  container; `--rm` deletes it, racing our inspect. fleet removes it
  after reading them (`docker rm`, no `-f`: it has already exited).

**Logs copied with `docker logs` before the exit is reported, not
streamed or bind-mounted.** "Exited" then means the log file is complete,
the guarantee ProcessRuntime gives by writing the file directly.
Alternatives: a `docker logs -f` process per container (one more process
per task), or bind-mounting the log directory and redirecting inside the
container (needs a shell in every image and changes the user's command).
Cost: the file appears only at exit (`docker logs <name>` works live), and
stdout and stderr, written by the CLI on two streams into one file, keep
their relative order only approximately.

**Container names `fleet-<task>-<attempt>-<first 8 hex of the
incarnation>`, not `fleet-<task>-<attempt>`.** Docker refuses a second
container with a name in use, and (task, attempt) is unique only within
one master's lifetime: a restarted master numbers tasks from 0 again, and
a container left from before would make every start of the new task 0
fail. The incarnation is random per registration (m2), so adding it keeps
names unique across restarts of either side and readable in `docker ps`
(`DockerArgs.ContainerNamesIncludeTaskAttemptAndIncarnation`).

**Image references validated at the master and again at the agent.**
The image sits right before the command in the argv, so an "image" of
`--privileged` would be parsed by docker as a flag (argv arrays rule out
shell injection, not argument injection). `valid_image_ref` (core/model.h)
requires an alphanumeric first character and the reference alphabet.
The master rejects at submit with INVALID_ARGUMENT, so the user sees it
at once; the agent checks again because it does not trust the network,
as with job names in m2. The default image lives in the runtime
(`--docker-image`), not the master, so a ProcessRuntime node is not told
it runs `busybox`. Pinned `busybox:1.36` rather than `:latest`, so a task
means the same bits next month.

**Stop: `docker stop -t ceil(grace)` once, retried only on evidence.**
The CLI takes whole seconds, so a 1,500 ms grace becomes 2 s (never less
than asked). A stop asked for while `docker run` is still in flight is
sent the moment it returns. If a later inspect still sees the container
running, the stop is sent again, but only when that inspect *started
after* the previous stop finished: an older inspect may have looked
before the stop took effect, and retrying on it would send a second
stop whenever an inspect overlapped the first. Tests: one stop for two `stop()` calls,
deferred stop during start, retried stop when the daemon ignores it.

**kill_all waits (bounded) for in-flight `docker run`s, then one `docker
rm -f` for all.** Killing the CLI between its create and start API calls
leaves a created container; killing it during create can let the daemon
finish after our rm. Waiting up to 5 s closes most of that window; a run
still going after that (a slow pull) is killed, and can leave one
created, never-started container. `rm -f` kills and removes in one call.

**Tests use a fake docker CLI passed as a path, not placed first on
PATH.** The fake is a /bin/sh script that records argv and keeps each
container as a state file; tests flip a file to simulate an exit or an
OOM. Passing its path (`DockerRuntimeOptions::docker`) instead of editing
PATH avoids `setenv` in a process where gRPC threads from other tests may
be calling `getenv`, which is a data race. The real-Docker tests skip
unless `docker version` answers and `busybox:1.36` is already pulled, so
CI (no daemon or no image) runs the fake-backed tests only and never
pulls from the network.

### Measurements (Apple M5 Pro, 18 threads, OrbStack; Release build)

`bench/docker_limits.sh`, three consecutive runs, 1-minute load average
at each start 3.42, 3.35, 3.53 (full output in
`bench/results/docker_limits.txt`):

| check | run 1 | run 2 | run 3 |
|---|---|---|---|
| 256 MB through a `--mem 32` limit | `oom`, exit 137 | `oom`, exit 137 | `oom`, exit 137 |
| control, 16 MB under `--mem 64` | exit 0 | exit 0 | exit 0 |
| placement to container start / kernel `oom` event / `die` (ms) | 166 / 650 / 776 | 201 / 388 / 468 | 197 / 416 / 497 |
| `die` to fleetlet sees it / master records it (ms after placement) | 124 / 1,029 | 89 / 1,035 | 40 / 537 |
| bare container, same command: start to OOM kill (ms) | 269 | 211 | 215 |
| `--cpu 500` spin loop: cgroup `usage_usec` over ~10.3 s | 50.0% of a core | 50.0% | 50.0% |
| same, `docker stats` median of 5 | 49.96% | 50.02% | 50.08% |
| same, CFS periods throttled | 103 of 103 | 103 of 103 | 103 of 103 |
| control `--cpu 2000`, same loop | 100.3%, 0 throttled | 100.0%, 0 | 100.2%, 0 |
| `docker run -d` with DockerRuntime's flags, median of 10 (min/max) | 118 ms (107/144) | 120 ms (112/223) | 129 ms (112/194) |
| `docker inspect`, median of 10 | 31 ms | 31 ms | 31 ms |

Two earlier full runs (16:27 and 16:28, load 3.35 and 4.76) gave the
same CPU numbers (50.3% and 49.9%; controls 100.3% and 100.2%) and start
medians of 122 and 127 ms, but the OOM task took 3.5 and 4.5 s from
submit to visible: fleetlet's log shows 2.66 and 3.97 s from spawning
`docker run` to seeing the OOM, while the control container started
alongside it went from start to reported exit in 0.5 s, so fleet's
pipeline was moving and the container itself ran longer before the
kernel killed it. Those runs predate the docker-events timeline, so the
split is not measured. It did not recur in three later bench runs or in
eight bare `docker run`s of the same command (223 to 269 ms at
`--cpus=0.5`, 154 to 158 ms at `--cpus=2`). The suspect is how long the
kernel spends reclaiming before it OOM-kills under `memory.max`; not
verified.

What the numbers say:
- Memory is enforced by the kernel, not approximated: the task is killed
  at its limit and Docker's OOMKilled flag reaches `fleetctl list` as
  `oom`. Submit to visible was 0.67 to 1.1 s, almost all of it pull-based
  heartbeats (up to 500 ms for the start to reach the node, up to 500 ms
  for the exit to reach the master); the runtime's own share, `die` to
  fleetlet, was 40 to 124 ms, bounded by the 250 ms poll.
- CPU is a CFS quota: `cpu.max` is `50000 100000` (50 ms per 100 ms
  period) for `--cpu 500`, and the loop is throttled in every period and
  held to 50.0%. The same loop with `--cpu 2000` uses a full core and is
  never throttled, so the 50% is the limit, not the loop.
- Start costs about 120 ms per container. A heartbeat interval (500 ms)
  still dominates placement-to-start; the runtime adds a fifth of that.

Not done, on purpose:
- **CPU throttling is measured, not reported.** The plan's m2b line says
  throttling is "detected and reported"; fleet reports OOM kills to the
  master but not throttling. Reporting it means sampling each cgroup's
  `cpu.stat` (`docker stats` or `docker exec`, one more process per task
  per sample) and adding usage fields to TaskStatus; it fits better with
  the m6 measurements than with a heartbeat that today carries only
  lifecycle.
- **Orphan containers after an agent crash.** A SIGKILLed fleetlet leaves
  its containers running, as ProcessRuntime leaves processes (m2 known
  gap). The names make them findable (`docker ps --filter name=^/fleet-`),
  but several agents can share one daemon (the bench and tests do), so
  cleanup on startup needs a per-node label (`--label fleet.node=<name>`)
  first. Planned for m3 with the ProcessRuntime fix.
- **Node capacity is not checked against the Docker VM.** Docker Desktop
  and OrbStack run containers in a VM with its own CPU and memory (here
  18 CPUs, 12.6 GB). fleetlet's `--cpu`/`--mem` must not exceed it, or the
  scheduler will place more than the VM can hold.

### Verification

Tests: 86 in fleet_tests (30 new).
- `Subprocess` (7): stdout, stderr and exit code captured; 1 MB of output
  does not deadlock on a 64 KB pipe; poll() returns at once on a running
  command; a command past its deadline is SIGKILLed and marked timed_out;
  output redirected into a file; a command not on PATH fails at spawn;
  the destructor kills and reaps.
- `DockerArgs` (4): `--cpus` formatting (500 -> 0.5, 1 -> 0.001, 12345 ->
  12.345), the exact run argv, image references that could be flags are
  rejected, container names.
- `FakeDockerTest` (14, fake CLI): availability probe (ok, daemon down,
  no CLI); exact run argv, exit code, logs copied before the exit is
  reported, then `docker rm`; OOM reported with the limit in the message;
  137 without OOMKilled is not an OOM; a failed run is a start failure
  with docker's first stderr line and a forced rm; invalid image and
  missing CLI fail synchronously; stop is one `docker stop -t 2` for a
  1.5 s grace; a stop during start is sent when run returns; an ignored
  stop is retried; one inspect covers three containers; a container
  removed behind fleet's back is reported as vanished; a daemon outage is
  not an exit; kill_all is one `rm -f` for all; and end to end over gRPC
  (master, Agent with DockerRuntime, fake CLI): a `--privileged` image is
  refused at submit, the image and limits reach `docker run`, the
  container name carries task and attempt, and an OOM kill comes back to
  `List` as `oom_killed` with the message.
- `RealDockerTest` (3, real daemon): exit code 3 and the container's
  output in the log file, then the container removed; 256 MB through a
  32 MB limit is OOM-killed (exit 137, OOMKilled); a stopped `sleep 600`
  exits 143 well inside a 10 s grace because of `--init`.
- `MasterCore` (2 new): images validated at submit, stored, sent in the
  start and listed; an OOM exit is recorded and listed, a plain 137 is not.

| build | command | result | load average |
|---|---|---|---|
| Release | `ctest --test-dir build` | 86/86, 10.40 s | 5.19 4.13 3.91 |
| ASan + UBSan | `ctest --test-dir build-asan` | 86/86, 23.54 s | 5.12 4.17 3.93 |
| TSan | `ctest --test-dir build-tsan` | 86/86, 107.51 s, no reports | 4.63 4.14 3.92 |
| Release, repeated | `fleet_tests --gtest_filter='FakeDockerTest.*:Subprocess.*:RealDockerTest.*' --gtest_repeat=10` | 10/10 | 3.07 3.74 3.79 |
| Release, concurrent | `ctest --test-dir build -j 18 --repeat until-fail:3` | 86/86, 6.42 s | 3.23 3.66 3.75 |

The real-Docker tests ran (not skipped) in all three builds (3.3 to 3.9 s
each under TSan). `bench/docker_limits.sh` with Docker off PATH
(`PATH=/usr/bin:/bin`) prints `SKIP: Docker is not available (docker CLI
not on PATH)` and exits 0. `bench/local_cluster.sh` (ProcessRuntime,
load 3.26 3.68 3.75) still passes: every outcome as expected,
placement-to-start 1 / 415 / 469 ms, nothing left behind. After all runs
`docker ps -a --filter name=fleet-` lists no containers. Zero warnings in
project code (clean Release rebuild; the one warning is GoogleTest's own
`-Wcharacter-conversion` in gtest-all.cc).

## 2026-10-24: m3, failure detection, rescheduling, fencing, drain, restart policies

What was built:
- `master/failure_detector.{h,cpp}`: `FailureDetector`, a clock-free
  alive / suspect / dead state machine over heartbeat silence.
- `master/restart_policy.h`: the never / on-failure / always policies and
  the capped exponential backoff schedule, as pure functions.
- `MasterCore` (rewritten around a reconciler): per-registration epochs
  from a monotonic counter, `tick(now)` (failure detection, backoff
  timers, a scheduling pass), fencing of stale and revoked epochs, restart
  policies, graceful drain. `MasterServer`'s scheduler thread became a
  tick thread (100 ms, `--tick-ms`).
- `ClusterState`: `requeue` (running to pending without counting a
  preemption), `enter_backoff` / `leave_backoff` and a new `kBackoff`
  task state, `Node::suspect`, and draining no longer evicts.
- Agent: on `FAILED_PRECONDITION` it SIGKILLs every task and registers
  again; before its first registration it kills tasks a crashed
  predecessor left running (`Runtime::kill_leftovers`, both runtimes);
  `set_partitioned()` fault injection for tests.
- Wire: `incarnation` renamed `epoch` (same field numbers and type, so
  the wire is unchanged), `TASK_STATE_BACKOFF`, `RestartPolicy` on
  `SubmitRequest`, `NodeInfo.suspect` / `epoch`, `TaskInfo.restart_policy`
  / `restarts` / `restart_in_ms`, `DrainResponse.evicted` renamed
  `moving`.
- CLI: `fleetmaster --heartbeat-ms --suspect-after --dead-after --tick-ms
  --backoff-base-ms --backoff-max-ms --backoff-reset-ms` (integer flags
  now range-checked); `fleetctl submit --restart`, `list` shows `suspect`,
  `backoff Ns`, the policy and the restart count.
- `bench/node_failure.sh`, output in `bench/results/node_failure.txt`.

**Counted missed heartbeats with two thresholds, instead of one timeout
or a phi-accrual detector.** `missed = floor(silence / interval)`;
suspect at 3, dead at 6 (flags). Why two states: a node that has missed
1.5 s of heartbeats is probably in trouble, so new work should not be
sent there, but moving its tasks is expensive and, if it is only slow,
creates a second copy of each. Suspect therefore stops placements and
nothing else; dead moves everything. One timeout would force a choice
between reacting early (false deaths) and placing work onto dying
machines. Phi-accrual (Hayashibara et al.; Cassandra, Akka) estimates the
inter-arrival distribution and adapts to jitter; fleet's master sets the
agents' cadence itself, so "6 missed beats" is a number an operator can
reason about and detection time follows from two flags (measured below).
Cost: a fixed threshold cannot adapt, so a stall longer than 3 s (a long
GC pause, a swapped-out agent, a slow link) is a false death. Dead is
absorbing: a heartbeat does not revive a dead node, only a new
registration does, so a flapping link cannot make a node oscillate
between dead and alive while its tasks are being rescheduled. The
detector takes `now` on every call and owns no clock, which is what lets
`FailureDetectorTest` step time 10 ms at a time and assert that suspicion
comes at exactly 1,500 ms and death at exactly 3,000 ms.

**Judged at the master, every 100 ms tick.** The tick bounds how late a
death is noticed (at most one tick after the sixth missed beat), so it is
a fifth of the heartbeat interval. Not handled: if the master itself
stalls (the tick thread is starved, or the process is paused), every node
looks silent when it resumes and all are declared dead at once
(`Failover.ATickStallDeclaresDeathOnceNotOnEveryTick` shows the one-shot
declaration). A guard such as "do not declare deaths on a tick that ran
much later than scheduled", or refusing to evict when a large fraction of
nodes fail together (Kubernetes' node lifecycle controller slows
evictions when a zone looks disrupted), is the fix; it was left out to
keep the detector a pure function of heartbeat times.

**Epoch from one master-wide counter that only goes up, seeded from the
wall clock, instead of a random 64-bit incarnation (m2).** Both make every
registration distinguishable, which is all the master's own checks use
(an instance or heartbeat is current only if its epoch equals the node's
current one). Ordering is what a fencing token needs beyond that: anyone
who sees tokens can reject a smaller one than the largest seen, without
asking the master (Kleppmann's fencing-token argument for distributed
locks). fleet does not use that yet; m5's checkpoint storage is where it
will, rejecting a checkpoint write tagged with an older epoch than the
last one accepted, which protects the stateful jobs even when a fenced
node has not yet heard that it is fenced. A random value cannot do that.
One global counter rather than one per node: epochs are then unique across
nodes too (container names use them), and m4 has one number to persist.
The seed is microseconds since 1970, so a master restarted after any real
downtime starts above everything its predecessor issued; that matters
because a restarted master numbers tasks from 0 again, and an old process
"task 0, attempt 1, epoch E" must never match a new task 0. Cost: it
assumes the wall clock does not step backwards across a master restart
(NTP can); m4 persists the counter in the WAL and drops the assumption.
Zero is never issued and means "revoked". The container name now carries
the whole epoch in hex: with a counter the high digits are identical for
every registration, so m2b's 8-digit prefix would have collided.

**Fencing: refuse the whole heartbeat, and the agent kills everything
before registering again.** A heartbeat carrying an epoch other than the
node's current one, including a revoked one, gets `FAILED_PRECONDITION`
and nothing in it is applied: its exits describe instances the master no
longer tracks (an exit of the old copy must not finish the new one), and
its liveness must not count. The agent then SIGKILLs every task and only
then registers. Why not m2's path (register again, report the old tasks,
get them stopped as stale): by the time a node is fenced its tasks are
already running elsewhere, so every moment they run is a duplicate; a
graceful stop would add the stop grace to that, and if registration
fails (the master is unreachable again) the stale-stop path never runs at
all. `NOT_FOUND` (the master has no record of the node at all, i.e. it
restarted without state) keeps the m2 behavior of registering and
letting the master stop stale instances, because a master that never knew
the tasks cannot have rescheduled them.

What fencing prevents: a node that was declared dead and comes back
continuing to run, and to report on, tasks that now run elsewhere; any
report from an older registration (an earlier agent process, or a
duplicate agent with the same name) changing state; exits of old attempts
finishing new ones. What it cannot prevent: **a partitioned node does not
hear that it was fenced.** Its agent cannot reach the master, so it keeps
running its copies until the partition heals and its first heartbeat is
refused. Measured below: with the partition held 1 s past the restart,
old and new copies ran side by side for 1,093 ms, and that window is
simply the partition's length minus the 2.9 s it took to restart the work
elsewhere. It is unbounded. Fencing by epoch also cannot undo side effects
the old copy already committed. **The real fix is a lease**: each
accepted heartbeat grants the agent a lease of L seconds; an agent that
cannot renew kills its own tasks when its lease runs out; the master
reschedules a dead node's tasks only after L plus a margin for clock
*rate* drift has passed since its last accepted heartbeat (each side
measures with its own monotonic clock, so only rates, not absolute times,
need to agree). With L shorter than the dead timeout, the old copy is
gone before the new one exists. Cost of a lease: a master outage longer
than L stops every task in the cluster, trading availability during
control-plane outages for never running twice; storage-side fencing
tokens (above) are the belt to that brace. Not built in m3; m4b's leader
election has the same shape and is the place to build it once.

**One reconciler, level-triggered, instead of an event handler per kind
of failure.** Desired state is ClusterState. The reconciler is the only
code that turns observations into desired-state changes, in two halves:
`tick(now)` (the cluster half: silence becomes suspect or dead, finished
backoffs become pending, then a scheduling pass) and `heartbeat()` (the
node half: apply the report, then reply with the diff between what is
placed on the node and what it reports: start, stop, forget). Restart,
reschedule-after-death and drain are each only a change to desired state;
the same diff carries them out, and because the reply depends only on
current state, any lost message is repaired by the next heartbeat. The
alternative, edge-triggered handlers ("on node death, send starts to the
new nodes"), must never lose an event and must be replayed exactly after
a crash. Why the per-node diff runs on the heartbeat rather than on the
tick: in the pull model a reply is the only way to reach an agent, and a
heartbeat is when the freshest report exists; diffing on the tick against
a cached report would compare older data and still wait for the same
heartbeat to deliver the result.

**Restart policy is decided at the master and goes through the scheduler,
not restarted in place by the agent.** On an exit of the current attempt:
`never` finishes it (succeeded or failed by exit code); `on-failure`
restarts it unless it exited 0; `always` restarts it on any exit; a start
failure counts as a failure; an unset policy is `never`, so m2 clients
behave as before. A restarted task enters `kBackoff` (no node, not
pending, quota still charged), waits `min(max, base * 2^(n-1))` for the
n-th consecutive restart (1 s, doubling, capped at 60 s), then becomes
pending and is placed like any task, on whichever node fits. The count
resets once an attempt has stayed up `reset_after` (60 s), so a service
that crashes once a day restarts after 1 s, not 60. Why at the master:
the task leaves the node, so a task crashing because of its machine (a
bad disk, a full /tmp) gets a different one, and the backoff timer is
visible in `fleetctl list`. Restarting in place at the agent (kubelet's
approach) is faster (no placement, no heartbeat round trip) and keeps
node-local state; it is the better choice once tasks have local disks.
Why a `kBackoff` state instead of "pending with a not-before time": the
scheduler stays a pure pass over the pending queue with no clock, and the
timers live in one ordered set the tick pops in O(log n). Why doubling by
a loop instead of `base << (n-1)`: the shift overflows for large n; the
loop stops at the cap after at most log2(max/base) steps. Preemption,
drain and node death are not the task's fault: they requeue it at once,
whatever its policy, and never touch its backoff
(`Restart.NodeDeathIsNotTheTasksFaultAndSkipsBackoff`). Cost: the reset
clock starts at placement, so it includes up to one heartbeat before the
process actually started.

**Graceful drain: stop first, move after, instead of make-before-break.**
`fleetctl drain NODE` marks the node draining (no new tasks) and returns.
On each heartbeat from that node, a task it reports running is told to
stop (again every beat until its exit arrives); the exit is recorded as a
move, not as the task's own exit, and the task goes back to pending and
is placed elsewhere at a new attempt. A task placed there that the node
does not report is not running there and moves at once: the agent sends
each heartbeat only after applying the previous reply, and no start is
sent to a draining node, so no start can be in flight. Make-before-break
(start the replacement, then stop the original) gives a stateless service
zero downtime, but it runs two copies at once, which for m5's stateful
jobs means two writers to one checkpoint, and it needs spare capacity.
Cost: each moved task is down for one heartbeat to deliver the stop, the
stop itself, one heartbeat to report the exit, and one more for the
replacement's start: in `bench/local_cluster.sh` the drained `web` task
was reported stopped 739 ms after the drain and its replacement started
504 ms later (1,243 ms down). Edge cases, each tested: a task that exits
on its own before any stop was sent follows its policy (a finished batch
task is not rerun); one that exits in the instant after the stop was sent
is rerun (at least once for that race, never two copies at once); a
draining node that dies hands its tasks over at once; a dead node cannot
be drained; the operator's drain survives the node dying and
re-registering (kept apart from NodeState, which death overwrites).
Draining no longer evicts in ClusterState, so its invariant became "every
running task is on a node that is not dead". Not built: `undrain` (a
drained node returns to service only by re-registering, as in m2).

**Leftovers of a crashed agent are killed before it registers (deferred
from m2 and m2b).** Without this, a SIGKILLed fleetlet's tasks keep
running as orphans, the restarted fleetlet knows nothing of them, and its
registration makes the master requeue them: duplicates forever, which no
epoch can reach. ProcessRuntime records each task's pid and kernel start
time (`sysctl(KERN_PROC_PID)` on macOS, `/proc/<pid>/stat` field 22 on
Linux) in `<work-dir>/pids`, deletes the record when it reaps the task,
and a fresh runtime kills the process group of every recorded pid whose
process still has the recorded start time. The start time is what makes it
safe: pids are reused, and a bare pid could by now name an unrelated
process (`ProcessRuntimeTest.KillLeftoversNeverKillsAReusedPid`).
DockerRuntime labels containers `fleet.owner=<node>` and removes every
container carrying its node's label (a label, not the `fleet-` name
prefix, because nodes share a daemon in tests and benches). Limits: a
task whose group leader exited while the agent was down leaves its other
group members alive (only a live leader can be matched), and a crash
between fork and writing the record leaves that task unrecorded. Linux's
`PR_SET_PDEATHSIG` would let tasks die with the agent, but macOS has
nothing like it, and it would also kill tasks on a mere agent upgrade.

**Suspect is a flag on the node, not a NodeState.** A ready or a draining
node can be suspect, and a suspect node keeps its tasks; it only drops
out of placement and preemption (`Node::accepts_tasks()`).

**Testing with a fake clock and a fake partition.** Every MasterCore and
FailureDetector call takes `now`, so `FailoverTest`, `FencingTest`,
`RestartTest` and `DrainTest` step time by hand (no sleeps) and check
`check_invariants()`, which now also verifies: backoff timers match
exactly the tasks in backoff, detector health agrees with node state and
epoch (dead iff kDead iff epoch 0; suspect iff the flag), live epochs are
distinct and already issued, and "moving" is set only on tasks running on
a draining node. Over the wire, `Agent::set_partitioned()` drops all RPCs
while tasks keep running, which is a partition as the agent sees it. The
integration tests declare a node dead after 30 missed 100 ms beats rather
than 6: under TSan a fork takes hundreds of ms on the agent's heartbeat
thread, and 600 ms would kill healthy nodes in unrelated tests.

What is unchanged and still open: on macOS memory is still not enforced
per task by ProcessRuntime (`setrlimit(RLIMIT_AS)` is refused below the
process's large default reservation, measured in m2; DockerRuntime
enforces it with cgroups), and CPU is enforced only by the scheduler's
accounting. A preemption victim still overlaps its preemptor for up to
the stop grace (m2 known gap): fixing it means the ledger keeps the
victim's resources reserved until its exit is reported, a ClusterState
change of its own.

### Measurements (Apple M5 Pro, 18 threads, Release build)

`bench/node_failure.sh` (10 trials; load average at start 3.26 3.91
3.89; full output in `bench/results/node_failure.txt`). Master: 500 ms
heartbeats, suspect after 3 missed, dead after 6, 100 ms tick. Four
4-core fleetlets with ProcessRuntime, six 1-core `sleep` tasks; each trial
SIGKILLs the fleetlet running task 0 at a random point (0 to 499 ms) in
its heartbeat cycle. That node held 4 tasks in every trial.

| from SIGKILL to | min | median | max |
|---|---|---|---|
| master declares the node dead | 2,606 ms | 2,771 ms | 2,950 ms |
| first replacement process started | 2,625 ms | 2,854 ms | 3,055 ms |
| last of the 4 replacements started | 2,749 ms | 3,107 ms | 3,440 ms |

How the numbers follow from the settings. Death is declared on the first
tick at which 6 x 500 = 3,000 ms have passed since the node's last
heartbeat; the master logged 3,004 to 3,099 ms of silence at each of the
11 deaths (10 trials plus the partition), i.e. 3,000 ms plus up to one
100 ms tick. The kill lands phi after that last heartbeat, phi uniform in
[0, 500), so kill-to-dead = 3,000 - phi + (0 to 100) ms, which predicts
2,500 to 3,100 ms with a median near 2,800: measured 2,606 to 2,950,
median 2,771. Suspicion came at 1,528 to 1,602 ms of silence (3 x 500 plus
about one tick; the tick timer starts after the previous tick's work, so
it can run a few ms past 100). The tasks are requeued and placed in the same tick that
declares the death; each replacement then starts on its new node's next
heartbeat, which is up to 500 ms away: the first replacement started 5 to
129 ms after the death in these trials (the earliest of the target nodes'
heartbeats), the last 7 to 494 ms after it (the latest). So the worst case is about 3,100 + 500 =
3,600 ms; to halve it, halve the interval (twice the heartbeat load) or
the miss count (twice the false-death risk).

After each trial the script restarts the fleetlet under the same name; it
killed exactly the 4 orphans its SIGKILLed predecessor left (checked by
the script every trial) and re-registered with a new epoch. The orphans
had run beside their replacements for 112 to 509 ms, which measures how
fast the script restarted the agent, not fleet: with an agent down for
longer, that window is as long as the agent is down. Machine failures do
not have this window (the tasks die with the machine); agent crashes on a
healthy machine do, until the agent comes back.

Partition trial (same run): SIGSTOP one fleetlet, which freezes the agent
but not its tasks. Its 4 tasks were running elsewhere 2,880 ms later
while all 4 old copies kept running. After a further second the script
sent SIGCONT; the agent's next heartbeat was refused ("node n1 was
declared dead; epoch ... is revoked") and it killed its 4 copies 4 ms
after SIGCONT, then re-registered. Old and new copies ran side by side for
1,093 ms = partition length 3,969 ms - 2,880 ms to restart elsewhere +
4 ms to be told. That is the window only a lease closes.

`bench/local_cluster.sh` (m2's scenario, now with graceful drain; load
3.55 3.91 3.90; `bench/results/local_cluster.txt`): every outcome as
expected, settled in 6,579 ms, placement-to-start 1 / 435 / 504 ms
(min / median / max over 15 placements), the drained `web` task moved
from n3 to n1 as above, nothing left behind.

### Verification

Tests: 122 in fleet_tests (36 new, 1 rewritten).
- `FailureDetector` (9, fake clock): misses are whole intervals of
  silence and never negative; suspect at exactly 1,500 ms and dead at
  exactly 3,000 ms when stepped 10 ms at a time; 200 heartbeats arriving
  480 ms late never raise suspicion; a heartbeat clears suspicion and
  restarts the silence clock; dead is absorbing until a new registration;
  a stalled evaluator goes straight from alive to dead in one change;
  nodes are independent and reported in id order; last-heard never moves
  backwards; thresholds that cannot work are rejected.
- `RestartPolicy` and `Backoff` (4): which exits restart under each
  policy; 1, 2, 4, 8, 16, 32, 60, 60 s; n = 0, a huge n, a cap that is not
  a power-of-two multiple of the base, base = cap, base 0.
- `Failover` (3), `Fencing` (3), `Restart` (6), `Drain` (4): MasterCore
  with a fake clock. A silent node is suspect at 1.5 s and dead at 3 s,
  its epoch revoked and its task requeued without counting a preemption
  or a restart, then started elsewhere at attempt 2; a suspect node gets
  no new tasks until it is heard from; a tick stall declares a death once;
  epochs only go up, and a later master starts above an earlier one; a
  fenced node's heartbeat is refused and nothing in it (running copies, an
  exit) is applied, and after re-registering, reports under the old epoch
  are stopped and their exits ignored; an older epoch is refused while the
  node is alive; on-failure backs off 100, 200, 400, 800, 800 ms (not one
  millisecond early) and stops at exit 0; the backoff resets after an
  attempt that stayed up; never and unset policies finish; an unknown
  policy value is rejected; a start failure is a failure; a kill during
  backoff is final, cancels the timer and releases quota; node death skips
  backoff; the drain cases listed above.
- `MasterCore.DrainStopsTheTaskFirstAndMovesItAfterItsExit` (rewritten):
  no second copy is placed while the first runs; stop re-sent every beat;
  the exit (SIGTERM, 143) is a move, not a failure; attempt 2 elsewhere.
- `ClusterTest` (3 new, 1 strengthened, real processes over gRPC): a
  stopped agent's task restarts on the other node after at least 2.8 s
  (30 missed 100 ms beats) and the node lists as dead with epoch 0; a
  partitioned agent keeps its copy running while the replacement starts,
  then is fenced when the partition heals, kills its copy and comes back
  ready with a larger epoch; on-failure reruns a task that fails twice
  until it succeeds at attempt 3 with 2 restarts; drain samples the
  per-node process counts every 20 ms and never sees both copies at once.
- `ProcessRuntimeTest` (3 new): a second runtime kills the group of a
  task an earlier one left running (it dies of SIGKILL); a record whose
  pid is alive but whose start time differs, and one for a pid that does
  not exist, are left alone and cleaned up; records disappear as tasks
  exit. `FakeDockerTest` (1 new): containers carry the owner label, and
  `kill_leftovers` lists by that label and removes what it finds.
- `Property`: now also requeue, backoff in and out, kill from backoff and
  suspect flips; seed 2 did 105 requeues, 92 backoffs and 102 suspect
  flips with every invariant checked after each of 5,000 operations.

| build | command | result | load average |
|---|---|---|---|
| Release | `ctest --test-dir build` | 122/122, 19.49 s | 2.55 3.28 3.63 |
| ASan + UBSan | `ctest --test-dir build-asan` | 122/122, 36.81 s | 3.52 3.72 3.85 |
| TSan | `ctest --test-dir build-tsan` | 122/122, 149.30 s, no reports | 3.73 3.74 3.85 |
| Release, repeated | `fleet_tests --gtest_filter='ClusterTest.*:Failover.*:Fencing.*:Restart.*:Drain.*:FailureDetector.*:ProcessRuntimeTest.*:FakeDockerTest.Owner*' --gtest_repeat=5` | 5/5, 45 tests each | 3.38 4.06 4.00 |
| Release, concurrent | `ctest --test-dir build -j 18 --repeat until-fail:3` | 122/122, 9.91 s | 2.91 3.76 3.89 |
| ASan bench | `TRIALS=3 BIN=build-asan/bin bench/node_failure.sh` | all trials recovered (dead 2,577 to 2,975 ms), partition fenced, no sanitizer report in any log, nothing left behind | 3.72 3.92 3.94 |

Zero warnings in project code (clean Release rebuild, `cmake --build
build --clean-first`; the one warning is `-Wcharacter-conversion` inside
GoogleTest's own gtest-printers.h, not compiled with fleet_flags; the
ASan and TSan rebuilds printed none).

## 2026-10-24: m4, write-ahead log, snapshots, single-master crash recovery

What was built:
- `proto/wal.proto`: the master's storage format, separate from the RPC
  contract in `fleet.proto`. `WalRecord` (an LSN plus one of 11 ops:
  `NodeRegistered`, `NodeDied`, `NodeDraining`, `TasksSubmitted`,
  `TaskPlaced`, `TaskPreempted`, `TaskRequeued`, `TaskMoving`,
  `TaskExited`, `TaskBackoffOver`, `TaskKilled`) and `Snapshot`.
- `master/master_state.{h,cpp}`: `MasterState`, everything the master must
  not forget (ClusterState plus node names, epochs, drain flags, each
  task's command line, attempt, restart counters, backoff deadline and
  last exit, the epoch counter, the LSN), and the single function that
  changes it, `apply(record)`. Snapshots in and out.
- `master/wal.{h,cpp}`: `Storage`. Length-prefixed, CRC-32C-checksummed
  frames, appends synced with `F_FULLFSYNC`, group commit, snapshot and
  log rotation, recovery (snapshot plus replay, torn tail cut off,
  corruption refused).
- `MasterCore` rewritten so it never changes durable state directly: every
  decision becomes a record, applied through `MasterState::apply` and
  queued (`take_records()`). The scheduler got an `Actuator` interface so
  its placements and evictions go through the same path. A second
  constructor starts a core from a recovered state in a report window.
- `MasterServer`: recovery before the port opens; `commit_locked()` after
  every RPC and every tick, before the lock is released; snapshots when
  due; abort on any disk error.
- `fleetmaster --state-dir --wal-sync full|fsync|none --wal-per-record
  --snapshot-every --recovery-window-ms`.
- Agent: `--event-log FILE`, a timestamped start/exit line per task
  instance, used by the overlap check.
- `master/walbench.cpp` + `bench/wal_commit.sh`; `bench/master_failure.sh`.
  Output in `bench/results/wal_commit.txt` and
  `bench/results/master_failure.txt`.

**Log decisions, not inputs (a state-machine command log).** A record is
something the master already decided: "task 7 placed on node 2 as
attempt 3 at T", "task 4 exited 1 and backs off until T+2000". It is never
an input the master still has to decide about ("node 2 heartbeat at t").
So `apply()` needs no clock, no scheduler, no failure detector and no
randomness, and it never iterates over an unordered container. The same
records applied to the same state always give the same state. Recovery
needs that, and so does m4b: Raft followers apply the leader's records,
and they must end up with the leader's state. Alternatives: (1) log the
inputs (every RPC request and tick time) and replay them through the
whole master. That is deterministic only if the scheduler, the failure
detector and the backoff math are too, and it would log one record per
heartbeat, i.e. 2 per node per second even when nothing changes. Any
change to the scheduler would also change what an old log replays to.
(2) Dump the whole state on every change: simple, but each write is
O(state) instead of O(change). Cost of the choice: everything decided
from time or policy has to travel in the record (`restart_at_ms`,
`consecutive_restarts`, `attempt`, `epoch`, `placed_at_ms`), and MasterCore
had to be rewritten so every mutation goes through `emit()`.

**One apply path, used live and on replay.** `emit()` applies the record
to the live state with the same `MasterState::apply` that recovery runs,
then queues it. There is no second "replay version" of any transition to
drift from the live one. A record the live state refuses throws: it is a
MasterCore bug, and running on a state the log cannot reproduce would be
worse. `apply()` also checks that each record follows the last LSN and
that a placement's attempt is the previous one plus 1. So a log applied
to the wrong snapshot, or with a hole in it, fails loudly instead of
quietly giving a different state. The randomized test below proves the
property.

**The ordering rule: durable before visible.** Every call runs as: lock,
decide and apply in memory, append that call's records and sync, unlock,
reply. The tick thread does the same. So when the lock is released,
everything in memory is on disk, and no reply, start, stop, forget, epoch
or submission ack leaves the process before the change behind it is
durable. A crash at any instant is then safe in both directions. A change
that was not synced was never seen by anyone, so losing it is as if it
never happened, and the master re-decides it. A change anyone has seen
survives. Concretely, what goes wrong without the rule (reply first, log
later):
- a forgotten exit: the agent drops an exit once the master acks it, and
  if the ack goes out before the `TaskExited` record is durable, a crash
  loses the exit for good, so the task "runs" forever;
- a reissued epoch: a node holds epoch E while a restarted master issues
  E again to another registration;
- a double start: a start goes out for attempt 3 on node A, the record is
  lost, and the restarted master places attempt 3 again on node B.
`Recovery.AnExitLostWithAnUncommittedBatchIsReportedAgainAndAppliedOnce`
shows the first case handled correctly. The cost: the master's lock is
held across the sync. At about 3 ms per `F_FULLFSYNC` that caps commits
at about 330 per second, and every RPC (reads included) waits behind a
sync in progress. Group commit (below) makes each sync cover a whole
scheduling pass, so the cap is on commits, not records: one pass placing
1,000 tasks is one sync. The alternative, for m4b: decide under the lock,
release it, sync off the lock, and hold each reply until the durable LSN
reaches the LSN its decisions produced. That is exactly Raft's commit
index, so it lands with m4b rather than being built twice.

**`F_FULLFSYNC` on macOS, `fdatasync` elsewhere.** On macOS `fsync()`
hands the data to the drive but does not make the drive flush its own
write cache, so a power cut can still lose it. Apple's fsync(2) man page
says so and points to `fcntl(F_FULLFSYNC)`. Measured here (table below):
p50 3.005 ms for `F_FULLFSYNC` against 0.021 ms for plain `fsync`, about
140 times slower, and that ratio is the whole reason for group commit.
On Linux `fdatasync` already flushes the device cache and skips metadata
the data does not need (it still writes the file size an append
changes). If a file system refuses `F_FULLFSYNC` (some network or FUSE
mounts), the master falls back to `fsync` rather than refusing to run.
`--wal-sync fsync|none` trade power-loss safety for speed; even `none`
survives a process crash such as SIGKILL, because the page cache
outlives the process. On any sync failure the master aborts and does not
retry: after a failed fsync the kernel may already have dropped the dirty
pages and marked them clean, so a later successful fsync proves nothing
(PostgreSQL's 2018 "fsyncgate"). The next start recovers from whatever
the disk really holds.

**Group commit per call (one scheduler iteration, or one RPC), measured
against a sync per record.** One `append()` per call writes all of that
call's frames with one `write()` and one sync. `walbench` timed one
scheduling pass on 100 nodes placing K tasks (real MasterCore, real
Storage, F_FULLFSYNC):

| K placements | group: syncs, commit ms | per-record: syncs, commit ms | speedup |
|---|---|---|---|
| 1 | 1, 2.89 | 1, 2.95 | 1x |
| 10 | 1, 3.94 | 10, 30.97 | 8x |
| 100 | 1, 2.86 | 100, 306.16 | 107x |
| 1,000 | 1, 3.17 | 1,000, 3,096.56 | 977x |

The group commit time stays flat at about 3 ms: the sync dominates, not
the 26 KB written for 1,000 records. With a sync per record it grows
linearly at about 3.1 ms per record, i.e. about 323 records per second no
matter how big the batch. Per-call batching is safe because a reply
depends only on its own call's records and everything before them. It
also costs no added latency, unlike a timer-based group commit (gather
for N ms, then sync), which only pays off when many independent callers
each commit small batches at once. Under the master's single lock, calls
are serial anyway.

**Frame = `[u32 length][u32 CRC-32C(length ++ payload)][Protobuf
payload]`.** The length lets a reader find record boundaries. The checksum
tells a completely written record from one torn by a crash or rotted on
disk. It covers the length too, so a corrupted length cannot make a wrong
span of bytes look valid. CRC-32C (Castagnoli) is what ext4, iSCSI,
LevelDB and RocksDB use: better error detection than zlib's CRC-32 for
the same cost, and a 256-entry table is small enough to write here
instead of adding a dependency. Why Protobuf payloads rather than a
hand-rolled binary layout: field numbers give forward and backward
compatibility (a master must read logs its previous version wrote), and
the records are already Protobuf-shaped for m4b's Raft RPCs. The cost is
about 21 to 27 bytes per `TaskPlaced` (header included), which does not
matter next to a 3 ms sync.

**The torn-tail rule: a bad frame followed only by zeros or EOF is torn;
anything else is corruption, and the master refuses to start.** A crash
mid-append leaves a prefix of the last batch, plus zero-filled space on
some file systems. That batch was never synced, so nobody acted on it,
and recovery truncates it and carries on. A bad frame with valid-looking
data after it means damage in the middle of the log. Skipping it would
silently drop acknowledged records, so the master stops, names the
offset, and leaves the file untouched for a human. A frame whose checksum
matches but which does not parse is also refused: those are the bytes
that were written, from a newer or broken writer, not a tear. A length
above 1 GiB in a complete header is refused as damage even at the end,
because a torn write leaves a correct prefix and never invents such a
length. The rule gets two cases wrong:
- a power cut can persist the pages of the last unsynced batch out of
  order, leaving a garbage page with good data after it. That is refused
  although it is really a tear: the safe direction, it needs a human, and
  nothing is lost;
- a length in the middle of the log corrupted to point past the end of
  the file looks exactly like a tear, so the records after it would be
  dropped. That is the unsafe direction. A separate checksum over the
  8-byte header would close it, at 4 bytes per record. It was left out
  because bit rot in an fsynced file on APFS is rare and m4b removes the
  single copy: a replica with a damaged log can catch up from the leader.

**Snapshot: primary state only, written to a temp file, synced, renamed,
directory synced, then the log rotated.** A snapshot holds only primary
state. Everything derived (node allocations, the pending queue, per-node
task sets, quota usage, the backoff timer set) is rebuilt on load by
`ClusterState::restore_node/restore_task` and then verified with
`check_invariants()`, so a snapshot that does not rebuild cleanly is
refused. Storing derived state too would mean two copies that can
disagree. The steps: write `snapshot.tmp`, sync it, rename it over
`snapshot`, sync the directory (a rename is a directory change and is not
durable until the directory is synced), then rotate: an empty synced
`wal.next` is renamed over `wal` and the directory is synced again. Every
crash point recovers. Before the first rename there is the old snapshot
and the full log. After it there is the new snapshot and the old log,
whose records at or below the snapshot's LSN replay skips
(`Snapshot.CrashBetweenSnapshotAndRotationSkipsRecordsAlreadyIn`).
Leftover temp files are deleted at start and never read. A snapshot is
taken every `--snapshot-every` records (default 10,000); recovery counts
the replayed tail toward it, so a master that keeps crashing still
rotates. Measured in the master_failure run: 13.1 to 17.0 ms per
snapshot (four full syncs dominate), under the lock. Alternatives: a
fuzzy snapshot built while the lock is released, or Redis-style fork and
copy-on-write. Both avoid the pause, but at this state size the pause is
a few syncs, so neither earns its complexity yet.

What snapshots buy here is honest to state: they bound the log's size
more than its replay time. `walbench` replayed 100,150 records (50,000
tasks) in 20.1 ms (0.20 us per record), and loading a snapshot of the
same state took 17.8 ms. Both are dominated by state size, because fleet
never forgets a task: terminal tasks stay in the state, so the snapshot
grows with history too (2.05 MB against the log's 2.59 MB). Garbage-
collecting terminal tasks after a retention period is what would make
snapshots pay off in time as well; it is left for m6's scale work.

**Wall-clock milliseconds in records, steady clock for heartbeats.** The
master measures silence with the steady clock, which never steps. But a
steady clock's zero is arbitrary per boot (and per machine, for m4b), so
a steady time written before a crash means nothing after it. Durable
times (backoff deadlines, placement times) are therefore wall-clock
milliseconds, converted with one offset measured when the core is built.
Cost: if the wall clock steps across a restart, a backoff ends early or
late by the step. That is a timing effect, never a safety one. Epochs,
which are the safety-critical counter, no longer depend on the clock
since m4: `NodeRegistered` carries the epoch and replay moves the counter
past it, so a restarted master keeps issuing larger ones. The m3 clock
seed (microseconds since 1970) remains only as a floor for a master whose
state directory was lost.

**Report before schedule: the restarted master waits for every live node
to report (or up to `--recovery-window-ms`, default 1,000) before its
first scheduling pass.** After a restart the log knows what the master
decided, but not what happened while it was down: placements whose start
never reached the agent (38 in the first master_failure run, 30 in the
second), tasks that exited during the outage and still hold their
resources, and nodes that died. A pass run on that view would place,
preempt or call things unschedulable on the strength of phantom
allocations. Also, every node that has not reported yet is suspect (it
keeps its tasks but gets no new ones), so an early pass would herd all
the pending work onto whichever node reported first. So the core accepts
heartbeats and applies what they report, but schedules nothing until
every node that is not dead has reported once. It ends the window early
the moment the last one does: in the master_failure runs the window was
a median 215 ms and 313 ms against the 1,000 ms limit. The default is
two heartbeat intervals because an agent whose channel is in gRPC's
reconnect backoff (capped at 1 s, agent.cpp) can need about that long to
get its first beat through. A node that misses the window stays suspect
and rejoins on its first heartbeat
(`Recovery.ALateNodeStaysSuspectAfterTheWindowThenRejoins`). The failure
detector's clocks are reset at the restart, so no node is declared dead
for silence that happened while the master was down; one that never
reports is declared dead on the usual 3 s, counted from the restart. Cost:
up to the window of no scheduling after each restart, and a node that
died during the outage is detected 3 s after the restart rather than at
once. Mesos does the same thing at a larger scale: after a master
failover it waits, up to an agent re-registration timeout, for agents to
re-register and report their tasks, then reconciles.

**Reconcile at each node's first report: unreported tasks go back to
pending at a new attempt; unknown instances are stopped.** A task the log
has running on a node that the node does not report never got its start.
The agent applies each reply before it sends its next heartbeat, so if
the reply carrying the start had arrived, this report would list the
task, running or exited. That inference is what makes the requeue safe
even though the log says the task was placed. Requeue rather than re-send
the start: re-sending would also work, and would keep the task on the
same node. But a requeue gives the scheduler a fresh decision against the
cluster as it now is, and a new attempt number. Then if the old attempt
ever did start, through some path this argument missed, it is a
non-current instance and gets stopped, never mistaken for the new one.
The cost is an attempt-counter bump for those tasks, and occasionally a
move to another node. An instance a node runs that the log does not know
(an attempt the log never made, a task id it never issued) is told to
stop, by the same "not current" rule as any stale instance. Agents do not
re-register after a master restart: their epochs are in the log, so their
next heartbeat is simply accepted, and the services survive untouched (4
of 4 still at attempt 1 after 20 kills).

**How "none ran twice concurrently" is checked.** Each fleetlet logs one
line per instance event (`--event-log`). "start" is stamped just before
the runtime is asked to start the process, and "exit" once the agent has
seen it gone, so each [start, exit] interval contains the process's real
lifetime. That makes the check conservative: if two intervals of one
task do not overlap, the processes did not overlap either. The bench
sorts each task's instances across all agents' logs by start and requires
every start to be at or after the previous instance's exit. Clock: all
agents run on one machine and use the same wall clock, so their stamps
are comparable. Across machines this check would need clock-skew bounds.

**Known limits.**
- One copy on one disk: losing the state directory loses the cluster's
  state. That is m4b's job (Raft: three copies, quorum commit).
- The lock is held across the sync (see the ordering rule).
- Submissions are not idempotent. If the master commits a submission and
  dies before replying, the client sees an error, and a retry admits the
  job twice. The bench counts this case: "batch tasks the master has"
  against "acknowledged"; it was 0 in both runs, but it can happen. The
  fix is a client-chosen request id, logged with `TasksSubmitted` and
  deduplicated on apply.
- Terminal tasks are never removed, so the state and the snapshot grow
  with history.
- The middle-of-log length corruption case above.

### Measurements (Apple M5 Pro, 18 threads, APFS internal SSD, Release build)

`bench/wal_commit.sh` (load average 2.70 3.07 2.76;
`bench/results/wal_commit.txt`), 200 appends of one `TaskPlaced` record
per mode:

| sync mode | p50 | p99 | max |
|---|---|---|---|
| none (page cache only) | 0.002 ms | 0.005 ms | 0.268 ms |
| fsync | 0.021 ms | 0.043 ms | 0.050 ms |
| full (`F_FULLFSYNC`) | 3.005 ms | 4.953 ms | 5.047 ms |

Group commit against per-record sync: the table above. Recovery, 100
nodes:

| records | tasks | log bytes | replay ms | snapshot bytes | snapshot load ms |
|---|---|---|---|---|---|
| 10,105 | 5,000 | 247,008 | 5.1 | 207,710 | 3.2 |
| 100,150 | 50,000 | 2,590,530 | 20.1 | 2,052,711 | 17.8 |

`bench/master_failure.sh` (`bench/results/master_failure.txt`, both
runs). Master with a state directory, `F_FULLFSYNC`, a snapshot every 200
records (so most restarts recover from a snapshot plus a log tail), 500 ms
heartbeats. Four 4-core fleetlets with ProcessRuntime and event logs. 4
long-running services, plus a client submitting a 2-task batch job (each
task sleeps 0.1 to 0.9 s) every 250 ms. 20 times: wait 1.0 to 2.5 s,
SIGKILL the master, start it again at once on the same directory.

| from SIGKILL to | run 1 min / median / max | run 2 min / median / max |
|---|---|---|
| serving RPCs again | 24 / 29 / 32 ms | 22 / 28 / 32 ms |
| scheduling again (every agent reported) | 40 / 241 / 525 ms | 44 / 340 / 488 ms |
| of which: report window | 17 / 215 / 500 ms | 23 / 313 / 464 ms |

| outcome | run 1 | run 2 |
|---|---|---|
| load average at start | 2.10 2.85 2.69 | 2.27 2.74 2.66 |
| snapshots written | 3 | 4 (13.1 to 17.0 ms each) |
| batch tasks acknowledged / in the master | 266 / 266 | 312 / 312 |
| submissions refused while the master was down | 4 | 3 |
| acknowledged tasks lost (not succeeded at the end) | 0 | 0 |
| services still running at attempt 1 | 4 of 4 | 4 of 4 |
| placed before a crash, never started, requeued at first report | 38 | 30 |
| tasks / instances in event logs / tasks started twice | 270 / 270 / 0 | 316 / 316 / 0 |
| overlapping instances of one task | 0 | 0 |
| agents fenced / nodes re-registered | 0 / 0 | 0 / 0 |
| torn tails found | 0 in 20 | 0 in 20 |

How the times follow. Serving again in about 29 ms is process start, plus
loading the snapshot and replaying at most about 200 records (0.4 to
5.2 ms in run 2; each recovery's load time is printed in the master log),
plus binding the port. The
rest is the report window, which ends when the last agent's next
heartbeat arrives. The four fleetlets were started within milliseconds of
each other, so their 500 ms heartbeat phases are aligned: in the log, all
four first reports after a restart arrive within about 8 ms of each
other. The window is therefore the wait for one shared next beat, uniform
on [0, 500) ms, which predicts a median near 250 ms and a max near 500.
Measured: medians of 215 and 313 ms, maxima of 500 and 464 ms. With
unaligned agents the window would be the latest of N phases (median near
500 x 0.84 = 420 ms for four), still bounded by one heartbeat interval
plus reconnect time. No torn tail appeared in 40 SIGKILLs. That is
expected: data a `write()` has handed to the kernel sits in the page
cache, which outlives the process, and each commit here is one small
write of a few hundred bytes that the kernel completes before the
process can die. So a SIGKILL could tear the log only in the middle of a
large multi-page write, and a power cut or kernel panic is what really
tears it. The torn-tail tests cut files byte by byte instead.

ASan bench: `TRIALS=5 BIN=build-asan/bin PORT=17371
bench/master_failure.sh` (load 2.15 2.75 2.67): PASS, 52 of 52
acknowledged tasks succeeded, 56 tasks with 56 instances and 0 overlaps,
kill to scheduling 160 / 261 / 515 ms, no sanitizer report in any
master or agent log, nothing left behind.

### Verification

Tests: 155 in fleet_tests (33 new).
- `Crc32c`, `WalRecord`, `Storage` (record round trip): the standard
  CRC-32C check value (0xE3069283 for "123456789") and chaining; every
  op type round-trips through frames; the byte layout is length, then
  checksum, then payload; records appended and reopened rebuild the same
  state.
- `TornTail`: the log cut at every byte offset inside the last record
  loses exactly that record and nothing else; recovery truncates the
  tail and appends cleanly after it, at cuts in the header, at the
  header/payload boundary and in the payload; a bad checksum on the last
  record and trailing zero fill are both treated as torn.
- `Corruption`: a flipped byte in the middle is refused (with the offset)
  and the file is left as it was; an impossible length is damage even at
  the end; a valid checksum over bytes that do not parse is an error; a
  hole in the LSNs is refused.
- `Snapshot` (snapshot round trip): a state with tasks in every state and
  a dead node round-trips exactly; inconsistent snapshots (a running
  task on a dead node, a live node with a revoked epoch, a duplicate node
  name) are refused; snapshot, rotate, more records, then
  recover from snapshot plus log; a crash between the snapshot rename and
  the rotation skips the records already in the snapshot; a corrupt
  snapshot is refused; snapshots are taken automatically every N records.
- `GroupCommit`: one sync per batch against one per record (sync
  counters); `F_FULLFSYNC` works on this file system.
- `Replay.EqualsLiveStateForRandomizedOperationSequences` (replay equals
  live state): 5 seeds x 3,000 random operations (submissions,
  heartbeats with random exits, kills, rare drains, agents going silent,
  restarting and joining, time passing, tick stalls, and master restarts
  from the log, 19 to 40 per seed), with a follower `MasterState` that
  applies every record the master makes. After every single operation the
  follower's snapshot must equal the live one byte for byte (canonical
  serialization), the master's invariants must hold, and every 97
  operations the follower's own snapshot must rebuild to the same state.
  It produced 3,166 to 7,383 records per seed, covering all 11 record
  types in every seed.
- `Recovery` (fake clock): no pass until every live node has reported,
  then the window ends early and places the waiting task; the tick does
  not spin during the window (the bug bench/master_failure.sh found: the
  wake flag stayed set and the tick thread kept the lock, starving the
  heartbeats it was waiting for); an unreported task is requeued and
  started at attempt 2; unknown instances are stopped and the real one
  kept; a late node stays suspect after the window, then rejoins with its
  task untouched; a node that never reports dies 3 s after the restart,
  not before; backoff deadlines survive a restart to the millisecond;
  epochs continue from the log, with a floor of 1 so the clock cannot
  help; an exit lost with an uncommitted batch is reported again and
  applied exactly once; a drain in progress across a restart still
  treats the exit as a move.
- `MasterRestartTest` (real gRPC and processes, the master stopped and
  started on the same port and directory): running tasks survive with
  one start each and new work is scheduled after the restart; exits that
  happen during the outage are applied once after it; recovery from a
  snapshot plus the log after it; a corrupt log stops the master from
  starting.

| build | command | result | load average |
|---|---|---|---|
| Release | `ctest --test-dir build -j8` | 155/155, 9.95 s | 2.37 2.56 2.60 |
| ASan + UBSan (Debug) | `ctest --test-dir build-asan -j8` | 155/155, 43.40 s | 2.40 2.56 2.60 |
| TSan (RelWithDebInfo) | `ctest --test-dir build-tsan -j8` | 155/155, 36.69 s, no reports | 2.94 2.71 2.65 |
| Release, repeated | `fleet_tests --gtest_filter='MasterRestartTest.*:Recovery.*:ClusterTest.*:Storage.*:Snapshot.*:GroupCommit.*:TornTail.*' --gtest_repeat=5` | 5/5, 34 tests each | 2.20 2.62 2.62 |
| Release bench | `bench/master_failure.sh` (twice) and `bench/wal_commit.sh` | PASS both runs; numbers above | above |
| ASan bench | `TRIALS=5 BIN=build-asan/bin PORT=17371 bench/master_failure.sh` | PASS, no sanitizer report | 2.15 2.75 2.67 |

Zero warnings in project code (clean Release rebuild, `cmake --build
build --target clean` then a full build, 55 steps: the only warning is
GoogleTest's own `-Wcharacter-conversion` in gtest-printers.h, which is not
compiled with fleet_flags; the ASan rebuild of 38 files printed only that
same one).
