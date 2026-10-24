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
