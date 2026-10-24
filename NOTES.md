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
