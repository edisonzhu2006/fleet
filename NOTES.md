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
