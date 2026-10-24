# Design notes

Running log of decisions. Every entry answers a "why X instead of Y"
question, names the alternative, the cost, and how it was verified.

## 2026-10-24: scope

**Own write-ahead log instead of etcd.** One master, so there is nothing to
reach consensus about; durability needs only an append-only log, fsync and
an atomic snapshot rename. Building it is the part worth understanding.
What etcd (Raft underneath) would add is master replication, so the
cluster survives losing the master's disk, not just its process. That is
the first item on the "next" list.

**Own TCP protocol instead of gRPC.** Fewer than ten message types between
programs we control, all on one machine or LAN. A length-prefixed binary
frame over POSIX sockets is a few hundred lines and has no build-time
dependency. gRPC would buy schema evolution, streaming, deadlines and
cross-language clients, which matter when other teams talk to the service.

**Stateful jobs as the differentiator.** Rescheduling a stateless task is
"start it again somewhere". Rescheduling a job with state means the new
copy must pick up from the old one's checkpoint and must never run at the
same time as the old one (fencing). That is where cluster schedulers get
hard, and adstack's streamkit gives a real job with a real correctness
check (its batch oracle).
