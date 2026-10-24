#!/usr/bin/env bash
# wal_commit.sh: what durability costs (m4). Runs walbench (master/walbench.cpp),
# which uses the real MasterCore and Storage in-process:
#   1. latency of one append + sync per sync mode (page cache only, fsync,
#      F_FULLFSYNC);
#   2. one scheduling pass placing K = 1, 10, 100, 1000 tasks on 100 nodes,
#      committed as one group (one F_FULLFSYNC) or one sync per record;
#   3. recovery time: replaying a log of ~10k and ~100k records, versus
#      loading a snapshot of the same state.
# Prints the machine and load average first. Takes about 15 s; the
# slowest single step (1000 per-record F_FULLFSYNCs) is about 3 s.
#
# Usage: bench/wal_commit.sh          (uses build/bin)
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIN=${BIN:-$ROOT/build/bin}
echo "== machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), load: $(uptime | sed 's/.*load averages*: //')"
dev=$(df /private/tmp | tail -1 | awk '{print $1}')
echo "== /private/tmp is on: $(mount | grep "^$dev " | sed 's/^[^(]*//' || echo unknown)"
"$BIN/walbench" /private/tmp/fleet/walbench
