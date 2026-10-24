#!/usr/bin/env bash
# local_cluster.sh: a real fleet cluster on one machine, end to end (m2).
#
# Starts fleetmaster and three fleetlets on localhost ports, submits a mix
# of real tasks with fleetctl (CPU-bound batch work, a higher-priority
# service that has to preempt some of it, a label-constrained task, a task
# that fails, one too big for any node, one that gets killed), drains a
# node mid-run, waits for everything to settle, and prints placement and
# completion. Then it measures placement-to-start latency from the
# timestamped master and agent logs (the cost of pull-based heartbeats).
# A trap tears everything down on any exit, and the script fails if any
# task process outlives the teardown.
#
# Usage: bench/local_cluster.sh            (uses build/bin)
#        BIN=build-asan/bin bench/local_cluster.sh
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIN=${BIN:-$ROOT/build/bin}
PORT=${PORT:-17170}
WORK=/private/tmp/fleet/local_cluster
# Every task command carries this marker as its $0, so leftovers can be
# found with pgrep after teardown.
MARK=fleet_lc_$$
ADDR=127.0.0.1:$PORT
# Harmless for non-ASan builds; for BIN=build-asan/bin it disables only the
# container-overflow check, which misfires on prebuilt (uninstrumented)
# libprotobuf (see the root CMakeLists.txt).
export ASAN_OPTIONS=${ASAN_OPTIONS:-detect_container_overflow=0}

rm -rf "$WORK"
mkdir -p "$WORK"
PIDS=()

ctl() { "$BIN/fleetctl" --master "$ADDR" --timeout-ms 2000 "$@"; }

cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  echo
  echo "== teardown"
  # Agents first (each kills its tasks' process groups), then the master.
  for ((i = ${#PIDS[@]} - 1; i >= 0; i--)); do
    kill -TERM "${PIDS[$i]}" 2>/dev/null || true
    wait "${PIDS[$i]}" 2>/dev/null || true
  done
  sleep 0.2
  local left
  left=$(pgrep -f "$MARK" || true)
  if [[ -n "$left" ]]; then
    echo "LEFTOVER task processes: $left"
    kill -KILL $left 2>/dev/null || true
    rc=1
  else
    echo "no task processes left behind"
  fi
  exit $rc
}
trap cleanup EXIT INT TERM

echo "== machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), load: $(uptime | sed 's/.*load averages*: //')"
"$BIN/fleetmaster" --listen "$ADDR" 2>"$WORK/master.log" &
PIDS+=($!)
for _ in $(seq 50); do ctl list >/dev/null 2>&1 && break; sleep 0.1; done

start_agent() {  # name cpu mem [label]
  local extra=()
  [[ $# -ge 4 ]] && extra=(--label "$4")
  "$BIN/fleetlet" --master "$ADDR" --name "$1" --cpu "$2" --mem "$3" \
    --work-dir "$WORK/$1" --stop-grace-ms 1000 ${extra[@]+"${extra[@]}"} 2>"$WORK/$1.log" &
  PIDS+=($!)
}
start_agent n1 4000 4096
start_agent n2 4000 8192 disk=ssd
start_agent n3 2000 2048
for _ in $(seq 50); do
  [[ $(ctl list | awk '$2=="ready"' | wc -l) -eq 3 ]] && break
  sleep 0.1
done

echo "== submitting (cluster capacity 10000m / 14336MB)"
T0=$(date +%s.%N 2>/dev/null || date +%s)
T0_MS=$(perl -MTime::HiRes=time -e 'printf "%d", time()*1000')
# 6 x 1 core of shell arithmetic: real CPU-bound batch work.
ctl submit --job crunch --priority 0 --cpu 1000 --mem 256 --count 6 -- \
  /bin/sh -c 'n=0; while [ $n -lt 400000 ]; do n=$((n+1)); done; echo crunched $n' "$MARK"
# A production service needing 3 x 1.5 cores: only 4 cores are left, so
# the scheduler must preempt batch work to fit it.
ctl submit --job web --priority 2 --cpu 1500 --mem 512 --count 3 -- \
  /bin/sh -c 'echo serving; sleep 4' "$MARK"
ctl submit --job ssd --priority 1 --cpu 500 --mem 256 --label disk=ssd -- \
  /bin/sh -c 'echo on an ssd node; sleep 1' "$MARK"
ctl submit --job flaky --priority 1 --cpu 500 --mem 128 -- \
  /bin/sh -c 'echo about to fail >&2; exit 2' "$MARK"
ctl submit --job huge --priority 1 --cpu 16000 --mem 1024 -- \
  /bin/sh -c 'echo never placed' "$MARK"
ctl submit --job long --priority 0 --cpu 500 --mem 128 -- \
  /bin/sh -c 'sleep 600' "$MARK"

sleep 1
echo
echo "== after 1 s"
ctl list

echo
echo "== kill job long; drain n3"
ctl kill --job long
ctl drain n3

# Settled = nothing running and the only pending task is the one that
# cannot fit anywhere. Progress every 2 s so a slow run is visible.
DONE_MS=""
for i in $(seq 300); do
  out=$(ctl list)
  running=$(awk '$4=="running"' <<<"$out" | wc -l | tr -d ' ')
  pending=$(awk '$4=="pending"' <<<"$out" | wc -l | tr -d ' ')
  if [[ $running -eq 0 && $pending -eq 1 ]]; then
    DONE_MS=$(perl -MTime::HiRes=time -e 'printf "%d", time()*1000')
    break
  fi
  if ((i % 20 == 0)); then echo "  ... ${running} running, ${pending} pending"; fi
  sleep 0.1
done
[[ -n "$DONE_MS" ]] || { echo "did not settle in 30 s"; ctl list; exit 1; }

echo
echo "== settled after $((DONE_MS - T0_MS)) ms"
FINAL=$(ctl list)
echo "$FINAL"

# Expected outcomes; any mismatch fails the script.
check() {  # job state count
  local n
  n=$(awk -v j="$1" -v s="$2" '$2==j && $4==s' <<<"$FINAL" | wc -l | tr -d ' ')
  if [[ $n -ne $3 ]]; then echo "FAIL: job $1 has $n $2 tasks, expected $3"; exit 1; fi
}
check crunch succeeded 6
check web succeeded 3
check ssd succeeded 1
check flaky failed 1
check huge pending 1
check long killed 1
ssd_node=$(awk '$2=="ssd" {print $5}' <<<"$FINAL")
[[ $ssd_node == n2 ]] || { echo "FAIL: ssd task ran on $ssd_node"; exit 1; }
echo "outcomes as expected: crunch 6/6 ok, web 3/3 ok, ssd on n2, flaky exit 2, huge pending, long killed"

echo
echo "== events (master)"
grep -E "preempted|draining|moving|placed|killed" "$WORK/master.log" | sed 's/^/  /'

echo
echo "== placement-to-start latency (master 'placed' -> agent 'started', same task and attempt)"
to_ms() { awk -F'[:.]' '{print (($1*60+$2)*60+$3)*1000+$4}' <<<"$1"; }
lat=()
skipped=0
while read -r ts _ _ task _ _ _ node _ attempt; do
  attempt=${attempt%,}; node=${node%,}
  # A placement can be preempted or drained before its node's next
  # heartbeat, so it never starts; those are skipped, not counted.
  started=$(grep -h "started task $task (.*) attempt $attempt," "$WORK/$node.log" | head -1 | cut -d' ' -f1 || true)
  [[ -n "$started" ]] || { skipped=$((skipped + 1)); continue; }
  lat+=($(( $(to_ms "$started") - $(to_ms "$ts") )))
done < <(grep " placed on " "$WORK/master.log")
n=${#lat[@]}
[[ $n -gt 0 ]] || { echo "FAIL: no placement-to-start pairs found"; exit 1; }
sorted=$(printf '%s\n' "${lat[@]}" | sort -n)
echo "  n=$n (plus $skipped placements undone before they started)  min=$(head -1 <<<"$sorted") ms  median=$(sed -n "$(( (n + 1) / 2 ))p" <<<"$sorted") ms  max=$(tail -1 <<<"$sorted") ms  (heartbeat interval 500 ms)"
