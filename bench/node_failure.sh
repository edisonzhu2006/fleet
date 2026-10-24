#!/usr/bin/env bash
# node_failure.sh: how fast does fleet notice a dead node and run its work
# elsewhere (m3)?
#
# Starts fleetmaster (500 ms heartbeats, suspect after 3 missed, dead after
# 6, 100 ms tick) and four fleetlets with ProcessRuntime (so it runs
# anywhere, no Docker), submits six 1-core `sleep` tasks, then TRIALS times
# (default 10):
#   1. picks the node running task 0 and waits a random 0..499 ms, so the
#      kill lands at a random point in that node's heartbeat cycle;
#   2. SIGKILLs its fleetlet (only the fleetlet: its tasks are separate
#      processes and keep running, orphaned, as they would if the agent
#      crashed on a healthy machine);
#   3. measures, from the timestamped logs (one clock: same machine):
#        kill -> "declared dead"            (master log)
#        kill -> first and last replacement "started task" (agent logs)
#   4. restarts the fleetlet under the same name. Before registering it
#      kills the orphans its predecessor left (ProcessRuntime's pid records,
#      m3), then re-registers with a new epoch; the script checks that it
#      killed exactly the tasks that were moved, and how long the orphans
#      ran beside their replacements.
# Then one partition trial: SIGSTOP a fleetlet (it and its tasks stay
# alive but it stops heartbeating), wait until its tasks run elsewhere,
# hold the partition one more second, SIGCONT it, and measure how long the
# old copies ran beside their replacements before the agent was fenced.
#
# Prints per-trial times and median / max. Expected from the settings, if
# the kill lands phi after the node's last heartbeat (phi uniform in
# [0, 500) ms): dead at 3000 - phi plus up to one 100 ms tick, so 2.5 to
# 3.1 s; placement happens in that same tick; the replacement starts at
# the target node's next heartbeat, up to 500 ms later.
#
# Usage: bench/node_failure.sh             (uses build/bin)
#        TRIALS=3 BIN=build-asan/bin bench/node_failure.sh
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIN=${BIN:-$ROOT/build/bin}
PORT=${PORT:-17270}
TRIALS=${TRIALS:-10}
WORK=/private/tmp/fleet/node_failure
MARK=fleet_nf_$$
ADDR=127.0.0.1:$PORT
NODES=(n1 n2 n3 n4)
export ASAN_OPTIONS=${ASAN_OPTIONS:-detect_container_overflow=0}

rm -rf "$WORK"
mkdir -p "$WORK"
MASTER_PID=""
# Agent pids by node number (n3 -> AGENT_PID[3]): macOS's bash 3.2 has no
# associative arrays.
AGENT_PID=()
pid_of() { echo "${AGENT_PID[${1#n}]:-}"; }
# The tasks running on a node, one "task-id next-attempt" per line, into
# the array MOVING (bash 3.2 has no mapfile).
tasks_on() {  # list-output node
  MOVING=()
  while read -r line; do
    [[ -n "$line" ]] && MOVING+=("$line")
  done < <(awk -v n="$2" '$2=="svc" && $4=="running" && $5==n {print $1" "$6+1}' <<<"$1")
}

ctl() { "$BIN/fleetctl" --master "$ADDR" --timeout-ms 2000 "$@"; }
# Local time of day in ms, the same clock and format the logs use.
now_ms() {
  perl -MTime::HiRes=time -e '$t=time; @l=localtime($t); printf "%d", (($l[2]*60+$l[1])*60+$l[0])*1000 + int(($t-int($t))*1000)'
}
# "HH:MM:SS.mmm" -> ms of the day. (A run that crosses midnight would
# break this; the script fails loudly rather than report negative times.)
to_ms() { awk -F'[:.]' '{print (($1*60+$2)*60+$3)*1000+$4}' <<<"$1"; }

cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  echo
  echo "== teardown"
  for n in "${NODES[@]}"; do
    local p
    p=$(pid_of "$n")
    [[ -n "$p" ]] || continue
    kill -CONT "$p" 2>/dev/null || true
    kill -TERM "$p" 2>/dev/null || true
    wait "$p" 2>/dev/null || true
  done
  if [[ -n "$MASTER_PID" ]]; then
    kill -TERM "$MASTER_PID" 2>/dev/null || true
    wait "$MASTER_PID" 2>/dev/null || true
  fi
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

start_agent() {  # name; logs are appended so a restarted agent keeps history
  "$BIN/fleetlet" --master "$ADDR" --name "$1" --cpu 4000 --mem 4096 \
    --work-dir "$WORK/$1" --stop-grace-ms 1000 2>>"$WORK/$1.log" &
  AGENT_PID[${1#n}]=$!
}
node_state() { ctl list | awk -v n="$1" '$1==n {print $2}'; }
wait_ready() {  # name
  for _ in $(seq 100); do
    [[ $(node_state "$1") == ready ]] && return 0
    sleep 0.05
  done
  echo "FAIL: $1 not ready"; exit 1
}

echo "== machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), load: $(uptime | sed 's/.*load averages*: //')"
"$BIN/fleetmaster" --listen "$ADDR" --heartbeat-ms 500 --suspect-after 3 --dead-after 6 \
  --tick-ms 100 2>"$WORK/master.log" &
MASTER_PID=$!
for _ in $(seq 50); do ctl list >/dev/null 2>&1 && break; sleep 0.1; done
for n in "${NODES[@]}"; do start_agent "$n"; done
for n in "${NODES[@]}"; do wait_ready "$n"; done
grep "listening" "$WORK/master.log" | sed 's/^/  /'

# Six 1-core services on four 4-core nodes. Best-fit packs them, so the
# node holding task 0 usually has several and each trial moves more than
# one task.
ctl submit --job svc --priority 1 --cpu 1000 --mem 128 --count 6 -- \
  /bin/sh -c 'exec sleep 600' "$MARK" >/dev/null
for _ in $(seq 100); do
  [[ $(ctl list | awk '$2=="svc" && $4=="running"' | wc -l) -eq 6 ]] && break
  sleep 0.05
done
sleep 0.6  # one heartbeat: every task reported running

DEAD=(); FIRST=(); LAST=(); MOVED=(); OVERLAP=()
printf "\n== %d SIGKILL trials\n" "$TRIALS"
printf "  %-5s %-6s %-5s %12s %14s %14s %16s\n" trial node tasks "kill->dead" "kill->1st run" \
  "kill->last run" "orphans killed"
for trial in $(seq "$TRIALS"); do
  list=$(ctl list)
  victim=$(awk '$1=="0" && $2=="svc" {print $5}' <<<"$list")
  # Tasks on the victim and the attempt each will restart at.
  tasks_on "$list" "$victim"
  pid=$(pid_of "$victim")
  orphans=$( (pgrep -P "$pid" || true) | wc -l | tr -d ' ')  # task group leaders
  leftover_lines=$(grep -c "left running by a previous fleetlet" "$WORK/$victim.log" || true)
  deaths_before=$(grep -c "node $victim declared dead" "$WORK/master.log" || true)
  perl -e 'select(undef,undef,undef,rand(0.5))'
  t_kill=$(now_ms)
  kill -KILL "$pid"
  wait "$pid" 2>/dev/null || true

  # Wait (up to 20 s, progress every 5 s) for the death and every restart.
  done_waiting=""
  for i in $(seq 400); do
    deaths=$(grep -c "node $victim declared dead" "$WORK/master.log" || true)
    if [[ $deaths -gt $deaths_before ]]; then
      all=1
      for m in "${MOVING[@]}"; do
        read -r id att <<<"$m"
        grep -qh "started task $id (svc) attempt $att," "$WORK"/n*.log || { all=0; break; }
      done
      [[ $all -eq 1 ]] && { done_waiting=1; break; }
    fi
    ((i % 100 == 0)) && echo "  ... trial $trial still waiting ($((i / 20)) s)"
    sleep 0.05
  done
  [[ -n "$done_waiting" ]] || { echo "FAIL: trial $trial did not recover in 20 s"; ctl list; exit 1; }

  dead_ts=$(grep "node $victim declared dead" "$WORK/master.log" | tail -1 | cut -d' ' -f1)
  d=$(( $(to_ms "$dead_ts") - t_kill ))
  first=""; last=""
  for m in "${MOVING[@]}"; do
    read -r id att <<<"$m"
    ts=$(grep -h "started task $id (svc) attempt $att," "$WORK"/n*.log | head -1 | cut -d' ' -f1)
    s=$(( $(to_ms "$ts") - t_kill ))
    [[ -z "$first" || $s -lt $first ]] && first=$s
    [[ -z "$last" || $s -gt $last ]] && last=$s
  done
  (( d > 0 && first > 0 )) || { echo "FAIL: non-positive time (clock or midnight?)"; exit 1; }
  # The fleetlet comes back: same name, new epoch. It must first kill the
  # orphans of its SIGKILLed predecessor, which until now have been running
  # beside their replacements.
  start_agent "$victim"
  wait_ready "$victim"
  line=$(grep "left running by a previous fleetlet" "$WORK/$victim.log" | tail -1 || true)
  n_lines=$(grep -c "left running by a previous fleetlet" "$WORK/$victim.log" || true)
  [[ $n_lines -gt $leftover_lines ]] || { echo "FAIL: restarted $victim did not report its orphans"; exit 1; }
  killed=$(sed -E 's/.*killed ([0-9]+) task.*/\1/' <<<"$line")
  [[ $killed -eq $orphans ]] || { echo "FAIL: $victim killed $killed orphans, expected $orphans"; exit 1; }
  overlap=$(( $(to_ms "$(cut -d' ' -f1 <<<"$line")") - t_kill - first ))

  DEAD+=("$d"); FIRST+=("$first"); LAST+=("$last"); MOVED+=("${#MOVING[@]}"); OVERLAP+=("$overlap")
  printf "  %-5s %-6s %-5s %9d ms %11d ms %11d ms %6d after %4d ms\n" "$trial" "$victim" \
    "${#MOVING[@]}" "$d" "$first" "$last" "$killed" "$overlap"
done

stats() {  # label values...
  local label=$1; shift
  local sorted n
  sorted=$(printf '%s\n' "$@" | sort -n)
  n=$#
  # Median of an even count: mean of the two middle values.
  local lo hi
  lo=$(sed -n "$(( (n + 1) / 2 ))p" <<<"$sorted")
  hi=$(sed -n "$(( n / 2 + 1 ))p" <<<"$sorted")
  printf "  %-28s min %5d  median %5d  max %5d ms\n" "$label" "$(head -1 <<<"$sorted")" \
    $(( (lo + hi) / 2 )) "$(tail -1 <<<"$sorted")"
}
echo
echo "== summary over $TRIALS trials ($(printf '%s+' "${MOVED[@]}" | sed 's/+$//') tasks moved)"
stats "kill -> declared dead" "${DEAD[@]}"
stats "kill -> first replacement" "${FIRST[@]}"
stats "kill -> last replacement" "${LAST[@]}"
stats "orphans beside replacements" "${OVERLAP[@]}"
echo "  expected: dead in (2500, 3100] ms (6 x 500 ms of silence minus the kill's phase in"
echo "  the heartbeat cycle, plus up to one 100 ms tick); a replacement up to 500 ms after"
echo "  that (the target node's next heartbeat carries the start). Orphans ran beside their"
echo "  replacements until this script restarted the fleetlet: that number measures the"
echo "  script's restart, not fleet; on a real machine it is however long the agent is down"

echo
echo "== partition trial: SIGSTOP a fleetlet, its tasks keep running"
list=$(ctl list)
victim=$(awk '$1=="0" && $2=="svc" {print $5}' <<<"$list")
tasks_on "$list" "$victim"
pid=$(pid_of "$victim")
old_tasks=$( (pgrep -P "$pid" || true) | wc -l | tr -d ' ')
t_stop=$(now_ms)
kill -STOP "$pid"
for i in $(seq 400); do
  all=1
  for m in "${MOVING[@]}"; do
    read -r id att <<<"$m"
    grep -qh "started task $id (svc) attempt $att," "$WORK"/n*.log || { all=0; break; }
  done
  [[ $all -eq 1 ]] && break
  ((i % 100 == 0)) && echo "  ... still waiting for replacements ($((i / 20)) s)"
  sleep 0.05
done
[[ $all -eq 1 ]] || { echo "FAIL: replacements never started"; exit 1; }
still=$( (pgrep -P "$pid" || true) | wc -l | tr -d ' ')
echo "  $victim stopped; ${#MOVING[@]} task(s) restarted elsewhere; the stopped node still runs $still of its $old_tasks old copies"
sleep 1
t_cont=$(now_ms)
kill -CONT "$pid"
for i in $(seq 200); do
  grep -q "fenced:" "$WORK/$victim.log" && break
  sleep 0.05
done
fenced_line=$(grep "fenced:" "$WORK/$victim.log" | tail -1 || true)
[[ -n "$fenced_line" ]] || { echo "FAIL: $victim was never fenced"; exit 1; }
wait_ready "$victim"
after=$( (pgrep -P "$pid" || true) | wc -l | tr -d ' ')
fenced_ms=$(to_ms "$(cut -d' ' -f1 <<<"$fenced_line")")
first_rep=""
for m in "${MOVING[@]}"; do
  read -r id att <<<"$m"
  ts=$(grep -h "started task $id (svc) attempt $att," "$WORK"/n*.log | head -1 | cut -d' ' -f1)
  s=$(to_ms "$ts")
  [[ -z "$first_rep" || $s -lt $first_rep ]] && first_rep=$s
done
echo "  agent log: ${fenced_line#* }"
echo "  SIGCONT -> fenced and its copies killed: $(( fenced_ms - t_cont )) ms"
echo "  old and new copies ran side by side for $(( fenced_ms - first_rep )) ms"
echo "    (= partition length $(( t_cont - t_stop )) ms - time to restart elsewhere $(( first_rep - t_stop )) ms"
echo "       + time to be told $(( fenced_ms - t_cont )) ms: it grows with the partition, unbounded)"
echo "  copies left on $victim after fencing: $after; $victim is $(node_state "$victim") again"
[[ $after -eq 0 ]] || { echo "FAIL: fenced node still runs tasks"; exit 1; }
