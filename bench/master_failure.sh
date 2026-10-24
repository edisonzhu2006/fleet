#!/usr/bin/env bash
# master_failure.sh: SIGKILL the master mid-workload, again and again, and
# check that nothing is lost and nothing runs twice at once (m4).
#
# Starts fleetmaster with a state directory (write-ahead log + snapshots,
# F_FULLFSYNC, a snapshot every 200 records so restarts recover from a
# snapshot plus a log tail) and four fleetlets with ProcessRuntime, each
# writing an event log of every task instance's start and exit
# (fleetlet --event-log). Then:
#   - submits 4 long-running services (sleep 600) that must survive every
#     master crash untouched: same attempt, started exactly once;
#   - runs a background client that submits a 2-task batch job every
#     250 ms (each task sleeps 0.1 to 0.9 s) and records the task ids the
#     master acknowledged; submissions that fail while the master is down
#     are simply not acknowledged;
#   - TRIALS times (default 20): waits a random 1.0 to 2.5 s, SIGKILLs the
#     master, starts it again at once on the same state directory, and
#     measures from the timestamped logs:
#         kill -> serving RPCs again ("listening")
#         kill -> scheduling again ("recovery complete": every agent has
#                 reported, the report-before-schedule rule)
#     plus what recovery found (snapshot, records replayed, torn tail);
#   - stops the client, waits for every acknowledged task to finish, and
#     checks:
#         no task lost: every acknowledged task succeeded, every service is
#           still running at attempt 1;
#         none ran twice concurrently: for every task, the [start, exit]
#           intervals of its instances across all agents' event logs do not
#           overlap (start is stamped before the process is started, exit
#           after the agent saw it gone, so each interval contains the
#           process's real lifetime);
#         no agent was fenced or re-registered (each kept its epoch).
#
# Usage: bench/master_failure.sh              (uses build/bin)
#        TRIALS=5 BIN=build-asan/bin bench/master_failure.sh
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIN=${BIN:-$ROOT/build/bin}
PORT=${PORT:-17370}
TRIALS=${TRIALS:-20}
WORK=/private/tmp/fleet/master_failure
MARK=fleet_mf_$$
ADDR=127.0.0.1:$PORT
NODES=(n1 n2 n3 n4)
export ASAN_OPTIONS=${ASAN_OPTIONS:-detect_container_overflow=0}

rm -rf "$WORK"
mkdir -p "$WORK"
touch "$WORK/acked" "$WORK/refused"
MASTER_PID=""
CLIENT_PID=""
AGENT_PIDS=()

ctl() { "$BIN/fleetctl" --master "$ADDR" --timeout-ms 2000 "$@"; }
now_ms() {
  perl -MTime::HiRes=time -e '$t=time; @l=localtime($t); printf "%d", (($l[2]*60+$l[1])*60+$l[0])*1000 + int(($t-int($t))*1000)'
}
# "HH:MM:SS.mmm" -> ms of the day (a run across midnight fails loudly).
to_ms() { awk -F'[:.]' '{print (($1*60+$2)*60+$3)*1000+$4}' <<<"$1"; }

cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  echo
  echo "== teardown"
  if [[ -n "$CLIENT_PID" ]]; then
    touch "$WORK/stop"
    wait "$CLIENT_PID" 2>/dev/null || true
  fi
  for p in "${AGENT_PIDS[@]}"; do
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

start_master() {  # logs are appended, one file across every incarnation
  "$BIN/fleetmaster" --listen "$ADDR" --heartbeat-ms 500 --suspect-after 3 --dead-after 6 \
    --tick-ms 100 --state-dir "$WORK/state" --snapshot-every 200 2>>"$WORK/master.log" &
  MASTER_PID=$!
}

echo "== machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), load: $(uptime | sed 's/.*load averages*: //')"
start_master
for _ in $(seq 50); do ctl list >/dev/null 2>&1 && break; sleep 0.1; done
for n in "${NODES[@]}"; do
  "$BIN/fleetlet" --master "$ADDR" --name "$n" --cpu 4000 --mem 4096 --work-dir "$WORK/$n" \
    --stop-grace-ms 1000 --event-log "$WORK/$n.events" 2>>"$WORK/$n.log" &
  AGENT_PIDS+=($!)
done
for _ in $(seq 100); do
  [[ $(ctl list | awk '$2=="ready"' | wc -l) -eq 4 ]] && break
  sleep 0.05
done

ctl submit --job svc --priority 2 --cpu 500 --mem 64 --count 4 -- \
  /bin/sh -c 'exec sleep 600' "$MARK" | sed 's/^/  /'
for _ in $(seq 100); do
  [[ $(ctl list | awk '$2=="svc" && $4=="running"' | wc -l) -eq 4 ]] && break
  sleep 0.05
done

# The client: a 2-task batch job every 250 ms, acknowledged ids to a file.
# It stops when the stop file appears, after finishing the submission in
# flight, so every id the master acknowledged is recorded.
(
  k=0
  while [[ ! -f "$WORK/stop" ]]; do
    k=$((k + 1))
    d=$((RANDOM % 9 + 1))
    if out=$("$BIN/fleetctl" --master "$ADDR" --timeout-ms 1000 submit --job "b$k" --priority 1 \
        --cpu 250 --mem 32 --count 2 -- /bin/sh -c "sleep 0.$d" "$MARK" 2>/dev/null); then
      sed -E 's/^submitted job [^:]*://' <<<"$out" | tr ' ' '\n' | grep -E '^[0-9]+$' >>"$WORK/acked"
    else
      echo "$k" >>"$WORK/refused"
    fi
    sleep 0.25
  done
) &
CLIENT_PID=$!
sleep 1

SERVE=(); SCHED=(); WINDOW=()
printf "\n== %d SIGKILL trials (master restarted at once each time)\n" "$TRIALS"
printf "  %-5s %8s %13s %15s %12s  %s\n" trial lsn "kill->serving" "kill->schedule" "window" "recovered from"
for trial in $(seq "$TRIALS"); do
  perl -e 'select(undef,undef,undef,1.0+rand(1.5))'
  done_before=$(grep -c "recovery complete" "$WORK/master.log" || true)
  listen_before=$(grep -c "listening on port" "$WORK/master.log" || true)
  t_kill=$(now_ms)
  kill -KILL "$MASTER_PID"
  wait "$MASTER_PID" 2>/dev/null || true
  start_master
  ok=""
  for i in $(seq 400); do
    if [[ $(grep -c "recovery complete" "$WORK/master.log" || true) -gt $done_before ]]; then ok=1; break; fi
    ((i % 100 == 0)) && echo "  ... trial $trial still recovering ($((i / 20)) s)"
    sleep 0.05
  done
  [[ -n "$ok" ]] || { echo "FAIL: trial $trial: master did not finish recovery in 20 s"; tail -20 "$WORK/master.log"; exit 1; }
  listen_ts=$(grep "listening on port" "$WORK/master.log" | sed -n "$((listen_before + 1))p" | cut -d' ' -f1)
  done_line=$(grep "recovery complete" "$WORK/master.log" | sed -n "$((done_before + 1))p")
  state_line=$(grep "fleetmaster: state dir" "$WORK/master.log" | tail -1)
  serve=$(( $(to_ms "$listen_ts") - t_kill ))
  sched=$(( $(to_ms "$(cut -d' ' -f1 <<<"$done_line")") - t_kill ))
  window=$(sed -E 's/.*recovery complete after ([0-9]+) ms.*/\1/' <<<"$done_line")
  lsn=$(sed -E 's/.*; lsn ([0-9]+);.*/\1/' <<<"$state_line")
  from=$(sed -E 's/.*state dir [^:]*: (.*); lsn.*/\1/' <<<"$state_line" | \
    sed -E 's/ record\(s\) \(skipped ([0-9]+) already in the snapshot\), discarded a torn tail of ([0-9]+) byte\(s\)/, skipped \1, torn \2 B/')
  SERVE+=("$serve"); SCHED+=("$sched"); WINDOW+=("$window")
  printf "  %-5s %8s %10d ms %12d ms %9s ms  %s\n" "$trial" "$lsn" "$serve" "$sched" "$window" "$from"
done

echo
echo "== stopping the client; waiting for every acknowledged task to finish"
touch "$WORK/stop"
wait "$CLIENT_PID" 2>/dev/null || true
CLIENT_PID=""
for i in $(seq 600); do
  left=$(ctl list | awk '$1 ~ /^[0-9]+$/ && $2 ~ /^b[0-9]+$/ && $4 != "succeeded"' | wc -l | tr -d ' ')
  [[ $left -eq 0 ]] && break
  ((i % 100 == 0)) && echo "  ... $left batch task(s) not finished yet ($((i / 20)) s)"
  sleep 0.05
done
LIST=$(ctl list)
acked=$(sort -n -u "$WORK/acked" | wc -l | tr -d ' ')
refused=$(wc -l <"$WORK/refused" | tr -d ' ')
in_master=$(awk '$1 ~ /^[0-9]+$/ && $2 ~ /^b[0-9]+$/' <<<"$LIST" | wc -l | tr -d ' ')
lost=0
while read -r id; do
  st=$(awk -v id="$id" '$1==id {print $4}' <<<"$LIST")
  if [[ "$st" != "succeeded" ]]; then
    echo "  LOST: acknowledged task $id is '${st:-missing}'"
    lost=$((lost + 1))
  fi
done < <(sort -n -u "$WORK/acked")
svc_ok=$(awk '$2=="svc" && $4=="running" && $6=="1"' <<<"$LIST" | wc -l | tr -d ' ')

# Overlap: per task, instances sorted by start; each must start at or
# after the previous one's exit. An instance with no exit yet is still
# running (open-ended).
read -r tasks_seen instances multi overlaps < <(cat "$WORK"/n*.events | perl -ne '
  my ($ts, $what, $task, $att, $ep, $node) = split;
  my $k = "$att/$ep/$node";
  if ($what eq "start") { $s{$task}{$k} = $ts } else { $e{$task}{$k} = $ts }
  END {
    my ($n, $inst, $multi, $ov) = (0, 0, 0, 0);
    for my $t (keys %s) {
      $n++;
      my @iv = sort { $a->[0] <=> $b->[0] }
               map { [$s{$t}{$_}, exists $e{$t}{$_} ? $e{$t}{$_} : 9e18] } keys %{$s{$t}};
      $inst += @iv;
      $multi++ if @iv > 1;
      for my $i (1 .. $#iv) {
        if ($iv[$i][0] < $iv[$i-1][1]) {
          $ov++;
          print STDERR "  OVERLAP: task $t: instance starting at $iv[$i][0] us began before the previous one exited at $iv[$i-1][1] us\n";
        }
      }
    }
    print "$n $inst $multi $ov\n";
  }')
fenced=$(cat "$WORK"/n*.log | grep -c "fenced:" || true)
reregistered=$(grep -c "re-registered" "$WORK/master.log" || true)
snapshots=$(grep -c "snapshot at lsn .* written" "$WORK/master.log" || true)
# "node n2 reported after restart: 6 task(s) confirmed running, 2 not
# running there requeued; ..." -> the 2: placements whose start never
# reached the agent before the crash.
requeued=$(grep "reported after restart" "$WORK/master.log" | \
  sed -E 's/.* ([0-9]+) not running there requeued.*/\1/' | awk '{s+=$1} END {print s+0}')

stats() {  # label values...
  local label=$1; shift
  local sorted n lo hi
  sorted=$(printf '%s\n' "$@" | sort -n)
  n=$#
  lo=$(sed -n "$(( (n + 1) / 2 ))p" <<<"$sorted")
  hi=$(sed -n "$(( n / 2 + 1 ))p" <<<"$sorted")
  printf "  %-30s min %5d  median %5d  max %5d ms\n" "$label" "$(head -1 <<<"$sorted")" \
    $(( (lo + hi) / 2 )) "$(tail -1 <<<"$sorted")"
}
echo
echo "== summary over $TRIALS master SIGKILLs"
stats "kill -> serving RPCs" "${SERVE[@]}"
stats "kill -> scheduling (recovered)" "${SCHED[@]}"
stats "report window" "${WINDOW[@]}"
echo "  snapshots written during the run: $snapshots"
echo "  batch tasks acknowledged to the client: $acked (submissions refused while the master was down: $refused)"
echo "  batch tasks the master has: $in_master (any above $acked were committed, then the master died before replying)"
echo "  lost (acknowledged but not succeeded): $lost"
echo "  services still running at attempt 1: $svc_ok of 4"
echo "  placed before a crash but never started (requeued at the node's first report): $requeued"
echo "  tasks with event-log instances: $tasks_seen, instances: $instances, tasks started more than once: $multi"
echo "  overlapping instances of one task: $overlaps"
echo "  agents fenced: $fenced, nodes re-registered: $reregistered"
fail=0
[[ $lost -eq 0 ]] || { echo "FAIL: $lost task(s) lost"; fail=1; }
[[ $svc_ok -eq 4 ]] || { echo "FAIL: services were disturbed"; fail=1; }
[[ $overlaps -eq 0 ]] || { echo "FAIL: a task ran twice at the same time"; fail=1; }
[[ $fenced -eq 0 && $reregistered -eq 0 ]] || { echo "FAIL: an agent lost its registration"; fail=1; }
[[ $fail -eq 0 ]] && echo "  PASS: no task lost, none ran twice concurrently"
exit $fail
