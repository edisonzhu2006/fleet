#!/usr/bin/env bash
# leader_failover.sh: SIGKILL the Raft leader of a 3-replica fleetmaster
# mid-workload, again and again, then partition it; check that nothing
# committed is lost, nothing runs twice, and there is never a split brain
# (m4b).
#
# Starts 3 fleetmaster replicas (--id/--peers, each with its own state
# directory: Raft log, term and vote, snapshots; F_FULLFSYNC; a log
# compaction every 200 entries, so restarted replicas are sometimes caught
# up by InstallSnapshot) and 3 fleetlets given all three addresses, each
# writing an event log of every task instance's start and exit. Then:
#   - submits 4 long-running services (sleep 600) that must survive every
#     failover untouched: same attempt, started exactly once;
#   - runs a background client that submits a 2-task batch job every
#     250 ms (each task sleeps 0.1 to 0.9 s) through fleetctl with all
#     three addresses, and records the task ids acknowledged. fleetctl
#     retries across a failover with the same request id, so a submission
#     whose first reply was lost is answered, not admitted twice;
#   - TRIALS times (default 20): waits a random 1.0 to 2.5 s, finds the
#     leader (fleetctl status), SIGKILLs it, restarts it at once on the
#     same state directory, and measures from the timestamped logs:
#         kill -> a new leader elected        ("became leader of term T")
#         kill -> the new leader serving      ("serving as leader": its
#                 no-op committed and applied)
#         kill -> scheduling again            ("recovery complete": every
#                 agent has reported, the report-before-schedule rule)
#     and waits for the restarted replica to catch up before the next one;
#   - the partition test: isolates the current leader from the other two
#     replicas (SIGUSR1: it drops all Raft traffic; agents and clients can
#     still reach it), shows that a submission sent to it is not
#     acknowledged and that it cannot advance its commit index, that the
#     other two elect a new leader of a higher term that serves, then heals
#     it (SIGUSR2) and shows it steps down to follower, catches up, and that
#     the submission it took while isolated never took effect;
#   - stops the client, waits for every acknowledged task to finish, and
#     checks: every acknowledged task succeeded; no job was admitted
#     twice; every service is still at attempt 1; for every task, the [start, exit] intervals of its
#     instances across all agents' event logs never overlap; no agent was
#     fenced or re-registered; at most one leader per term in every log.
#
# First, before any cluster is up, replicabench times one-record writes and
# reads against an m4 single master and against the leader of 3 replicas
# (what replication costs per call). REPLICABENCH=0 skips it.
#
# Usage: bench/leader_failover.sh              (uses build/bin)
#        TRIALS=5 BIN=build-asan/bin PORT=17611 bench/leader_failover.sh
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIN=${BIN:-$ROOT/build/bin}
PORT=${PORT:-17501}
TRIALS=${TRIALS:-20}
WORK=/private/tmp/fleet/leader_failover
MARK=fleet_lf_$$
IDS=(1 2 3)
ADDR=(); PEERS=""
for i in "${IDS[@]}"; do
  ADDR[$i]=127.0.0.1:$((PORT + i - 1))
  PEERS+="${PEERS:+,}$i=${ADDR[$i]}"
done
ALL="${ADDR[1]},${ADDR[2]},${ADDR[3]}"
NODES=(n1 n2 n3)
export ASAN_OPTIONS=${ASAN_OPTIONS:-detect_container_overflow=0}

rm -rf "$WORK"
mkdir -p "$WORK"
touch "$WORK/acked" "$WORK/refused" "$WORK/unknown"
CLIENT_PID=""
AGENT_PIDS=()
declare -a RPID

ctl() { "$BIN/fleetctl" --master "$ALL" --timeout-ms 3000 "$@"; }
now_ms() {
  perl -MTime::HiRes=time -e '$t=time; @l=localtime($t); printf "%d", (($l[2]*60+$l[1])*60+$l[0])*1000 + int(($t-int($t))*1000)'
}
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
  for i in "${IDS[@]}"; do
    if [[ -n "${RPID[$i]:-}" ]]; then
      kill -TERM "${RPID[$i]}" 2>/dev/null || true
      wait "${RPID[$i]}" 2>/dev/null || true
    fi
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

start_replica() {  # logs are appended, one file per replica across incarnations
  local i=$1
  "$BIN/fleetmaster" --id "$i" --peers "$PEERS" --state-dir "$WORK/r$i" --snapshot-every 200 \
    --heartbeat-ms 500 --suspect-after 3 --dead-after 6 --tick-ms 100 2>>"$WORK/r$i.log" &
  RPID[$i]=$!
}

# "id term" of the serving leader, or nothing.
serving_leader() { ctl status 2>/dev/null | awk '$3=="leader" && /serving/ {print $2, $4; exit}'; }
status_of() { "$BIN/fleetctl" --master "${ADDR[$1]}" --timeout-ms 1000 status 2>/dev/null | awk 'NR==2'; }

echo "== machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), load: $(uptime | sed 's/.*load averages*: //')"
if [[ "${REPLICABENCH:-1}" != 0 ]]; then
  echo "== replication cost per call (replicabench, in-process, loopback gRPC)"
  "$BIN/replicabench" --n 200 | sed 's/^/  /'
  echo
fi
for i in "${IDS[@]}"; do start_replica "$i"; done
for _ in $(seq 100); do [[ -n "$(serving_leader)" ]] && break; sleep 0.05; done
for n in "${NODES[@]}"; do
  "$BIN/fleetlet" --master "$ALL" --name "$n" --cpu 4000 --mem 4096 --work-dir "$WORK/$n" \
    --stop-grace-ms 1000 --event-log "$WORK/$n.events" 2>>"$WORK/$n.log" &
  AGENT_PIDS+=($!)
done
for _ in $(seq 100); do
  [[ $(ctl list 2>/dev/null | awk '$2=="ready"' | wc -l) -eq 3 ]] && break
  sleep 0.05
done
ctl status | sed 's/^/  /'
ctl submit --job svc --priority 2 --cpu 500 --mem 64 --count 4 -- \
  /bin/sh -c 'exec sleep 600' "$MARK" | sed 's/^/  /'
for _ in $(seq 100); do
  [[ $(ctl list | awk '$2=="svc" && $4=="running"' | wc -l) -eq 4 ]] && break
  sleep 0.05
done

# The client: a 2-task batch job every 250 ms. fleetctl follows redirects
# and retries an unavailable master until its deadline; a submission that
# comes back as of unknown outcome is recorded apart (it may exist).
(
  k=0
  while [[ ! -f "$WORK/stop" ]]; do
    k=$((k + 1))
    d=$((RANDOM % 9 + 1))
    if out=$("$BIN/fleetctl" --master "$ALL" --timeout-ms 3000 submit --job "b$k" --priority 1 \
        --cpu 250 --mem 32 --count 2 -- /bin/sh -c "sleep 0.$d" "$MARK" 2>&1); then
      sed -E 's/^submitted job [^:]*://' <<<"$out" | tr ' ' '\n' | grep -E '^[0-9]+$' >>"$WORK/acked"
    elif grep -q "outcome unknown" <<<"$out"; then
      echo "$k" >>"$WORK/unknown"
    else
      echo "$k" >>"$WORK/refused"
    fi
    sleep 0.25
  done
) &
CLIENT_PID=$!
sleep 1

ELECT=(); SERVE=(); SCHED=(); CATCH=()
printf "\n== %d leader SIGKILL trials (the killed replica restarted at once each time)\n" "$TRIALS"
printf "  %-5s %-7s %-7s %12s %12s %13s %10s %9s\n" trial killed "new" "kill->elect" "kill->serve" "kill->sched" "catch-up" elections
for trial in $(seq "$TRIALS"); do
  perl -e 'select(undef,undef,undef,1.0+rand(1.5))'
  read -r old old_term < <(serving_leader) || true
  [[ -n "${old:-}" ]] || { echo "FAIL: no serving leader before trial $trial"; ctl status; exit 1; }
  t_kill=$(now_ms)
  kill -KILL "${RPID[$old]}"
  wait "${RPID[$old]}" 2>/dev/null || true
  start_replica "$old"
  # Wait for a leader of a higher term to finish its report window.
  ok=""
  for i in $(seq 400); do
    read -r new new_term < <(serving_leader) || true
    if [[ -n "${new:-}" && "${new_term:-0}" -gt "$old_term" ]] &&
       awk -v t="$new_term" '$0 ~ "serving as leader of term "t":" {s=1} s && /recovery complete/ {f=1} END {exit !f}' "$WORK/r$new.log"; then
      ok=1; break
    fi
    ((i % 100 == 0)) && echo "  ... trial $trial: no new serving leader yet ($((i / 20)) s)"
    sleep 0.05
  done
  [[ -n "$ok" ]] || { echo "FAIL: trial $trial: no new leader scheduling within 20 s"; ctl status; exit 1; }
  elect_ts=$(grep -h "became leader of term $new_term " "$WORK/r$new.log" | tail -1 | cut -d' ' -f1)
  serve_ts=$(grep -h "serving as leader of term $new_term:" "$WORK/r$new.log" | tail -1 | cut -d' ' -f1)
  sched_ts=$(awk -v t="$new_term" '$0 ~ "serving as leader of term "t":" {s=1} s && /recovery complete/ {print $1; exit}' "$WORK/r$new.log")
  elections=$(cat "$WORK"/r*.log | awk -v o="$old_term" -v n="$new_term" '/became leader of term/ {
      for (i = 1; i <= NF; i++) if ($i == "term") t = $(i+1) + 0
      if (t > o && t <= n) c++ } END {print c+0}')
  # The restarted replica rejoins as a follower and catches up, timed from
  # here (the new leader is scheduling; the replica was restarted right
  # after the kill), polled every 50 ms: the "catch-up" column (or it won
  # the election itself: it restarts in about 30 ms with the most complete
  # log, so it can, and a leader is caught up by definition).
  t_back=$(now_ms); caught=""
  [[ "$new" == "$old" ]] && caught=1
  for i in $(seq 400); do
    [[ -n "$caught" ]] && break
    lead_row=$(status_of "$new")  # first, so the follower is compared with an earlier commit
    row=$(status_of "$old")
    if [[ -n "$row" && "$(awk '{print $3}' <<<"$row")" == "follower" ]] &&
       [[ $(awk '{print $7}' <<<"$row") -ge $(awk '{print $6}' <<<"$lead_row") ]]; then
      caught=1; break
    fi
    sleep 0.05
  done
  [[ -n "$caught" ]] || { echo "FAIL: trial $trial: replica $old did not catch up in 20 s"; ctl status; exit 1; }
  e=$(( $(to_ms "$elect_ts") - t_kill )); s=$(( $(to_ms "$serve_ts") - t_kill ))
  c=$(( $(to_ms "$sched_ts") - t_kill )); u=$(( $(now_ms) - t_back ))
  ELECT+=("$e"); SERVE+=("$s"); SCHED+=("$c"); CATCH+=("$u")
  printf "  %-5s %-7s %-7s %9d ms %9d ms %10d ms %7d ms %9s\n" "$trial" "r$old/t$old_term" \
    "r$new/t$new_term" "$e" "$s" "$c" "$u" "$elections"
done

echo
echo "== partition test: cut the leader off from the other two replicas"
read -r L L_term < <(serving_leader)
OTHERS=""; for i in "${IDS[@]}"; do [[ $i -ne $L ]] && OTHERS+="${OTHERS:+,}${ADDR[$i]}"; done
echo "  leader is replica $L (term $L_term); before:"
ctl status | sed 's/^/    /'
kill -USR1 "${RPID[$L]}"
sleep 0.1
before=$(status_of "$L")  # from here on its commit index must not move
t0=$(now_ms)
if out=$("$BIN/fleetctl" --master "${ADDR[$L]}" --timeout-ms 2000 submit --job split --cpu 100 --mem 16 -- \
    /bin/sh -c 'exit 0' "$MARK" 2>&1); then
  echo "FAIL: the isolated leader acknowledged a submission: $out"; exit 1
fi
echo "  submission sent to the isolated leader: not acknowledged after $(( $(now_ms) - t0 )) ms: $out"
new=""; for i in $(seq 200); do
  read -r new new_term < <("$BIN/fleetctl" --master "$OTHERS" --timeout-ms 1000 status 2>/dev/null |
    awk '$3=="leader" && /serving/ {print $2, $4; exit}') || true
  [[ -n "${new:-}" && "${new_term:-0}" -gt "$L_term" ]] && break
  new=""; sleep 0.05
done
[[ -n "$new" ]] || { echo "FAIL: the majority did not elect a new leader"; exit 1; }
iso_ts=$(grep -h "ISOLATED from every other replica" "$WORK/r$L.log" | tail -1 | cut -d' ' -f1)
won_ts=$(grep -h "became leader of term $new_term " "$WORK/r$new.log" | tail -1 | cut -d' ' -f1)
srv_ts=$(grep -h "serving as leader of term $new_term:" "$WORK/r$new.log" | tail -1 | cut -d' ' -f1)
echo "  the other two elected replica $new leader of term $new_term $(( $(to_ms "$won_ts") - $(to_ms "$iso_ts") )) ms after the isolation, serving $(( $(to_ms "$srv_ts") - $(to_ms "$iso_ts") )) ms after it"
"$BIN/fleetctl" --master "$OTHERS" --timeout-ms 3000 submit --job majority --cpu 100 --mem 16 -- \
  /bin/sh -c 'exit 0' "$MARK" | sed 's/^/  majority side: /'
sleep 0.5
during=$(status_of "$L")
echo "  while isolated (replica $L still thinks it leads term $L_term, but cannot commit):"
ctl status | sed 's/^/    /'
c_before=$(awk '{print $6}' <<<"$before"); c_during=$(awk '{print $6}' <<<"$during")
l_during=$(awk '{print $8}' <<<"$during"); r_during=$(awk '{print $3}' <<<"$during")
t_during=$(awk '{print $4}' <<<"$during")
split_brain=0
[[ "$r_during" == "leader" && "$t_during" == "$L_term" ]] || echo "  note: isolated replica is $r_during of term $t_during"
if [[ $c_during -ne $c_before ]]; then
  echo "FAIL: the isolated leader's commit index moved from $c_before to $c_during"; split_brain=1
fi
echo "  isolated replica: commit index $c_before -> $c_during, last index $l_during (its uncommitted entries)"
kill -USR2 "${RPID[$L]}"
t1=$(now_ms); healed=""
for i in $(seq 200); do
  lead_row=$(status_of "$new"); row=$(status_of "$L")
  if [[ "$(awk '{print $3}' <<<"$row")" == "follower" && $(awk '{print $4}' <<<"$row") -ge $new_term &&
        $(awk '{print $7}' <<<"$row") -ge $(awk '{print $6}' <<<"$lead_row") ]]; then
    healed=1; break
  fi
  sleep 0.05
done
[[ -n "$healed" ]] || { echo "FAIL: the old leader did not step down and catch up after healing"; ctl status; exit 1; }
lift_ts=$(grep -h "isolation lifted" "$WORK/r$L.log" | tail -1 | cut -d' ' -f1)
down_ts=$(grep -h "no longer serving (leader of term $L_term)" "$WORK/r$L.log" | tail -1 | cut -d' ' -f1)
echo "  healed: replica $L stepped down $(( $(to_ms "$down_ts") - $(to_ms "$lift_ts") )) ms after the isolation was lifted (its log), and had caught up (applied >= the leader's commit when polled) $(( $(now_ms) - t1 )) ms after:"
ctl status | sed 's/^/    /'
split_tasks=$(ctl list | awk '$2=="split"' | wc -l | tr -d ' ')
majority_tasks=$(ctl list | awk '$2=="majority"' | wc -l | tr -d ' ')
echo "  tasks of the submission the isolated leader took: $split_tasks (its entries were overwritten); of the majority's: $majority_tasks"
[[ $split_tasks -eq 0 && $majority_tasks -eq 1 ]] || { echo "FAIL: split/majority submissions"; split_brain=1; }
grep -h "no longer serving\|stepped down" "$WORK/r$L.log" | tail -2 | sed 's/^/  log: /'

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
unknown=$(wc -l <"$WORK/unknown" | tr -d ' ')
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
# A job admitted twice would have more than its 2 tasks (each submission
# carries a request id; a retry must get the first attempt's ids back).
doubled=$(awk '$1 ~ /^[0-9]+$/ && $2 ~ /^b[0-9]+$/ {c[$2]++} END {n=0; for (j in c) if (c[j] > 2) n++; print n}' <<<"$LIST")

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
reregistered=$(cat "$WORK"/r*.log | grep -c "re-registered" || true)
# At most one leader per term, across every replica's whole log.
dup_terms=$(cat "$WORK"/r*.log | { grep "became leader of term" || true; } |
  sed -E 's/.*fleetmaster\[([0-9]+)\]: became leader of term ([0-9]+) .*/\2 \1/' | sort -u |
  awk '{c[$1]++} END {n=0; for (t in c) if (c[t] > 1) n++; print n}')
terms=$(cat "$WORK"/r*.log | grep -c "became leader of term" || true)
compactions=$(cat "$WORK"/r*.log | grep -c "log compacted" || true)
installs=$(cat "$WORK"/r*.log | grep -c "state machine restored from snapshot" || true)
# (grep finding nothing must not end the script under pipefail.)
requeued=$(cat "$WORK"/r*.log | { grep "reported after restart" || true; } |
  sed -E 's/.* ([0-9]+) not running there requeued.*/\1/' | awk '{s+=$1} END {print s+0}')

stats() {  # label values...
  local label=$1; shift
  local sorted n lo hi
  sorted=$(printf '%s\n' "$@" | sort -n)
  n=$#
  lo=$(sed -n "$(( (n + 1) / 2 ))p" <<<"$sorted")
  hi=$(sed -n "$(( n / 2 + 1 ))p" <<<"$sorted")
  printf "  %-38s min %5d  median %5d  max %5d ms\n" "$label" "$(head -1 <<<"$sorted")" \
    $(( (lo + hi) / 2 )) "$(tail -1 <<<"$sorted")"
}
echo
echo "== summary over $TRIALS leader SIGKILLs and one partition"
stats "kill -> new leader elected" "${ELECT[@]}"
stats "kill -> new leader serving" "${SERVE[@]}"
stats "kill -> scheduling (agents reported)" "${SCHED[@]}"
stats "leader scheduling -> old one caught up" "${CATCH[@]}"
echo "  leaderships over the run: $terms, terms with two leaders: $dup_terms"
echo "  log compactions: $compactions, snapshot restores (startup or InstallSnapshot): $installs"
echo "  batch tasks acknowledged to the client: $acked (refused: $refused submissions, outcome unknown: $unknown)"
echo "  batch tasks the master has: $in_master (any above $acked were committed after the client gave up)"
echo "  batch jobs admitted twice: $doubled"
echo "  lost (acknowledged but not succeeded): $lost"
echo "  services still running at attempt 1: $svc_ok of 4"
echo "  placed but never started before a leader died (requeued at the node's first report): $requeued"
echo "  tasks with event-log instances: $tasks_seen, instances: $instances, tasks started more than once: $multi"
echo "  overlapping instances of one task: $overlaps"
echo "  agents fenced: $fenced, nodes re-registered: $reregistered"
fail=0
[[ $lost -eq 0 ]] || { echo "FAIL: $lost task(s) lost"; fail=1; }
[[ $doubled -eq 0 ]] || { echo "FAIL: $doubled job(s) admitted twice"; fail=1; }
[[ $svc_ok -eq 4 ]] || { echo "FAIL: services were disturbed"; fail=1; }
[[ $overlaps -eq 0 ]] || { echo "FAIL: a task ran twice at the same time"; fail=1; }
[[ $fenced -eq 0 && $reregistered -eq 0 ]] || { echo "FAIL: an agent lost its registration"; fail=1; }
[[ $dup_terms -eq 0 && $split_brain -eq 0 ]] || { echo "FAIL: split brain"; fail=1; }
[[ $fail -eq 0 ]] && echo "  PASS: no committed task lost, none ran twice concurrently, one leader per term"
exit $fail
