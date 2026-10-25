#!/usr/bin/env bash
# stateful_failover.sh: kill nodes under running streamkit jobs and check
# that every job still produces exactly the oracle's output (m5).
#
# Setup, once:
#   - adstack's gen writes a 2M-event input (fixed seed), and adstack's
#     batch oracle computes the ground truth once (cached by seed in
#     /private/tmp/fleet-m5/stateful_input);
#   - 3 fleetmaster replicas (Raft, m4b) and 3 fleetlets (ProcessRuntime,
#     4 cores and 4 GB each), every agent writing an event log of each
#     task instance's start and exit.
# Then ROUNDS times (default 10):
#   - submits 3 stateful streamkit jobs (2 cores each, so a node holds two
#     and the survivors of one kill still have room for all three), each
#     with its own checkpoint directory and output file in the shared
#     directory, a checkpoint every 100K events, throttled to 150K events/s
#     so an uninterrupted run takes about 14 s; the resume command is the
#     same command plus --resume;
#   - while any job is unfinished: waits a random 3.0 to 6.0 s (and until
#     every node is up again, so one node is down at a time and the
#     survivors always have room), picks a random node that runs at least
#     one job, and kills the whole node:
#     SIGSTOP its fleetlet (so it cannot start anything more), SIGKILL the
#     process group of every task it runs, then SIGKILL the fleetlet: a
#     machine dying with its tasks. The node comes back (fleetlet restarted
#     under the same name) after a random 0.5 to 4.0 s, so a node is
#     sometimes back before the master would declare it dead (it then
#     re-registers, which requeues its tasks at once) and sometimes after;
#   - waits for every job to finish and checks each one: SUCCEEDED, its
#     output identical to the oracle's (adstack bench/compare.sh), its
#     checkpoint directory gone (streamkit removes it on completion).
# At the end, over all rounds:
#   - no two attempts of one task ever ran at the same time: from the
#     agents' event logs, each attempt's [start, exit] interval, where an
#     attempt on a killed node ends at the kill (the script's timestamp,
#     taken after the SIGKILLs), must not overlap another attempt's;
#   - no start was refused by the attempt fence (none should be: every
#     replacement is placed only after its old node is dead or back);
#   - totals: node kills, failovers (attempts after the first), resumes
#     (fresh vs from a MANIFEST), completed streamkit runs (more than one
#     per job when a run finished but its exit was lost with its node),
#     and for every (kill, task on the killed node) the time from the kill
#     to the replacement attempt's start on a node, min / median / max,
#     split by how the master learned the node was gone;
#   - machine pauses: a probe records every stretch over 500 ms in which
#     no process ran (a laptop sleeping with its lid closed), and timing
#     samples such a pause overlapped are reported apart from the others.
#
# Usage: bench/stateful_failover.sh | tee bench/results/stateful_failover.txt
#        ROUNDS=2 BIN=build-asan/bin PORT=17721 bench/stateful_failover.sh
# Work files in $WORK (default /private/tmp/fleet-m5/stateful_failover).
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIN=${BIN:-$ROOT/build/bin}
ADSTACK=${ADSTACK:-$(cd "$ROOT/../adstack" && pwd)}
SK=$ADSTACK/build/bin
PORT=${PORT:-17701}
ROUNDS=${ROUNDS:-10}
EVENTS=${EVENTS:-2000000}
SEED=${SEED:-7}
EPS=${EPS:-150000}
WORK=${WORK:-/private/tmp/fleet-m5/stateful_failover}
INPUT=${INPUT:-/private/tmp/fleet-m5/stateful_input}
SHARED=$WORK/shared   # "shared storage": every node sees the same path
IDS=(1 2 3)
NODES=(n1 n2 n3)
JOBS=3
ADDR=(); PEERS=""
for i in "${IDS[@]}"; do
  ADDR[$i]=127.0.0.1:$((PORT + i - 1))
  PEERS+="${PEERS:+,}$i=${ADDR[$i]}"
done
ALL="${ADDR[1]},${ADDR[2]},${ADDR[3]}"
export ASAN_OPTIONS=${ASAN_OPTIONS:-detect_container_overflow=0}

for b in gen oracle streamkit; do
  if [[ ! -x "$SK/$b" ]]; then
    echo "== building adstack ($SK/$b missing)"
    cmake -S "$ADSTACK" -B "$ADSTACK/build" -G Ninja -DCMAKE_BUILD_TYPE=Release >/dev/null
    cmake --build "$ADSTACK/build" >/dev/null
    break
  fi
done

rm -rf "$WORK"
mkdir -p "$WORK" "$SHARED" "$INPUT"
: >"$WORK/kills"
: >"$WORK/moved"
declare -a RPID APID

ctl() { "$BIN/fleetctl" --master "$ALL" --timeout-ms 3000 "$@"; }
# ms of the day, the clock and format of fleet's logs ("HH:MM:SS.mmm").
now_ms() {
  perl -MTime::HiRes=time -e '$t=time; @l=localtime($t); printf "%d", (($l[2]*60+$l[1])*60+$l[0])*1000 + int(($t-int($t))*1000)'
}
# Unix microseconds, the clock of the agents' event logs.
unix_us() { perl -MTime::HiRes=time -e 'printf "%d", time*1e6'; }

cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  echo
  echo "== teardown"
  [[ -n "${PROBE_PID:-}" ]] && { kill "$PROBE_PID" 2>/dev/null || true; wait "$PROBE_PID" 2>/dev/null || true; }
  for i in 1 2 3; do
    if [[ -n "${APID[$i]:-}" ]]; then
      kill -TERM "${APID[$i]}" 2>/dev/null || true
      wait "${APID[$i]}" 2>/dev/null || true
    fi
  done
  for i in "${IDS[@]}"; do
    if [[ -n "${RPID[$i]:-}" ]]; then
      kill -TERM "${RPID[$i]}" 2>/dev/null || true
      wait "${RPID[$i]}" 2>/dev/null || true
    fi
  done
  sleep 0.2
  local left
  left=$(pgrep -f "$SHARED/" || true)
  if [[ -n "$left" ]]; then
    echo "LEFTOVER streamkit processes: $left"
    kill -KILL $left 2>/dev/null || true
    rc=1
  else
    echo "no task processes left behind"
  fi
  exit $rc
}
trap cleanup EXIT INT TERM

start_replica() {
  local i=$1
  "$BIN/fleetmaster" --id "$i" --peers "$PEERS" --state-dir "$WORK/r$i" --snapshot-every 200 \
    --heartbeat-ms 500 --suspect-after 3 --dead-after 6 --tick-ms 100 2>>"$WORK/r$i.log" &
  RPID[$i]=$!
}
start_agent() {  # node number; logs and event logs are appended across incarnations
  local i=$1 n=n$1
  "$BIN/fleetlet" --master "$ALL" --name "$n" --cpu 4000 --mem 4096 --work-dir "$WORK/$n" \
    --stop-grace-ms 1000 --event-log "$WORK/$n.events" 2>>"$WORK/$n.log" &
  APID[$i]=$!
}
# The node dies with its tasks. SIGSTOP first, so the fleetlet cannot fork
# another task between our listing its children and killing it; each task
# is the leader of its own process group (ProcessRuntime's setpgid), so the
# group kill takes anything it forked too.
kill_node() {
  local i=$1 p=${APID[$1]} kids k
  kill -STOP "$p"
  kids=$(pgrep -P "$p" || true)
  for k in $kids; do kill -KILL -- "-$k" 2>/dev/null || kill -KILL "$k" 2>/dev/null || true; done
  kill -KILL "$p"
  wait "$p" 2>/dev/null || true
  APID[$i]=""
  echo "$(unix_us) $(now_ms) n$i" >>"$WORK/kills"
}
serving_leader() { ctl status 2>/dev/null | awk '$3=="leader" && /serving/ {print $2; exit}'; }

echo "== machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), load: $(uptime | sed 's/.*load averages*: //')"
echo "== bin: $BIN   adstack: $SK"
# Stall probe. A laptop with its lid closed on battery sleeps in spite of
# any assertion and runs only in short DarkWake windows; while it sleeps
# every process here is frozen, the master's steady clock keeps counting,
# and on waking the master declares every node dead at once. That changes
# nothing about correctness (all checks below still apply) but it is not
# failover time. This probe wakes every 20 ms and records each pause of
# more than 500 ms ("start-ms end-ms", ms of the day), so the timing
# summary can set aside the samples a pause overlapped.
: >"$WORK/stalls"
perl -MTime::HiRes=time,sleep -e '
  $| = 1; open(my $f, ">>", $ARGV[0]) or die; select($f); $| = 1;
  sub dms { my $t = shift; my @l = localtime($t); (($l[2]*60+$l[1])*60+$l[0])*1000 + int(($t-int($t))*1000) }
  my $last = time;
  while (1) { sleep 0.02; my $t = time; printf "%d %d\n", dms($last), dms($t) if $t - $last > 0.5; $last = $t; }
' "$WORK/stalls" &
PROBE_PID=$!
if [[ ! -s "$INPUT/events-$SEED-$EVENTS.ads" || ! -s "$INPUT/truth-$SEED-$EVENTS.csv" ]]; then
  "$SK/gen" --n "$EVENTS" --seed "$SEED" --out "$INPUT/events-$SEED-$EVENTS.ads"
  "$SK/oracle" --in "$INPUT/events-$SEED-$EVENTS.ads" --out "$INPUT/truth-$SEED-$EVENTS.csv"
fi
cp "$INPUT/events-$SEED-$EVENTS.ads" "$SHARED/events.ads"
TRUTH=$INPUT/truth-$SEED-$EVENTS.csv
echo "== input: $EVENTS events, seed $SEED; oracle: $(($(wc -l <"$TRUTH") - 1)) windows"

for i in "${IDS[@]}"; do start_replica "$i"; done
for _ in $(seq 100); do [[ -n "$(serving_leader)" ]] && break; sleep 0.05; done
for i in 1 2 3; do start_agent "$i"; done
for _ in $(seq 100); do
  [[ $(ctl list 2>/dev/null | awk '$2=="ready"' | wc -l) -eq 3 ]] && break
  sleep 0.05
done
echo "== cluster: 3 replicas (leader $(serving_leader)), 3 nodes ready"

# Brings back every killed node whose return time has come; with "all",
# every killed node now.
declare -a BACK_AT
restart_due() {
  local i t
  t=$(now_ms)
  for i in 1 2 3; do
    if [[ -z "${APID[$i]:-}" && ( "${1:-}" == all || "${BACK_AT[$i]:-0}" -le "$t" ) ]]; then
      start_agent "$i"
    fi
  done
}
unfinished() {  # round
  ctl list 2>/dev/null | awk -v r="sk-r$1-" 'index($2, r) == 1 && $4 != "succeeded" &&
    $4 != "failed" && $4 != "killed"' | wc -l | tr -d ' '
}

FAILED=0
T_BEGIN=$(date +%s)
for round in $(seq "$ROUNDS"); do
  r0=$(date +%s)
  jobs=()
  for j in $(seq "$JOBS"); do
    job=sk-r$round-$j
    jobs+=("$job")
    ctl submit --job "$job" --priority 1 --cpu 2000 --mem 512 --restart never \
      --checkpoint-dir "$SHARED/ck-$job" --resume-arg --resume -- \
      "$SK/streamkit" --in "$SHARED/events.ads" --out "$SHARED/out-$job.csv" --lateness-s 60 \
      --threads 2 --ckpt-dir "$SHARED/ck-$job" --checkpoint-every-events 100000 \
      --throttle-eps "$EPS" >/dev/null
  done
  kills_before=$(wc -l <"$WORK/kills")
  next_kill=$(($(now_ms) + 3000 + RANDOM % 3001))
  next_check=0
  while :; do
    restart_due
    t=$(now_ms)
    if ((t >= next_check)); then
      (($(unfinished "$round") == 0)) && break
      next_check=$((t + 500))
      # Generous: a machine that sleeps (see the stall probe) can freeze a
      # round for minutes without anything being wrong.
      if (($(date +%s) - r0 > 900)); then
        echo "FAIL: round $round not finished after 900 s"; ctl list; exit 1
      fi
    fi
    if ((t < next_kill)); then
      sleep 0.05
      continue
    fi
    # One node down at a time: kill only once every node is back, so the
    # survivors always have room for all three jobs and the kill-to-start
    # times measure failover, not waiting for capacity.
    down=0
    for i in 1 2 3; do [[ -z "${APID[$i]:-}" ]] && down=1; done
    if ((down)); then
      sleep 0.05
      continue
    fi
    next_kill=$((t + 3000 + RANDOM % 3001))
    L=$(ctl list 2>/dev/null || true)
    # Nodes that are up and run at least one job of this round.
    up=()
    for v in $(awk -v r="sk-r$round-" 'index($2, r) == 1 && $4 == "running" {print $5}' <<<"$L" | sort -u); do
      [[ -n "${APID[${v#n}]:-}" ]] && up+=("$v")
    done
    ((${#up[@]} == 0)) && continue
    victim=${up[$((RANDOM % ${#up[@]}))]}
    vi=${victim#n}
    kill_node "$vi"
    t_kill=$(awk 'END {print $2}' "$WORK/kills")
    # The tasks it ran, by the list taken just before: "kill-ms node task attempt".
    awk -v r="sk-r$round-" -v n="$victim" -v t="$t_kill" \
      'index($2, r) == 1 && $4 == "running" && $5 == n {print t, n, $1, $6}' <<<"$L" >>"$WORK/moved"
    BACK_AT[$vi]=$((t_kill + 500 + RANDOM % 3501))
  done
  restart_due all
  for _ in $(seq 200); do
    [[ $(ctl list 2>/dev/null | awk '$2=="ready"' | wc -l) -eq 3 ]] && break
    sleep 0.05
  done
  for j in "${jobs[@]}"; do
    st=$(ctl list --job "$j" | awk -v j="$j" '$2==j {print $4, $6}')
    res=$("$ADSTACK/bench/compare.sh" "$TRUTH" "$SHARED/out-$j.csv" 2>&1 | head -1 || true)
    ck=$([[ -e "$SHARED/ck-$j" ]] && echo "ckpt dir LEFT" || echo "ckpt dir removed")
    echo "  round $round $j: ${st% *}, attempts ${st#* }, $res, $ck"
    [[ "$st" == succeeded* && "$res" == PASS* && "$ck" == "ckpt dir removed" ]] || FAILED=$((FAILED + 1))
    rm -f "$SHARED/out-$j.csv"  # 14 MB each; the verdict is recorded
  done
  echo "  round $round: $(($(wc -l <"$WORK/kills") - kills_before)) node kill(s), $(($(date +%s) - r0)) s, load $(uptime | sed 's/.*load averages*: //')"
done
T_TOTAL=$(($(date +%s) - T_BEGIN))

echo
echo "== summary over $ROUNDS rounds ($T_TOTAL s)"
KILLS=$(wc -l <"$WORK/kills" | tr -d ' ')
echo "  node kills: $KILLS"
cat "$WORK"/n?.log >"$WORK/agents.log"
cat "$WORK"/r?.log >"$WORK/masters.log"
fresh=$(grep -c 'started task .*\[stateful: fresh' "$WORK/agents.log" || true)
res_m=$(grep -c 'started task .*\[stateful: resume, MANIFEST present' "$WORK/agents.log" || true)
res_n=$(grep -c 'started task .*\[stateful: resume, no MANIFEST' "$WORK/agents.log" || true)
refused=$(grep -c 'refused:\|superseded' "$WORK/agents.log" || true)
# streamkit prints its checkpoint summary only when a run completes (output
# committed, checkpoint dir removed). More completions than jobs means a
# run finished but its node was killed before the agent reported the exit:
# the master rightly treats the attempt as lost, and the replacement finds
# no checkpoint dir, starts fresh and commits the same output again.
sk_done=$(cat "$WORK"/n?/sk-*.log 2>/dev/null | grep -c 'checkpoints: ' || true)
sk_resumed=$(cat "$WORK"/n?/sk-*.log 2>/dev/null | grep -c 'checkpoints: resumed from' || true)
echo "  attempts started: $((fresh + res_m + res_n)) ($fresh fresh, $res_m resumed from a MANIFEST, $res_n resumed over uncommitted leftovers)"
# A failover is any attempt after the first (every job runs with
# --restart never, so only a node kill makes a second one), whichever
# command line it ran: one whose node died before streamkit created its
# checkpoint directory correctly starts fresh again.
failovers=$(grep -cE 'started task [0-9]+ \([^ ]+\) attempt ([2-9]|[1-9][0-9]+) ' "$WORK/agents.log" || true)
echo "  failovers (attempts after the first: node kills, plus mass deaths after a machine pause): $failovers"
echo "  streamkit runs that completed: $sk_done for $((ROUNDS * JOBS)) jobs ($sk_resumed of them resumed from a checkpoint; $((sk_done - ROUNDS * JOBS)) completed but lost with their node before the exit was reported)"
echo "  starts refused or attempts stopped by the attempt fence: $refused (expected 0)"
# Death is declared after 6 x 500 ms = 3 s of silence; a much longer
# silence means the leader itself was not running (a machine-wide stall),
# which shows up as an outlier in the kill-to-replacement times below.
perl -ne '$m = $1 if /declared dead after \d+ missed heartbeats \((\d+) ms silent\)/ && $1 > $m;
  $n++ if /declared dead after/; END { printf "  nodes declared dead: %d, longest silence before it: %d ms (3,000 ms expected)\n", $n, $m }' \
  "$WORK/masters.log"

# Kill -> replacement attempt started, per (kill, task on the killed node),
# and how the master learned the node was gone (whichever came first after
# the kill: "declared dead", or the restarted agent's "re-registered").
perl -e '
  sub ms { my ($h,$m,$s,$f) = split /[:.]/, $_[0]; return (($h*60+$m)*60+$s)*1000+$f; }
  open A, "<", $ARGV[0] or die; my (@starts, %exited);  # %exited: "task/attempt" -> when it exited 0
  while (<A>) {
    push @starts, [ms($1), $2, $3, $4] if /^(\S+) fleetlet\[(\w+)\]: started task (\d+) \(\S+\) attempt (\d+)/;
    $exited{"$2/$3"} = ms($1) if /^(\S+) fleetlet\[\w+\]: task (\d+) attempt (\d+) exited: code 0/;
  }
  open M, "<", $ARGV[1] or die; my @node_ev;
  while (<M>) {
    if (/^(\S+) fleetmaster\S*: node (\w+) declared dead/) { push @node_ev, [ms($1), $2, "dead"]; }
    elsif (/^(\S+) fleetmaster\S*: node (\w+) re-registered/) { push @node_ev, [ms($1), $2, "back"]; }
  }
  open S, "<", $ARGV[3] or die; my @stalls = map { [split] } <S>;
  my ($n_stall, $stall_ms) = (0, 0);
  $n_stall++, $stall_ms += $_->[1] - $_->[0] for @stalls;
  printf "  machine pauses over 500 ms seen by the stall probe: %d, %.1f s in total\n", $n_stall, $stall_ms / 1000;
  open V, "<", $ARGV[2] or die; my (%d, $missing, $done_before);
  while (<V>) {
    my ($t, $n, $task, $att) = split;
    my ($first) = sort { $a->[0] <=> $b->[0] } grep { $_->[0] >= $t && $_->[2] == $task && $_->[3] > $att } @starts;
    my ($how) = sort { $a->[0] <=> $b->[0] } grep { $_->[0] >= $t && $_->[1] eq $n } @node_ev;
    # The kill picks its victims from a task list taken just before it; a
    # task can finish in between, and then it needs no replacement.
    if (!$first && defined $exited{"$task/$att"} && $exited{"$task/$att"} <= $t) { $done_before++; next; }
    if (!$first) { $missing++; next; }
    # A sample a machine pause overlapped measures the pause, not
    # failover: kept apart, so the clean numbers are what fleet does.
    if (grep { $_->[0] < $first->[0] && $_->[1] > $t } @stalls) {
      push @{$d{"overlapped by a machine pause, excluded above"}}, $first->[0] - $t; next;
    }
    my $k = $how ? ($how->[2] eq "dead" ? "declared dead first" : "back (re-registered) before declared dead") : "unknown";
    push @{$d{$k}}, $first->[0] - $t; push @{$d{"all"}}, $first->[0] - $t;
  }
  for my $k ("all", sort grep { $_ ne "all" } keys %d) {
    my @v = sort { $a <=> $b } @{$d{$k}}; next unless @v;
    printf "  kill -> replacement started (%s): n=%d min %d ms, median %d ms, max %d ms\n",
      $k eq "all" ? "all samples no pause overlapped" : $k, scalar @v, $v[0], $v[int($#v/2)], $v[-1];
  }
  print "  (kill, task) pairs where the task finished just before the kill (no replacement needed): $done_before\n" if $done_before;
  print "  MISSING replacement for $missing (kill, task) pair(s)\n" if $missing;
' "$WORK/agents.log" "$WORK/masters.log" "$WORK/moved" "$WORK/stalls"

# No two attempts of a task overlapped. An attempt without an exit event
# ended when its node was killed (the first kill of that node after its
# start); one with neither is still running, which no attempt should be.
OVERLAPS=$(perl -e '
  open K, "<", shift or die; my @kills; while (<K>) { my ($us, $ms, $n) = split; push @kills, [$us, $n]; }
  my (%start, %end, %node);
  for my $f (@ARGV) { open E, "<", $f or die;
    while (<E>) { my ($us, $what, $task, $att, $ep, $n) = split;
      my $key = "$task/$att/$ep"; $node{$key} = $n;
      if ($what eq "start") { $start{$key} = $us } else { $end{$key} = $us } } }
  my (%by_task, $bad, $open);
  for my $key (keys %start) {
    my $e = $end{$key};
    if (!defined $e) {
      my ($k) = sort { $a->[0] <=> $b->[0] } grep { $_->[1] eq $node{$key} && $_->[0] >= $start{$key} } @kills;
      if ($k) { $e = $k->[0] } else { $open++; $e = 9e18 }
    }
    my ($task) = split m{/}, $key; push @{$by_task{$task}}, [$start{$key}, $e, $key];
  }
  for my $t (keys %by_task) {
    my @iv = sort { $a->[0] <=> $b->[0] } @{$by_task{$t}};
    for my $i (1 .. $#iv) {
      if ($iv[$i][0] < $iv[$i-1][1]) { $bad++; print STDERR "  OVERLAP task $t: $iv[$i-1][2] until $iv[$i-1][1], $iv[$i][2] from $iv[$i][0]\n"; }
    }
  }
  print STDERR "  attempts with neither exit nor kill: $open\n" if $open;
  print(($bad // 0) + ($open // 0));
' "$WORK/kills" "$WORK"/n?.events)
echo "  attempt intervals checked from the event logs: $(cat "$WORK"/n?.events | grep -c ' start ') attempts, $OVERLAPS overlapping or unaccounted"
echo "  jobs that did not PASS: $FAILED of $((ROUNDS * JOBS))"
echo "  load at end: $(uptime | sed 's/.*load averages*: //')"
if ((FAILED > 0 || OVERLAPS > 0 || refused > 0)); then
  echo "RESULT: FAIL"
  exit 1
fi
echo "RESULT: PASS ($((ROUNDS * JOBS)) jobs identical to the oracle across $KILLS node kills)"
