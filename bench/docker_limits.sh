#!/usr/bin/env bash
# docker_limits.sh: does DockerRuntime really enforce limits? (m2b)
#
# Runs fleetmaster plus one fleetlet with --runtime=docker and checks the
# three things a container runtime is for, through fleet itself:
#   1. Memory: a task that allocates past its --mem is OOM-killed by the
#      cgroup and fleetctl reports it as "oom"; a control task that stays
#      under the same kind of limit succeeds.
#   2. CPU: a single-threaded spin loop submitted with --cpu 500 is held to
#      about half a core. Measured twice over the same window: from the
#      container's own cgroup counters (cpu.stat usage_usec and the CFS
#      throttling counters) and with `docker stats`. A control with
#      --cpu 2000 shows the same loop using a full core, so the 50% is the
#      limit and not the loop.
#   3. Start latency: `docker run -d` with DockerRuntime's exact flags, 10
#      times, median / min / max; plus the cost of one `docker inspect`,
#      which DockerRuntime pays every poll interval.
# Then tears everything down and fails if any fleet- container is left.
#
# Requires Docker (Docker Desktop or OrbStack on macOS). Without it the
# script prints SKIP and exits 0, so it is safe to run anywhere.
#
# Usage: bench/docker_limits.sh                (uses build/bin)
#        IMAGE=alpine:3.20 bench/docker_limits.sh
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIN=${BIN:-$ROOT/build/bin}
PORT=${PORT:-17171}
IMAGE=${IMAGE:-busybox:1.36}
WORK=/private/tmp/fleet/docker_limits
ADDR=127.0.0.1:$PORT
CPU_WINDOW_S=${CPU_WINDOW_S:-10}
export ASAN_OPTIONS=${ASAN_OPTIONS:-detect_container_overflow=0}

ms() { perl -MTime::HiRes=time -e 'printf "%d", time()*1000'; }
median() { sort -n | awk '{a[NR]=$1} END {print a[int((NR+1)/2)]}'; }

# --- is Docker there at all? macOS has no timeout(1), so the 5 s deadline
# on `docker version` is a background job plus a watchdog loop.
docker_ok() {
  command -v docker >/dev/null 2>&1 || { echo "docker CLI not on PATH"; return 1; }
  docker version --format '{{.Server.Version}}' >/private/tmp/fleet/dl_version 2>&1 &
  local p=$!
  for _ in $(seq 50); do kill -0 "$p" 2>/dev/null || break; sleep 0.1; done
  if kill -0 "$p" 2>/dev/null; then
    kill "$p" 2>/dev/null || true
    echo "docker version timed out (is the Docker VM running?)"
    return 1
  fi
  wait "$p" || { cat /private/tmp/fleet/dl_version; return 1; }
}
mkdir -p /private/tmp/fleet
if ! why=$(docker_ok); then
  echo "SKIP: Docker is not available ($why)."
  echo "Install Docker Desktop or OrbStack, start it, and rerun bench/docker_limits.sh."
  exit 0
fi

rm -rf "$WORK"
mkdir -p "$WORK"
PIDS=()
ctl() { "$BIN/fleetctl" --master "$ADDR" --timeout-ms 3000 "$@"; }

cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  echo
  echo "== teardown"
  # The agent first: on SIGTERM its DockerRuntime runs docker rm -f on
  # every container it started. Then the master.
  for ((i = ${#PIDS[@]} - 1; i >= 0; i--)); do
    kill -TERM "${PIDS[$i]}" 2>/dev/null || true
    wait "${PIDS[$i]}" 2>/dev/null || true
  done
  docker rm -f $(docker ps -aq --filter name=fleet-bench-) >/dev/null 2>&1 || true
  local left
  left=$(docker ps -a --filter name=^/fleet- --format '{{.Names}}' || true)
  if [[ -n "$left" ]]; then
    echo "LEFTOVER containers: $left"
    docker rm -f $left >/dev/null 2>&1 || true
    rc=1
  else
    echo "no fleet containers left behind"
  fi
  exit $rc
}
trap cleanup EXIT INT TERM

echo "== machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), load: $(uptime | sed 's/.*load averages*: //')"
echo "== docker: server $(cat /private/tmp/fleet/dl_version), $(docker info --format '{{.OperatingSystem}}, kernel {{.KernelVersion}}, {{.NCPU}} CPUs, {{.MemTotal}} bytes, cgroup v{{.CgroupVersion}}' 2>/dev/null)"
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  echo "== pulling $IMAGE"
  docker pull "$IMAGE"
fi

"$BIN/fleetmaster" --listen "$ADDR" 2>"$WORK/master.log" &
PIDS+=($!)
for _ in $(seq 50); do ctl list >/dev/null 2>&1 && break; sleep 0.1; done
"$BIN/fleetlet" --master "$ADDR" --name d1 --cpu 4000 --mem 4096 --runtime=docker \
  --docker-image "$IMAGE" --work-dir "$WORK/d1" --stop-grace-ms 1000 2>"$WORK/d1.log" &
PIDS+=($!)
for _ in $(seq 100); do
  [[ $(ctl list | awk '$2=="ready"' | wc -l) -eq 1 ]] && break
  sleep 0.1
done
grep -m1 "runtime docker" "$WORK/d1.log" || { echo "fleetlet did not start with docker"; cat "$WORK/d1.log"; exit 1; }

# Task id from "submitted job J: 3".
submit_id() { ctl submit "$@" | awk '{print $NF}'; }
task_line() { ctl list | awk -v id="$1" '$1==id'; }
# Polls until the task reaches a terminal state; prints progress every 5 s.
wait_terminal() {
  local id=$1
  for i in $(seq 600); do
    local st
    st=$(task_line "$id" | awk '{print $4}')
    case "$st" in succeeded|failed|killed) return 0 ;; esac
    (( i % 50 == 0 )) && echo "   ... task $id still $st"
    sleep 0.1
  done
  echo "task $id did not finish"
  return 1
}
# The container fleet started for a task (name fleet-<task>-<attempt>-<inc>).
container_of() {
  local id=$1
  for _ in $(seq 100); do
    local n
    n=$(docker ps --filter "name=^/fleet-$id-" --filter status=running --format '{{.Names}}' | head -1)
    [[ -n "$n" ]] && { echo "$n"; return 0; }
    sleep 0.1
  done
  return 1
}

FAIL=0

echo
echo "== 1. memory: 256 MB through a 32 MB limit, and a 16 MB control under 64 MB"
# Docker's own event stream, to split "how long until the kernel killed
# it" from "how long until fleet noticed".
docker events --filter type=container --format '{{.TimeNano}} {{.Action}} {{.Actor.Attributes.name}}' \
  >"$WORK/events.log" 2>/dev/null &
EVENTS_PID=$!
PIDS+=($EVENTS_PID)
sleep 0.3
T0=$(ms)
OOM_ID=$(submit_id --job oom --cpu 500 --mem 32 --image "$IMAGE" -- \
  /bin/sh -c 'echo allocating; head -c 256m /dev/zero | tail')
OK_ID=$(submit_id --job fits --cpu 500 --mem 64 -- \
  /bin/sh -c 'head -c 16m /dev/zero | tail >/dev/null; echo stayed under the limit')
wait_terminal "$OOM_ID"
T1=$(ms)
wait_terminal "$OK_ID"
ctl list | awk '/^ID/ || $2=="oom" || $2=="fits"'
oom_exit=$(task_line "$OOM_ID" | awk '{print $7}')
ok_exit=$(task_line "$OK_ID" | awk '{print $7}')
echo "   master log: $(grep -m1 "task $OOM_ID (oom) failed" "$WORK/master.log")"
# Event times (epoch ns) as local ms-of-day, to line up with the HH:MM:SS.mmm
# stamps in the fleet logs.
ev_ms() {  # action -> ms of day of the first such event for the OOM task's container
  awk -v a="$1" -v p="fleet-$OOM_ID-" '$2==a && index($3, p)==1 {print $1; exit}' "$WORK/events.log" |
    perl -ne 'chomp; my $ns = $_; my @t = localtime(int($ns / 1e9));
              printf "%d", (($t[2]*60 + $t[1])*60 + $t[0])*1000 + int(($ns % 1e9) / 1e6)'
}
log_ms() {  # "HH:MM:SS.mmm" at the start of the first line matching $2 in file $1
  grep -m1 "$2" "$1" | perl -ne '/^(\d+):(\d+):(\d+)\.(\d+)/ and printf "%d", (($1*60 + $2)*60 + $3)*1000 + $4'
}
placed=$(log_ms "$WORK/master.log" "task $OOM_ID (oom) placed")
st=$(ev_ms start); oomev=$(ev_ms oom); die=$(ev_ms die)
seen=$(log_ms "$WORK/d1.log" "task $OOM_ID attempt 1 OOM-killed")
rep_ms=$(log_ms "$WORK/master.log" "task $OOM_ID (oom) failed")
if [[ -n "$placed" && -n "$st" && -n "$die" && -n "$seen" && -n "$rep_ms" ]]; then
  echo "   timeline (ms after placement): container start +$((st - placed)), kernel oom event +$((oomev - placed)), die +$((die - placed)), fleetlet saw it +$((seen - placed)) (die to fleetlet $((seen - die)) ms), master recorded +$((rep_ms - placed))"
fi
echo "   submit to OOM visible in fleetctl: $((T1 - T0)) ms"
# How much of that is the kernel: the same command in a bare container,
# StartedAt to FinishedAt as Docker records them (same-day timestamps).
docker run -d --name fleet-bench-oom --init --cpus=0.5 --memory=32m --memory-swap=32m \
  "$IMAGE" /bin/sh -c 'head -c 256m /dev/zero | tail' >/dev/null
docker wait fleet-bench-oom >/dev/null
read -r started finished oomk < <(docker inspect --format \
  '{{.State.StartedAt}} {{.State.FinishedAt}} {{.State.OOMKilled}}' fleet-bench-oom)
docker rm fleet-bench-oom >/dev/null
run_ms=$(perl -e 'sub t { $_[0] =~ /T(\d+):(\d+):(\d+)(\.\d+)?/; $1*3600 + $2*60 + $3 + ($4 || 0) }
                  printf "%d", (t($ARGV[1]) - t($ARGV[0])) * 1000' "$started" "$finished")
echo "   bare container: ran ${run_ms} ms from start to OOM kill (OOMKilled=$oomk)"
if [[ "$oom_exit" == "oom" && "$ok_exit" == "0" ]]; then
  echo "   PASS: over-limit task reported as oom, control exited 0"
else
  echo "   FAIL: expected oom / 0, got $oom_exit / $ok_exit"
  FAIL=1
fi

echo
echo "== 2. cpu: one spinning thread, --cpu 500 vs a --cpu 2000 control, ${CPU_WINDOW_S} s window"
cpu_stat() { docker exec "$1" cat /sys/fs/cgroup/cpu.stat | awk -v k="$2" '$1==k {print $2}'; }
measure_cpu() {  # job milli
  local job=$1 milli=$2
  local id name u0 u1 p0 p1 th0 th1 t0 t1
  id=$(submit_id --job "$job" --cpu "$milli" --mem 64 -- /bin/sh -c 'while :; do :; done')
  name=$(container_of "$id") || { echo "   no container for task $id"; FAIL=1; return; }
  echo "   task $id -> container $name, cgroup cpu.max: $(docker exec "$name" cat /sys/fs/cgroup/cpu.max)"
  sleep 1  # past the start-up transient
  u0=$(cpu_stat "$name" usage_usec); p0=$(cpu_stat "$name" nr_periods); th0=$(cpu_stat "$name" nr_throttled)
  t0=$(ms)
  local stats=()
  # docker stats --no-stream samples for about 2 s per call, so a few
  # samples fill most of the window; the cgroup counters cover all of it.
  local end=$((t0 + CPU_WINDOW_S * 1000 - 2500))
  while [[ $(ms) -lt $end ]]; do
    stats+=("$(docker stats --no-stream --format '{{.CPUPerc}}' "$name" | tr -d '%')")
    echo "   ... docker stats sample: ${stats[${#stats[@]}-1]}%"
  done
  while [[ $(ms) -lt $((t0 + CPU_WINDOW_S * 1000)) ]]; do sleep 0.1; done
  u1=$(cpu_stat "$name" usage_usec); p1=$(cpu_stat "$name" nr_periods); th1=$(cpu_stat "$name" nr_throttled)
  t1=$(ms)
  local pct med
  pct=$(awk -v du=$((u1 - u0)) -v dt=$((t1 - t0)) 'BEGIN {printf "%.1f", du / (dt * 1000) * 100}')
  med=$(printf '%s\n' "${stats[@]}" | median)
  echo "   --cpu $milli: cgroup usage ${pct}% of one core over $((t1 - t0)) ms;" \
       "docker stats median ${med}% (${#stats[@]} samples: ${stats[*]});" \
       "throttled in $((th1 - th0)) of $((p1 - p0)) CFS periods"
  ctl kill --job "$job" >/dev/null
  for _ in $(seq 100); do
    [[ -z $(docker ps -aq --filter "name=^/$name\$") ]] && break
    sleep 0.1
  done
  echo "$pct" >"$WORK/cpu_$milli"
}
measure_cpu hog 500
measure_cpu ctl2000 2000
half=$(cat "$WORK/cpu_500" 2>/dev/null || echo 0)
full=$(cat "$WORK/cpu_2000" 2>/dev/null || echo 0)
if awk -v h="$half" -v f="$full" 'BEGIN {exit !(h >= 40 && h <= 60 && f >= 85)}'; then
  echo "   PASS: --cpu 500 held to ${half}% (expected ~50%), control ${full}%"
else
  echo "   FAIL: --cpu 500 at ${half}%, control ${full}%"
  FAIL=1
fi

echo
echo "== 3. latency: docker run -d with DockerRuntime's flags, 10 times; docker inspect, 10 times"
RUNS=()
INSPECTS=()
for i in $(seq 10); do
  a=$(ms)
  docker run -d --name "fleet-bench-lat-$i" --init --cpus=0.5 --memory=64m --memory-swap=64m \
    "$IMAGE" sleep 30 >/dev/null
  b=$(ms)
  docker inspect --format '{{.Name}} {{.State.Status}} {{.State.ExitCode}} {{.State.OOMKilled}}' \
    "fleet-bench-lat-$i" >/dev/null
  c=$(ms)
  RUNS+=($((b - a)))
  INSPECTS+=($((c - b)))
  docker rm -f "fleet-bench-lat-$i" >/dev/null
done
echo "   docker run -d (ms): ${RUNS[*]}"
echo "   start latency median $(printf '%s\n' "${RUNS[@]}" | median) ms, min $(printf '%s\n' "${RUNS[@]}" | sort -n | head -1), max $(printf '%s\n' "${RUNS[@]}" | sort -n | tail -1)"
echo "   docker inspect (ms): ${INSPECTS[*]}"
echo "   inspect median $(printf '%s\n' "${INSPECTS[@]}" | median) ms"

echo
echo "== load after: $(uptime | sed 's/.*load averages*: //')"
if [[ $FAIL -ne 0 ]]; then
  echo "RESULT: FAIL"
  exit 1
fi
echo "RESULT: PASS"
