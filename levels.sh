#!/usr/bin/env bash
set -uo pipefail

: "${SERVER_CMD:?SERVER_CMD is required}"
LOADGEN_CPUS=${LOADGEN_CPUS:-2-3}
NIM_RINGS=${NIM_RINGS:-4}
SEED=seed/feed.db
DB=bench-db/feed.db
mkdir -p bench-db

cleanup() {
  docker rm -f bench >/dev/null 2>&1 || true
}
trap cleanup EXIT

run_level() {
  local n=$1
  local log=/tmp/level.log
  cleanup
  rm -f "$DB" "$DB-wal" "$DB-shm"
  cp "$SEED" "$DB"
  echo "LEVEL_START n=$n $(date -u +%H:%M:%S)"

  docker run -d --name bench --network host \
    --cpuset-cpus=0 --cpus=1 --memory=2g --memory-swap=2g \
    --ulimit nofile=65535:65535 --security-opt seccomp=unconfined \
    -v "$PWD":/work -w /work \
    -e SQLITE_PATH=/work/bench-db/feed.db \
    -e JWT_SECRET=twelve-dollar-challenge \
    -e HOST=127.0.0.1 -e PORT=3000 \
    -e IOURING_BUFS=256 -e ZEROCOPY_STATS=0 \
    -e NIM_RINGS="$NIM_RINGS" \
    ubuntu:24.04 bash -lc "apt-get update -qq && (apt-get install -y -qq libssl3t64 >/dev/null 2>&1 || apt-get install -y -qq libssl3 >/dev/null 2>&1 || true); exec $SERVER_CMD" >/dev/null

  local up=0
  for _ in $(seq 150); do
    if curl -sf http://127.0.0.1:3000/health >/dev/null 2>&1; then up=1; break; fi
    if ! docker inspect -f '{{.State.Running}}' bench 2>/dev/null | grep -q true; then break; fi
    sleep 1
  done
  if [ "$up" -ne 1 ]; then
    echo "LEVEL_FAIL n=$n reason=startup"
    docker logs bench 2>&1 | tail -40 || true
    cleanup
    return 1
  fi

  local cpu_start mem_start sock_start start_ns
  cpu_start=$(docker exec bench cat /sys/fs/cgroup/cpu.stat 2>/dev/null || true)
  mem_start=$(docker exec bench grep -E 'VmRSS|VmHWM' /proc/1/status 2>/dev/null || true)
  sock_start=$(docker exec bench cat /proc/net/sockstat 2>/dev/null || true)
  start_ns=$(date +%s%N)

  ulimit -n 1000000 2>/dev/null || ulimit -n "$(ulimit -Hn)" 2>/dev/null || true
  timeout 420 sudo prlimit --nofile=1000000:1000000 taskset -c "$LOADGEN_CPUS" \
    /tmp/loadgen -H 127.0.0.1 -p 3000 \
    -c "$n" -d 360 -t 9000 -m mixed -T "$PWD/seed/tokens.json" >"$log" 2>&1
  local rc=$?

  local end_ns cpu_end mem_end sock_end
  end_ns=$(date +%s%N)
  cpu_end=$(docker exec bench cat /sys/fs/cgroup/cpu.stat 2>/dev/null || true)
  mem_end=$(docker exec bench grep -E 'VmRSS|VmHWM' /proc/1/status 2>/dev/null || true)
  sock_end=$(docker exec bench cat /proc/net/sockstat 2>/dev/null || true)

  local connected completed errors rps p50 p95 p99
  connected=$(awk '/^connections /{for(i=1;i<=NF;i++) if($i ~ /^connected=/){sub("connected=","",$i); print $i}}' "$log" | tail -1)
  completed=$(awk '/^requests /{for(i=1;i<=NF;i++) if($i ~ /^completed=/){sub("completed=","",$i); print $i}}' "$log" | tail -1)
  errors=$(awk '/^errors /{for(i=1;i<=NF;i++) if($i ~ /^total=/){sub("total=","",$i); print $i}}' "$log" | tail -1)
  rps=$(awk '/^rps /{print $2}' "$log" | tail -1)
  p50=$(grep -o 'p50=[0-9.]*' "$log" | tail -1 | cut -d= -f2)
  p95=$(grep -o 'p95=[0-9.]*' "$log" | tail -1 | cut -d= -f2)
  p99=$(grep -o 'p99=[0-9.]*' "$log" | tail -1 | cut -d= -f2)
  connected=${connected:-0}
  completed=${completed:-0}
  errors=${errors:-0}
  rps=${rps:-0}
  p50=${p50:-999999}
  p95=${p95:-999999}
  p99=${p99:-999999}

  local usage0 user0 sys0 usage1 user1 sys1 elapsed_us
  usage0=$(printf '%s\n' "$cpu_start" | awk '/usage_usec/{print $2; exit}')
  user0=$(printf '%s\n' "$cpu_start" | awk '/user_usec/{print $2; exit}')
  sys0=$(printf '%s\n' "$cpu_start" | awk '/system_usec/{print $2; exit}')
  usage1=$(printf '%s\n' "$cpu_end" | awk '/usage_usec/{print $2; exit}')
  user1=$(printf '%s\n' "$cpu_end" | awk '/user_usec/{print $2; exit}')
  sys1=$(printf '%s\n' "$cpu_end" | awk '/system_usec/{print $2; exit}')
  usage0=${usage0:-0}; user0=${user0:-0}; sys0=${sys0:-0}
  usage1=${usage1:-0}; user1=${user1:-0}; sys1=${sys1:-0}
  elapsed_us=$(( (end_ns - start_ns) / 1000 ))
  [ "$elapsed_us" -gt 0 ] || elapsed_us=1

  local cpu_pct user_pct sys_pct error_rate_pct
  cpu_pct=$(awk -v a="$usage0" -v b="$usage1" -v t="$elapsed_us" 'BEGIN{printf "%.1f", (b-a)*100.0/t}')
  user_pct=$(awk -v a="$user0" -v b="$user1" -v t="$elapsed_us" 'BEGIN{printf "%.1f", (b-a)*100.0/t}')
  sys_pct=$(awk -v a="$sys0" -v b="$sys1" -v t="$elapsed_us" 'BEGIN{printf "%.1f", (b-a)*100.0/t}')
  error_rate_pct=$(awk -v e="$errors" -v c="$completed" 'BEGIN{printf "%.4f", (c > 0 ? e*100.0/c : 100.0)}')

  local rss_mib hwm_mib mem_current_mib mem_peak_mib tcp_inuse
  rss_mib=$(printf '%s\n' "$mem_end" | awk '/VmRSS:/{printf "%.2f", $2/1024; exit}')
  hwm_mib=$(printf '%s\n' "$mem_end" | awk '/VmHWM:/{printf "%.2f", $2/1024; exit}')
  mem_current_mib=$(docker exec bench sh -c 'cat /sys/fs/cgroup/memory.current 2>/dev/null' 2>/dev/null | awk '{printf "%.2f", $1/1048576}')
  mem_peak_mib=$(docker exec bench sh -c 'cat /sys/fs/cgroup/memory.peak 2>/dev/null' 2>/dev/null | awk '{printf "%.2f", $1/1048576}')
  tcp_inuse=$(printf '%s\n' "$sock_end" | awk '/^TCP:/{for(i=1;i<=NF;i++) if($i=="inuse") print $(i+1); exit}')
  rss_mib=${rss_mib:-0}; hwm_mib=${hwm_mib:-0}; mem_current_mib=${mem_current_mib:-0}; mem_peak_mib=${mem_peak_mib:-0}; tcp_inuse=${tcp_inuse:-0}
  cpu_pct=${cpu_pct:-0}; user_pct=${user_pct:-0}; sys_pct=${sys_pct:-0}; error_rate_pct=${error_rate_pct:-100}

  local pass
  pass=1
  [ "$rc" -eq 0 ] || pass=0
  [ "$connected" -ge "$n" ] || pass=0
  [ "$completed" -gt 0 ] || pass=0
  # At 9s think, 360s is ~40 requests per connected VU. Require at least 10
  # completed requests per requested VU so a server cannot "pass" by accepting
  # connections without actually serving them.
  [ "$completed" -ge $((n * 10)) ] || pass=0
  awk -v r="$error_rate_pct" 'BEGIN{exit !(r < 1.0)}' || pass=0
  awk -v v="$p95" 'BEGIN{exit !(v < 500)}' || pass=0
  awk -v v="$p99" 'BEGIN{exit !(v < 1000)}' || pass=0

  echo "DOCKER_STATE $(docker inspect -f '{{.State.OOMKilled}} {{.State.ExitCode}} {{.State.Error}}' bench 2>&1 || true)"
  echo "LEVEL_RESULT n=$n rc=$rc connected=$connected completed=$completed errors=$errors error_rate_pct=$error_rate_pct rps=$rps p50=$p50 p95=$p95 p99=$p99 cpu_total_pct=$cpu_pct cpu_app_user_pct=$user_pct sys_cpu_pct=$sys_pct rss_mib=$rss_mib hwm_mib=$hwm_mib mem_current_mib=$mem_current_mib mem_peak_mib=$mem_peak_mib tcp_inuse=$tcp_inuse pass=$pass"
  tail -12 "$log" || true
  if [ "$pass" -ne 1 ]; then
    echo "LEVEL_SERVER_LOG n=$n"
    docker logs bench 2>&1 | tail -60 || true
  fi
  cleanup
  [ "$pass" -eq 1 ]
}

for n in ${LEVELS:-262000 300000 327675}; do
  run_level "$n" || true
done
