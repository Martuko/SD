#!/usr/bin/env bash
set -euo pipefail

# =======================
# Config global por defecto
# =======================
RESULTS_DIR=experiments_results/Tarea2
POOLS_DIR=data/pools/Tarea2
SRC_CSV=data/test.csv
POOL_TARGET=30000
SAMPLES_TARGET=10000

# Tráfico y router
CONCURRENCY=2
RATE=5
ROUTER_SCORE_THRESHOLD=0.6

# Para asegurar >= 10k: enviamos extra y paramos sólo cuando BD alcance la meta
GEN_FACTOR=3                      # 3x por seguridad
LENIENT_THRESHOLD=0.0             # umbral al activar modo lenient (acepta casi todo)
LENIENT_STALLS=12                 # ~6 min sin progreso para activar lenient (12*30s)
STALL_MAX=40                      # ~20 min sin progreso total -> aborta experimento

# Tópicos
TOPICS_IN=("questions.answers" "answers.scored")
TOPICS_ALL=("questions.answers" "answers.scored" "questions.requests" "questions.errors" "storage.persist" "cache.update")

# =======================
# Definición de Experimentos
# =======================
declare -A EXP1=( ["name"]="E1" ["distribution"]="poisson" ["policy"]="allkeys-lru" ["max_entries"]="5000" ["repeat_pool"]="0"    ["rate"]="5" ["concurrency"]="2" ["threshold"]="0.6" )
declare -A EXP2=( ["name"]="E2" ["distribution"]="poisson" ["policy"]="allkeys-lfu" ["max_entries"]="5000" ["repeat_pool"]="0"    ["rate"]="5" ["concurrency"]="2" ["threshold"]="0.6" )
declare -A EXP3=( ["name"]="E3" ["distribution"]="uniform"  ["policy"]="allkeys-lru" ["max_entries"]="5000" ["repeat_pool"]="0"    ["rate"]="5" ["concurrency"]="2" ["threshold"]="0.6" )
declare -A EXP4=( ["name"]="E4" ["distribution"]="uniform"  ["policy"]="allkeys-lfu" ["max_entries"]="5000" ["repeat_pool"]="0"    ["rate"]="5" ["concurrency"]="2" ["threshold"]="0.6" )
declare -A EXP5=( ["name"]="E5" ["distribution"]="poisson"  ["policy"]="allkeys-lru" ["max_entries"]="2000" ["repeat_pool"]="0"    ["rate"]="5" ["concurrency"]="2" ["threshold"]="0.6" )
declare -A EXP6=( ["name"]="E6" ["distribution"]="poisson"  ["policy"]="allkeys-lru" ["max_entries"]="5000" ["repeat_pool"]="2000" ["rate"]="6" ["concurrency"]="2" ["threshold"]="0.6" )

EXPERIMENTS=( EXP1 EXP2 EXP3 EXP4 EXP5 EXP6 )

# =======================
# Utilidades
# =======================
log() { echo "[$(date -Iseconds)] $*"; }

wait_http() {
  local url="$1" ; local tries="${2:-60}" ; local sleep_s="${3:-2}"
  for ((i=1;i<=tries;i++)); do
    if curl -sf "$url" >/dev/null 2>&1; then return 0; fi
    sleep "$sleep_s"
  done
  echo "Timeout esperando $url" >&2
  exit 1
}

wait_services() {
  log "Esperando servicios /health ..."
  wait_http "http://localhost:8000/health"   # LLM
  wait_http "http://localhost:8001/health"   # Cache
  wait_http "http://localhost:8002/health"   # Score
  wait_http "http://localhost:8003/health"   # Storage
  wait_http "http://localhost:8010/health"   # Traffic
  docker compose exec -T kafka bash -lc "kafka-topics --bootstrap-server localhost:9092 --list" >/dev/null
  log "Servicios OK."
}

ensure_dirs() { mkdir -p "$RESULTS_DIR" "$POOLS_DIR"; }

make_pool() {
  local exp_name="$1" ; local dst="${POOLS_DIR}/${exp_name}.csv"
  local k="${2:-$POOL_TARGET}" ; local seed="${3:-42}"
  if [[ -s "$dst" ]]; then log "Pool existente para ${exp_name}: $dst"; return 0; fi
  log "Generando pool ${k} para ${exp_name} → $dst"
  python3 - "$SRC_CSV" "$dst" "$k" "$seed" <<'PY'
import csv, random, sys
src, dst, k, seed = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
rng = random.Random(seed)
rows = []
with open(src, newline='', encoding='utf-8') as f:
    rdr = csv.reader(f, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
    for row in rdr:
        if len(row) >= 4:
            rows.append(row)
if k < len(rows):
    rows = rng.sample(rows, k)
with open(dst, "w", newline='', encoding='utf-8') as f:
    wr = csv.writer(f, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL, escapechar='\\')
    wr.writerows(rows)
PY
}

# ---------- Snapshots Kafka ----------
snapshot_kafka_offsets() {
  local EXP_DIR="$1" ; local phase="$2"
  local out="${EXP_DIR}/kafka_offsets_${phase}.txt"
  docker compose exec -T kafka bash -lc '
for t in '"${TOPICS_ALL[*]}"'; do
  echo "### $t"
  kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server localhost:9092 --topic $t --time -1 || true
done
' > "$out" || true
}

errors_breakdown() {
  local EXP_DIR="$1" ; local phase="$2"
  local csv="${EXP_DIR}/errors_breakdown_${phase}.csv"
  local tailf="${EXP_DIR}/errors_tail_${phase}.json"
  docker compose exec -T kafka bash -lc '
TMP=$(mktemp)
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic questions.errors --from-beginning --timeout-ms 5000 > "$TMP" || true
{
  echo "reason,count"
  grep -o "\"reason\":\"[^\"]*\"" "$TMP" | sed -E "s/\"reason\":\"([^\"]*)\"/\1/" | sort | uniq -c | awk "{print \$2\",\" \$1}"
} > /tmp/errors_breakdown.csv || true
tail -n 200 "$TMP" > /tmp/errors_tail.json || true
rm -f "$TMP"
' || true
  docker compose cp kafka:/tmp/errors_breakdown.csv "$csv" >/dev/null 2>&1 || true
  docker compose cp kafka:/tmp/errors_tail.json "$tailf" >/dev/null 2>&1 || true
  docker compose exec -T kafka bash -lc 'rm -f /tmp/errors_breakdown.csv /tmp/errors_tail.json' || true
}

# =======================
# Snapshots
# =======================
snapshot_before() {
  local name="$1" ; local dist="$2" ; local policy="$3" ; local max_entries="$4" ; local EXP_DIR="$5" ; local repeat_pool="$6" ; local rate="$7" ; local concurrency="$8" ; local threshold="$9"
  log "BEFORE snapshot → ${EXP_DIR}"

  docker exec sd-redis-1 redis-cli CONFIG GET maxmemory-policy maxmemory > "${EXP_DIR}/redis_config_before.txt"
  docker exec sd-redis-1 redis-cli INFO stats   > "${EXP_DIR}/redis_stats_before.txt"
  docker exec sd-redis-1 redis-cli INFO memory  > "${EXP_DIR}/redis_memory_before.txt"
  docker exec sd-redis-1 redis-cli INFO keyspace > "${EXP_DIR}/redis_keyspace_before.txt"

  docker exec -i sd-db-1 psql -U app -d qa -c \
  "\\COPY (
    SELECT COUNT(*) total,
           SUM(CASE WHEN COALESCE(NULLIF(final_answer,''), NULLIF(llm_answer,'')) IS NOT NULL THEN 1 ELSE 0 END) answered_any
    FROM interactions
  ) TO STDOUT WITH CSV HEADER" \
  > "${EXP_DIR}/baseline_before.csv"

  {
    echo "### kafka groups before"
    docker exec sd-kafka-1 kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group score-service || true
    docker exec sd-kafka-1 kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group cache-requests || true
    docker exec sd-kafka-1 kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group flink-answers-router || true
  } > "${EXP_DIR}/kafka_groups_before.txt" || true

  cat > "${EXP_DIR}/metadata_before.csv" <<EOF
name,distribution,policy,max_entries,rate,concurrency,pool_size,repeat_pool,sample_limit,router_threshold,timestamp
${name},${dist},${policy},${max_entries},${rate},${concurrency},${POOL_TARGET},${repeat_pool},${SAMPLES_TARGET},${threshold},$(date -Iseconds)
EOF

  snapshot_kafka_offsets "$EXP_DIR" "before"
}

snapshot_mid() {
  local name="$1" ; local dist="$2" ; local policy="$3" ; local max_entries="$4" ; local EXP_DIR="$5"
  log "MID snapshot → ${EXP_DIR}"

  docker exec -i sd-db-1 psql -U app -d qa -c \
  "\\COPY (
    SELECT COUNT(*) total,
           SUM(CASE WHEN COALESCE(NULLIF(final_answer,''), NULLIF(llm_answer,'')) IS NOT NULL THEN 1 ELSE 0 END) answered_any,
           ROUND(100.0*SUM(CASE WHEN COALESCE(NULLIF(final_answer,''), NULLIF(llm_answer,'')) IS NOT NULL THEN 1 ELSE 0 END)/COUNT(*),2) answer_rate_any,
           SUM(CASE WHEN cached THEN 1 ELSE 0 END) cache_hits,
           ROUND(100.0*SUM(CASE WHEN cached THEN 1 ELSE 0 END)/COUNT(*),2) cache_hit_rate,
           ROUND(AVG(latency_ms)::numeric,2) avg_latency
    FROM interactions
  ) TO STDOUT WITH CSV HEADER" \
  > "${EXP_DIR}/summary_mid.csv"

  docker exec -i sd-db-1 psql -U app -d qa -c \
  "\\COPY (
    SELECT
      COUNT(*) n,
      ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY latency_ms)::numeric,2) AS p50_ms,
      ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY latency_ms)::numeric,2) AS p90_ms,
      ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms)::numeric,2) AS p99_ms
    FROM interactions
  ) TO STDOUT WITH CSV HEADER" \
  > "${EXP_DIR}/latency_mid.csv"

  docker exec -i sd-db-1 psql -U app -d qa -c \
  "\\COPY (
    SELECT i.question_id, COUNT(*) times_asked,
           SUM(CASE WHEN i.cached THEN 1 ELSE 0 END) cache_hits,
           ROUND(100.0*SUM(CASE WHEN i.cached THEN 1 ELSE 0 END)/COUNT(*),2) cache_hit_rate
    FROM interactions i
    WHERE i.question_id IS NOT NULL
    GROUP BY i.question_id
    ORDER BY times_asked DESC
    LIMIT 20
  ) TO STDOUT WITH CSV HEADER" \
  > "${EXP_DIR}/top_qids_mid.csv"

  docker exec sd-redis-1 redis-cli INFO stats   > "${EXP_DIR}/redis_stats_mid.txt"
  docker exec sd-redis-1 redis-cli INFO memory  > "${EXP_DIR}/redis_memory_mid.txt"
  docker exec sd-redis-1 redis-cli INFO keyspace > "${EXP_DIR}/redis_keyspace_mid.txt"
  docker exec sd-redis-1 redis-cli --raw "SCAN 0 COUNT 1000" > "${EXP_DIR}/redis_scan_mid.txt" || true

  {
    echo "### kafka groups mid"
    docker exec sd-kafka-1 kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group score-service || true
    docker exec sd-kafka-1 kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group cache-requests || true
    docker exec sd-kafka-1 kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group flink-answers-router || true
  } > "${EXP_DIR}/kafka_groups_mid.txt" || true

  snapshot_kafka_offsets "$EXP_DIR" "mid"
  errors_breakdown "$EXP_DIR" "mid"

  docker logs sd-llm-1 --since 10m > "${EXP_DIR}/llm_mid.log" || true
  nvidia-smi -q -d MEMORY,UTILIZATION > "${EXP_DIR}/gpu_mid.txt" 2>/dev/null || true
  free -h > "${EXP_DIR}/ram_mid.txt"
}

snapshot_after() {
  local name="$1" ; local dist="$2" ; local policy="$3" ; local max_entries="$4" ; local EXP_DIR="$5" ; local repeat_pool="$6" ; local rate="$7" ; local concurrency="$8" ; local threshold="$9"

  docker exec -i sd-db-1 psql -U app -d qa -c \
    "COPY (SELECT * FROM interactions) TO STDOUT WITH CSV HEADER;" \
    > "${EXP_DIR}/interactions.csv"

  docker exec -i sd-db-1 psql -U app -d qa -c \
    "COPY (
       SELECT COUNT(*) as total,
              SUM(CASE WHEN COALESCE(NULLIF(final_answer,''), NULLIF(llm_answer,'')) IS NOT NULL THEN 1 ELSE 0 END) as answered_any,
              ROUND(100.0 * SUM(CASE WHEN COALESCE(NULLIF(final_answer,''), NULLIF(llm_answer,'')) IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS answer_rate_any,
              SUM(CASE WHEN cached THEN 1 ELSE 0 END) as cache_hits,
              SUM(CASE WHEN NOT cached THEN 1 ELSE 0 END) as cache_misses,
              ROUND(100.0 * SUM(CASE WHEN cached THEN 1 ELSE 0 END) / COUNT(*), 2) AS cache_hit_rate,
              ROUND(AVG(latency_ms)::numeric,2) as avg_latency,
              ROUND(AVG(score)::numeric,3) as avg_score
       FROM interactions
    ) TO STDOUT WITH CSV HEADER;" \
    > "${EXP_DIR}/summary.csv"

  docker exec -i sd-db-1 psql -U app -d qa -c \
    "COPY (
       SELECT
         COUNT(*) AS n,
         ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY latency_ms)::numeric,2) AS p50_ms,
         ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY latency_ms)::numeric,2) AS p90_ms,
         ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms)::numeric,2) AS p99_ms
       FROM interactions
     ) TO STDOUT WITH CSV HEADER;" \
    > "${EXP_DIR}/latency_percentiles.csv"

  docker exec sd-redis-1 redis-cli INFO stats    > "${EXP_DIR}/redis_stats_after.txt"
  docker exec sd-redis-1 redis-cli INFO memory   > "${EXP_DIR}/redis_memory_after.txt"
  docker exec sd-redis-1 redis-cli INFO keyspace > "${EXP_DIR}/redis_keyspace_after.txt"
  docker exec sd-redis-1 redis-cli CONFIG GET maxmemory-policy maxmemory > "${EXP_DIR}/redis_config_after.txt"

  {
    echo "### kafka groups after"
    docker exec sd-kafka-1 kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group score-service || true
    docker exec sd-kafka-1 kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group cache-requests || true
    docker exec sd-kafka-1 kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group flink-answers-router || true
  } > "${EXP_DIR}/kafka_groups_after.txt" || true

  snapshot_kafka_offsets "$EXP_DIR" "after"
  errors_breakdown "$EXP_DIR" "after"

  docker logs sd-llm-1     --since 12h > "${EXP_DIR}/llm.log"     || true
  docker logs sd-score-1   --since 12h > "${EXP_DIR}/score.log"   || true
  docker logs sd-storage-1 --since 12h > "${EXP_DIR}/storage.log" || true
  docker logs sd-cache-1   --since 12h > "${EXP_DIR}/cache.log"   || true

  cat > "${EXP_DIR}/metadata.csv" <<EOF
name,distribution,policy,max_entries,rate,concurrency,pool_size,repeat_pool,sample_limit,router_threshold,timestamp
${name},${dist},${policy},${max_entries},${rate},${concurrency},${POOL_TARGET},${repeat_pool},${SAMPLES_TARGET},${threshold},$(date -Iseconds)
EOF
}

# =======================
# Control Flink/Kafka
# =======================
reset_router_offsets() {
  for t in "${TOPICS_IN[@]}"; do
    docker compose exec -T kafka bash -lc \
      "kafka-consumer-groups --bootstrap-server localhost:9092 \
       --group flink-answers-router --topic ${t} \
       --reset-offsets --to-latest --execute" || true
  done
}

ensure_flink_job() {
  local threshold="${1:-$ROUTER_SCORE_THRESHOLD}"
  log "Validando job de Flink (threshold=${threshold}) ..."
  local out jobid
  out="$(docker compose exec -T flink-jobmanager bash -lc 'flink list' || true)"
  jobid="$(echo "$out" | awk -F' : ' '/RUNNING/ {print $2}' | head -n1)"
  if [[ -n "${jobid:-}" ]]; then
    log "Cancelando job previo ${jobid} ..."
    docker compose exec -T flink-jobmanager bash -lc "flink cancel ${jobid}" || true
  fi
  reset_router_offsets
  log "Enviando job con ROUTER_SCORE_THRESHOLD=${threshold} ..."
  docker compose exec -T flink-jobmanager bash -lc "ROUTER_SCORE_THRESHOLD=${threshold} flink run -py /opt/flink/job.py -d"
  sleep 3
  docker compose exec -T flink-jobmanager bash -lc 'flink list' || true
}

preflight_router() {
  local EXP_DIR="$1"
  mkdir -p "$EXP_DIR"
  log "Preflight: esperando adjunte del grupo flink-answers-router ..."
  for i in {1..30}; do
    local desc
    desc="$(docker compose exec -T kafka bash -lc \
      'kafka-consumer-groups --bootstrap-server localhost:9092 \
       --group flink-answers-router --describe' || true)"
    printf '%s\n' "$desc" > "${EXP_DIR}/kafka_group_attach.txt"
    if echo "$desc" | grep -q 'questions.answers' && echo "$desc" | grep -q 'answers.scored'; then
      log "Preflight OK: grupo adjunto."
      return 0
    fi
    sleep 1
  done
  log "Preflight: no se adjuntó el grupo a tiempo (ver kafka_group_attach.txt)"
  return 0
}

# =======================
# Ejecución
# =======================
run_experiment() {
  local name="$1" ; local dist="$2" ; local policy="$3"
  local max_entries="${4:-5000}" ; local repeat_pool="${5:-0}"
  local rate="${6:-$RATE}" ; local concurrency="${7:-$CONCURRENCY}"
  local threshold="${8:-$ROUTER_SCORE_THRESHOLD}"

  log "Iniciando experimento $name (Dist=$dist, Policy=$policy, MaxEntries=$max_entries, RepeatPool=$repeat_pool, Target=$SAMPLES_TARGET, Rate=$rate, Concurrency=$concurrency, Threshold=$threshold)"
  local EXP_DIR="$RESULTS_DIR/$name"
  mkdir -p "$EXP_DIR"

  # Pool reproducible
  local seed=$(( 12345 ^ $(echo -n "$name" | od -An -tu4 | tr -d ' ') ))
  make_pool "$name" "$POOL_TARGET" "$seed"
  local POOL_PATH="/data/pools/Tarea2/${name}.csv"

  # Reset DB y cache
  docker exec -i sd-db-1 psql -U app -d qa -c "TRUNCATE TABLE interactions RESTART IDENTITY CASCADE;"
  docker exec -i sd-db-1 psql -U app -d qa -c "TRUNCATE TABLE questions RESTART IDENTITY CASCADE;"
  docker exec sd-redis-1 redis-cli FLUSHALL
  docker exec sd-redis-1 redis-cli CONFIG SET maxmemory-policy "$policy"
  local est_mem=$(( max_entries * 20000 ))
  docker exec sd-redis-1 redis-cli CONFIG SET maxmemory "${est_mem}b"

  # Flink con umbral del experimento
  ensure_flink_job "$threshold"
  preflight_router "$EXP_DIR"

  # Snap inicial
  snapshot_before "$name" "$dist" "$policy" "$max_entries" "$EXP_DIR" "$repeat_pool" "$rate" "$concurrency" "$threshold"

  # Start traffic (se envía 3x y se corta al llegar a 10k reales)
  local GEN_LIMIT=$(( SAMPLES_TARGET * GEN_FACTOR ))
  log "Traffic /start con pool ${POOL_PATH} (sample_limit=${GEN_LIMIT}) ..."
  curl -s -X POST http://localhost:8010/start \
    -H "Content-Type: application/json" \
    -d "{
          \"csv_path\":\"${POOL_PATH}\",
          \"distribution\":\"${dist}\",
          \"rate\":${rate},
          \"concurrency\":${concurrency},
          \"sample_limit\":${GEN_LIMIT},
          \"run_id\":\"${name}\",
          \"POOL_SIZE\": 0,
          \"POOL_FRACTION\": 0,
          \"REPEAT_POOL_SIZE\": ${repeat_pool}
        }" \
    > "$EXP_DIR/start_response.json"

  # Progreso con guardias
  log "Esperando total persistido >= ${SAMPLES_TARGET} ..."
  local MID_DONE=0
  local MID_TARGET=$(( SAMPLES_TARGET / 2 ))
  local last=0
  local stalls=0
  local lenient_done=0

  while true; do
    counts=$(docker exec -i sd-db-1 psql -U app -d qa -t -A -c \
      "SELECT COALESCE(COUNT(*),0) FROM interactions;")
    counts_trimmed="${counts//[$'\t\r\n ']/}"

    # snapshot mid
    if [[ "$MID_DONE" -eq 0 && "$counts_trimmed" -ge "$MID_TARGET" ]]; then
      snapshot_mid "$name" "$dist" "$policy" "$max_entries" "$EXP_DIR"
      MID_DONE=1
    fi

    # fin por objetivo
    if [[ "$counts_trimmed" -ge "$SAMPLES_TARGET" ]]; then
      log "Objetivo alcanzado: $counts_trimmed filas en interactions"
      break
    fi

    # progreso / estancamiento
    if [[ "$counts_trimmed" -le "$last" ]]; then
      stalls=$((stalls+1))
    else
      stalls=0; last="$counts_trimmed"
    fi

    # activar modo lenient si no progresa
    if [[ "$stalls" -ge "$LENIENT_STALLS" && "$lenient_done" -eq 0 ]]; then
      log "Sin progreso ~6min → activando modo LENIENT (threshold=${LENIENT_THRESHOLD})"
      ensure_flink_job "$LENIENT_THRESHOLD"
      lenient_done=1
    fi

    # abortar realmente si ni con lenient progresa (se deja evidencia y se continúa)
    if [[ "$stalls" -ge "$STALL_MAX" ]]; then
      log "Sin progreso ~20min; corto experimento sin llegar a la meta (ver errores y logs)."
      break
    fi

    log "Progreso: $counts_trimmed / ${SAMPLES_TARGET} persistidos"
    sleep 30
  done

  # Stop traffic y snapshots finales
  curl -s -X POST http://localhost:8010/stop > /dev/null || true
  snapshot_after "$name" "$dist" "$policy" "$max_entries" "$EXP_DIR" "$repeat_pool" "$rate" "$concurrency" "$threshold"

  log "Experimento $name finalizado"
  echo "------------------------------------------------------"
}

run_experiment_from_map() {
  local mapname="$1"
  declare -n E="$mapname"
  run_experiment "${E[name]}" "${E[distribution]}" "${E[policy]}" "${E[max_entries]:-5000}" "${E[repeat_pool]:-0}" "${E[rate]:-$RATE}" "${E[concurrency]:-$CONCURRENCY}" "${E[threshold]:-$ROUTER_SCORE_THRESHOLD}"
}

# =======================
# Main
# =======================
ensure_dirs
wait_services

for EXP in "${EXPERIMENTS[@]}"; do
  run_experiment_from_map "$EXP"
done

log "Todos los experimentos completados. Resultados en ${RESULTS_DIR}/"
