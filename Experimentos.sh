#!/bin/bash
set -euo pipefail

RESULTS_DIR=experiments_results
POOLS_DIR=data/pools
SRC_CSV=data/test.csv         
POOL_TARGET=30000             
SAMPLES_TARGET=10000          

mkdir -p "$RESULTS_DIR" "$POOLS_DIR"

# === Definición de experimentos ===
declare -A EXP1=( ["name"]="E1" ["distribution"]="poisson" ["policy"]="allkeys-lru" )
declare -A EXP2=( ["name"]="E2" ["distribution"]="poisson" ["policy"]="allkeys-lfu" )
declare -A EXP3=( ["name"]="E3" ["distribution"]="uniform" ["policy"]="allkeys-lru" )
declare -A EXP4=( ["name"]="E4" ["distribution"]="uniform" ["policy"]="allkeys-lfu" )
declare -A EXP5=( ["name"]="E5" ["distribution"]="poisson" ["policy"]="allkeys-lru" ["max_entries"]="2000" )
declare -A EXP6=( ["name"]="E6" ["distribution"]="poisson" ["policy"]="allkeys-lfu" ["max_entries"]="2000" )
EXPERIMENTS=( EXP1 EXP2 EXP3 EXP4 EXP5 EXP6 )

CONCURRENCY=2
RATE=5

make_pool() {
  local exp_name="$1"
  local dst="${POOLS_DIR}/${exp_name}.csv"
  local k=${2:-$POOL_TARGET}
  local seed="${3:-42}"

  if [[ -s "$dst" ]]; then
    echo "📦 Pool existente para ${exp_name}: $dst"
    return 0
  fi
  echo "🎲 Generando pool ${k} para ${exp_name} → $dst"

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


snapshot_before() {
  local name="$1" ; local dist="$2" ; local policy="$3" ; local max_entries="$4" ; local EXP_DIR="$5"
  echo "📸 BEFORE snapshot → ${EXP_DIR}"

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

  cat > "${EXP_DIR}/metadata_before.csv" <<EOF
name,distribution,policy,max_entries,rate,concurrency,pool_size,sample_limit,timestamp
${name},${dist},${policy},${max_entries},${RATE},${CONCURRENCY},${POOL_TARGET},${SAMPLES_TARGET},$(date -Iseconds)
EOF
}

snapshot_mid() {
  local name="$1" ; local dist="$2" ; local policy="$3" ; local max_entries="$4" ; local EXP_DIR="$5"
  echo "📸 MID snapshot → ${EXP_DIR}"

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
  docker exec sd-redis-1 redis-cli INFO memory  > "${EXP_DIR}/redis_memory_mid.txt"}
  docker exec sd-redis-1 redis-cli INFO keyspace > "${EXP_DIR}/redis_keyspace_mid.txt"
  docker exec sd-redis-1 redis-cli --raw "SCAN 0 COUNT 1000" > "${EXP_DIR}/redis_scan_mid.txt" || true

  docker exec sd-kafka-1 kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group score-service \
    > "${EXP_DIR}/kafka_score_mid.txt" || true
  docker exec sd-kafka-1 kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group cache-requests \
    > "${EXP_DIR}/kafka_cache_requests_mid.txt" || true

  docker logs sd-llm-1 --since 10m > "${EXP_DIR}/llm_mid.log" || true
  nvidia-smi -q -d MEMORY,UTILIZATION > "${EXP_DIR}/gpu_mid.txt" 2>/dev/null || true
  free -h > "${EXP_DIR}/ram_mid.txt"

  cat > "${EXP_DIR}/metadata_mid.csv" <<EOF
name,distribution,policy,max_entries,rate,concurrency,pool_size,sample_limit,timestamp
${name},${dist},${policy},${max_entries},${RATE},${CONCURRENCY},${POOL_TARGET},${SAMPLES_TARGET},$(date -Iseconds)
EOF
}

snapshot_after() {
  local name="$1" ; local dist="$2" ; local policy="$3" ; local max_entries="$4" ; local EXP_DIR="$5"

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

  docker logs sd-llm-1     --since 12h > "${EXP_DIR}/llm.log"     || true
  docker logs sd-score-1   --since 12h > "${EXP_DIR}/score.log"   || true
  docker logs sd-storage-1 --since 12h > "${EXP_DIR}/storage.log" || true
  docker logs sd-cache-1   --since 12h > "${EXP_DIR}/cache.log"   || true

  cat > "${EXP_DIR}/metadata.csv" <<EOF
name,distribution,policy,max_entries,rate,concurrency,pool_size,sample_limit,timestamp
${name},${dist},${policy},${max_entries},${RATE},${CONCURRENCY},${POOL_TARGET},${SAMPLES_TARGET},$(date -Iseconds)
EOF
}

# === Ejecución de un experimento ===
run_experiment() {
  local name=$1
  local dist=$2
  local policy=$3
  local max_entries=${4:-5000}

  echo "Iniciando experimento $name (Dist=$dist, Policy=$policy, MaxEntries=$max_entries, Target=$SAMPLES_TARGET)"
  local EXP_DIR="$RESULTS_DIR/$name"
  mkdir -p "$EXP_DIR"

  local seed=$(( 12345 ^ $(echo -n "$name" | od -An -tu4 | tr -d ' ') ))
  make_pool "$name" "$POOL_TARGET" "$seed"
  local POOL_PATH="/data/pools/${name}.csv"

  docker exec -i sd-db-1 psql -U app -d qa -c "TRUNCATE TABLE interactions RESTART IDENTITY CASCADE;"
  docker exec -i sd-db-1 psql -U app -d qa -c "TRUNCATE TABLE questions RESTART IDENTITY CASCADE;"
  docker exec sd-redis-1 redis-cli FLUSHALL
  docker exec sd-redis-1 redis-cli CONFIG SET maxmemory-policy "$policy"
  local est_mem=$(( max_entries * 20000 ))
  docker exec sd-redis-1 redis-cli CONFIG SET maxmemory "${est_mem}b"

  snapshot_before "$name" "$dist" "$policy" "$max_entries" "$EXP_DIR"

  echo "Traffic /start con pool ${POOL_PATH} ..."
  curl -s -X POST http://localhost:8010/start \
    -H "Content-Type: application/json" \
    -d "{
          \"csv_path\":\"${POOL_PATH}\",
          \"distribution\":\"${dist}\",
          \"rate\":${RATE},
          \"concurrency\":${CONCURRENCY},
          \"sample_limit\":${SAMPLES_TARGET},
          \"run_id\":\"${name}\",
          \"POOL_SIZE\": 0,
          \"POOL_FRACTION\": 0,
          \"REPEAT_POOL_SIZE\": 0
        }" \
    > "$EXP_DIR/start_response.json"

  echo "Esperando answered_any >= ${SAMPLES_TARGET} ..."
  local MID_DONE=0
  local MID_TARGET=$(( SAMPLES_TARGET / 2 ))

  while true; do
    counts=$(docker exec -i sd-db-1 psql -U app -d qa -t -A -c \
      "SELECT COALESCE(SUM(CASE WHEN COALESCE(NULLIF(final_answer,''), NULLIF(llm_answer,'')) IS NOT NULL THEN 1 ELSE 0 END),0) FROM interactions;")
    counts_trimmed="${counts//[$'\t\r\n ']/}"

    if [[ "$MID_DONE" -eq 0 && "$counts_trimmed" -ge "$MID_TARGET" ]]; then
      snapshot_mid "$name" "$dist" "$policy" "$max_entries" "$EXP_DIR"
      MID_DONE=1
    fi

    if [[ "$counts_trimmed" -ge "$SAMPLES_TARGET" ]]; then
      echo " Objetivo alcanzado: $counts_trimmed respuestas"
      break
    else
      echo " Progreso: $counts_trimmed / $SAMPLES_TARGET respuestas"
      sleep 30
    fi
  done

  
  curl -s -X POST http://localhost:8010/stop > /dev/null || true

  snapshot_after "$name" "$dist" "$policy" "$max_entries" "$EXP_DIR"

  echo "Experimento $name finalizado"
  echo "------------------------------------------------------"
}

for EXP in "${EXPERIMENTS[@]}"; do
  eval "run_experiment \${${EXP}[name]} \${${EXP}[distribution]} \${${EXP}[policy]} \${${EXP}[max_entries]:-5000}"
done

echo "Todos los experimentos completados. Resultados en $RESULTS_DIR/"
