#!/bin/bash
set -euo pipefail

RESULTS_DIR=experiments_results
mkdir -p "$RESULTS_DIR"

# Cada experimento: run_id, distribución, política Redis
declare -A EXP1=( ["run_id"]="E1" ["distribution"]="poisson" ["policy"]="allkeys-lru" )
declare -A EXP2=( ["run_id"]="E2" ["distribution"]="poisson" ["policy"]="allkeys-lfu" )
declare -A EXP3=( ["run_id"]="E3" ["distribution"]="uniform" ["policy"]="allkeys-lru" )
declare -A EXP4=( ["run_id"]="E4" ["distribution"]="uniform" ["policy"]="allkeys-lfu" )

EXPERIMENTS=( EXP1 EXP2 EXP3 EXP4 )

TARGET=15000   # mínimo de respuestas por experimento
CONCURRENCY=2
RATE=2

run_experiment() {
    local run_id=$1
    local dist=$2
    local policy=$3

    echo "🚀 Iniciando experimento $run_id (Dist=$dist, Policy=$policy, Target=$TARGET)"

    local EXP_DIR="$RESULTS_DIR/$run_id"
    mkdir -p "$EXP_DIR"

    # --- Reset estado ---
    echo "🧹 Limpiando DB y Redis..."
    docker exec -i sd-db-1 psql -U app -d qa -c "TRUNCATE TABLE interactions RESTART IDENTITY CASCADE;"
    docker exec -i sd-db-1 psql -U app -d qa -c "TRUNCATE TABLE questions RESTART IDENTITY CASCADE;"
    docker exec sd-redis-1 redis-cli FLUSHALL
    docker exec sd-redis-1 redis-cli CONFIG SET maxmemory 100mb
    docker exec sd-redis-1 redis-cli CONFIG SET maxmemory-policy "$policy"

    # --- Lanzar tráfico ---
    echo "📤 Traffic /start ..."
    curl -s -X POST http://localhost:8010/start \
        -H "Content-Type: application/json" \
        -d "{\"distribution\":\"$dist\",\"rate\":$RATE,\"concurrency\":$CONCURRENCY,\"sample_limit\":$TARGET,\"run_id\":\"$run_id\"}" \
        > "$EXP_DIR/start_response.json"

    # --- Polling hasta alcanzar el objetivo ---
    echo "⏳ Esperando a que answered >= $TARGET ..."
    while true; do
        counts=$(docker exec -i sd-db-1 psql -U app -d qa -t -A -c \
          "SELECT COALESCE(SUM(CASE WHEN llm_answer != '' THEN 1 ELSE 0 END),0) FROM interactions;")
        if [ "$counts" -ge "$TARGET" ]; then
            echo "✅ Objetivo alcanzado: $counts respuestas"
            break
        else
            echo "⌛ Progreso: $counts / $TARGET respuestas"
            sleep 60   # espera 1 min antes de volver a consultar
        fi
    done

    # --- Guardar datos brutos ---
    echo "📥 Exportando interacciones..."
    docker exec -i sd-db-1 psql -U app -d qa -c \
      "COPY (SELECT * FROM interactions WHERE run_id='$run_id') TO STDOUT WITH CSV HEADER;" \
      > "$EXP_DIR/interactions.csv"

    # --- Guardar resumen ---
    echo "📊 Guardando métricas..."
    docker exec -i sd-db-1 psql -U app -d qa -c \
      "SELECT run_id,
              COUNT(*) as total,
              SUM(CASE WHEN cached THEN 1 ELSE 0 END) as cached,
              SUM(CASE WHEN NOT cached THEN 1 ELSE 0 END) as misses,
              ROUND(AVG(latency_ms)::numeric,2) as avg_latency,
              ROUND(AVG(score)::numeric,3) as avg_score
       FROM interactions
       WHERE run_id='$run_id'
       GROUP BY run_id;" \
      > "$EXP_DIR/summary.txt"

    # --- Guardar logs ---
    echo "📝 Guardando logs..."
    docker logs sd-llm-1     --since 4h > "$EXP_DIR/llm.log"
    docker logs sd-score-1   --since 4h > "$EXP_DIR/score.log"
    docker logs sd-storage-1 --since 4h > "$EXP_DIR/storage.log"
    docker logs sd-cache-1   --since 4h > "$EXP_DIR/cache.log"

    echo "✅ Experimento $run_id finalizado"
    echo "------------------------------------------------------"
}

# === Loop de experimentos ===
for EXP in "${EXPERIMENTS[@]}"; do
    eval "run_experiment \${${EXP}[run_id]} \${${EXP}[distribution]} \${${EXP}[policy]}"
done

echo "🎉 Todos los experimentos completados. Resultados en $RESULTS_DIR/"
