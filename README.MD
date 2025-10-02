# Tarea 1 — Sistemas Distribuidos
Plataforma event-driven para QA con **LLM local + caché** y **persistencia**. Los servicios se comunican vía Kafka y se despliegan con Docker Compose.

## Servicios

- **traffic**: genera carga (Poisson/Uniforme) desde CSV y publica en Kafka.
- **cache**: consulta Redis; en *hit* responde, en *miss* re-encola a LLM.
- **llm**: llama a **Ollama** (modelo local) y publica la respuesta.
- **score**: calcula *score* (ROUGE/Embeddings) y actualiza caché + persiste.
- **storage**: guarda interacciones en PostgreSQL.
- **infra**: Zookeeper, Kafka, Redis, PostgreSQL, Ollama.

> **Requisitos**: `docker` ≥ 24, `docker compose` v2, ~8–10 GB libres de disco.
>
> **Puertos** usados por defecto: `2181` (zookeeper), `9092` (kafka), `8010` (traffic), `6379` (redis), `5432` (postgres), `11434` (ollama).

---

## Preparación

1) **Clona** el repo y entra a la carpeta del proyecto:
```bash
git clone https://github.com/Martuko/SD/tree/e2c1c67bca6d45f6cbd7e3379cf82da1470853e8.git
cd SD
```
2) **Build**
```bash
docker-compose build 
```

3) **Aranque paso a paso**
Si antes tuviste conflictos, limpia puertos:
```bash
sudo fuser -k 2181/tcp 9092/tcp || true
```
Arranque Docker
```bash
docker-compose up -d  
```
Comprobaciones rapidas
```bash
curl -f http://localhost:8000/health
curl -f http://localhost:8001/health
curl -f http://localhost:8002/health
curl -f http://localhost:8003/health
curl -f http://localhost:8010/health  
```

## Test
Lanza un run pequeño (Poisson, 5 qps, concurrency 2, 50 muestras):
```bash
curl -s -X POST http://localhost:8010/start \
  -H "Content-Type: application/json" \
  -d '{
    "csv_path": "/data/pools/E1.csv",
    "distribution": "poisson",
    "rate": 5,
    "concurrency": 2,
    "sample_limit": 50,
    "run_id": "E50"
  }'
```
Consulta SQL — totales, answer-rate y cache hit-rate:
```bash
docker exec -it sd-db-1 psql -U app -d qa -c "
SELECT
  COUNT(*) AS total,
  SUM(CASE WHEN COALESCE(NULLIF(final_answer,''), NULLIF(llm_answer,'')) IS NOT NULL THEN 1 ELSE 0 END) AS answered_any,
  ROUND(
    100.0 * SUM(CASE WHEN COALESCE(NULLIF(final_answer,''), NULLIF(llm_answer,'')) IS NOT NULL THEN 1 ELSE 0 END)::numeric
    / NULLIF(COUNT(*),0), 2
  ) AS answer_rate_any,
  SUM(CASE WHEN cached THEN 1 ELSE 0 END) AS cache_hits,
  ROUND(
    100.0 * SUM(CASE WHEN cached THEN 1 ELSE 0 END)::numeric
    / NULLIF(COUNT(*),0), 2
  ) AS cache_hit_rate
FROM interactions;"
```