# cache.py
from fastapi import FastAPI
import asyncio, os, json, time, logging
import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from datetime import datetime

# Config
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_REQUESTS = os.getenv("TOPIC_REQUESTS", "questions.requests")
TOPIC_LLM = os.getenv("TOPIC_LLM", "questions.llm")
TOPIC_ANSWERS = os.getenv("TOPIC_ANSWERS", "questions.answers")
TOPIC_CACHE_UPDATE = os.getenv("TOPIC_CACHE_UPDATE", "cache.update")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
CACHE_TTL = int(os.getenv("CACHE_TTL", "86400"))

app = FastAPI(title="Cache Service")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cache")

producer = None
consumer = None
consumer_update = None
redis = None

async def connect_to_kafka_with_retry():
    """Intentar conectar a Kafka con reintentos antes de levantar producer/consumers"""
    max_retries = 15
    retry_delay = 5

    for attempt in range(max_retries):
        try:
            test_producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            await test_producer.start()
            await test_producer.stop()
            logger.info("✅ Conexión a Kafka establecida")
            return True
        except Exception as e:
            logger.warning(f"Kafka no disponible (intento {attempt+1}/{max_retries}): {e}")
            await asyncio.sleep(retry_delay)
    logger.error("❌ No se pudo conectar a Kafka después de varios intentos")
    return False

@app.on_event("startup")
async def startup_event():
    global redis, producer, consumer, consumer_update
    redis = aioredis.from_url(REDIS_URL, decode_responses=True)

    # Configuración de Redis
    try:
        await redis.config_set("maxmemory-policy", os.getenv("REDIS_POLICY", "allkeys-lru"))
        await redis.config_set("maxmemory", os.getenv("REDIS_MAXMEMORY", "100mb"))
    except Exception as e:
        logger.warning(f"No se pudo configurar Redis: {e}")

    # Esperar Kafka listo
    ok = await connect_to_kafka_with_retry()
    if not ok:
        raise RuntimeError("Kafka no disponible, abortando inicio del servicio Cache")

    # Inicializar producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    await producer.start()

    # Consumer para requests
    consumer = AIOKafkaConsumer(
        TOPIC_REQUESTS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="cache-requests",
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )
    await consumer.start()
    asyncio.create_task(consume_requests_loop())

    # Consumer para actualizaciones de cache (desde Score)
    consumer_update = AIOKafkaConsumer(
        TOPIC_CACHE_UPDATE,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="cache-updates",
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )
    await consumer_update.start()
    asyncio.create_task(consume_updates_loop())

@app.on_event("shutdown")
async def shutdown():
    global producer, consumer, consumer_update, redis
    if consumer:
        await consumer.stop()
    if consumer_update:
        await consumer_update.stop()
    if producer:
        await producer.stop()
    if redis:
        await redis.close()

async def consume_requests_loop():
    async for msg in consumer:
        try:
            data = msg.value
            question = data.get("question","")
            qid = data.get("id")
            t0 = time.perf_counter()
            cached = await redis.get(question)
            if cached:
                try:
                    final = json.loads(cached)
                    final_answer = final.get("final_answer", final.get("answer", cached))
                except Exception:
                    final_answer = cached
                resp = {
                    "id": qid,
                    "question_id": data.get("question_id"),
                    "question": question,
                    "final_answer": final_answer,
                    "cached": True,
                    "latency_ms": int((time.perf_counter()-t0)*1000),
                    "ts_answered": datetime.utcnow().isoformat()
                }
                await producer.send_and_wait(TOPIC_ANSWERS, resp)
            else:
                await producer.send_and_wait(TOPIC_LLM, data)
        except Exception as e:
            logger.error(f"cache consume error: {e}")

async def consume_updates_loop():
    async for msg in consumer_update:
        try:
            data = msg.value
            question = data.get("question")
            final_answer = data.get("final_answer") or data.get("llm_answer") or data.get("reference_answer")
            if question and final_answer is not None:
                payload = {"final_answer": final_answer, "ts": data.get("ts_scored")}
                await redis.set(question, json.dumps(payload), ex=CACHE_TTL)
        except Exception as e:
            logger.error(f"cache update error: {e}")

@app.get("/health")
def health():
    return {"ok": True, "bootstrap": KAFKA_BOOTSTRAP,
            "topics": [TOPIC_REQUESTS, TOPIC_LLM, TOPIC_ANSWERS, TOPIC_CACHE_UPDATE]}
