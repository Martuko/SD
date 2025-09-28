from fastapi import FastAPI
import asyncio, os, json, time, logging, hashlib, re
import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from datetime import datetime

# Config
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_REQUESTS = os.getenv("TOPIC_REQUESTS", "questions.requests")
TOPIC_LLM = os.getenv("TOPIC_LLM", "questions.llm")
TOPIC_ANSWERS = os.getenv("TOPIC_ANSWERS", "questions.answers")
TOPIC_CACHE_UPDATE = os.getenv("TOPIC_CACHE_UPDATE", "cache.update")
TOPIC_CACHE_METRICS = os.getenv("TOPIC_CACHE_METRICS", "cache.metrics")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
CACHE_TTL = int(os.getenv("CACHE_TTL", "86400"))
CACHE_MAX_ENTRIES = int(os.getenv("CACHE_MAX_ENTRIES", "5000"))
CACHE_POLICY = os.getenv("REDIS_POLICY", "allkeys-lru")  # lru, lfu, fifo

app = FastAPI(title="Cache Service")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cache")

producer = None
consumer = None
consumer_update = None
redis = None

# --------- utils -------------
def normalize_question(q: str) -> str:
    q = q or ""
    q = q.strip().lower()
    q = re.sub(r"\s+", " ", q)
    q = re.sub(r"[^\w\sñáéíóúü-]", "", q, flags=re.UNICODE)
    return q

def key_from(payload: dict) -> str:
    qid = payload.get("question_id")
    if qid is not None:
        return f"qid:{qid}"
    q = normalize_question(payload.get("question", ""))
    h = hashlib.sha256(q.encode("utf-8")).hexdigest()
    return f"qhash:{h}"

async def enforce_fifo():
    """Mantener FIFO manual si se excede CACHE_MAX_ENTRIES"""
    sz = await redis.zcard("cache_queue")
    while sz > CACHE_MAX_ENTRIES:
        k = await redis.zpopmin("cache_queue")
        if k and len(k) > 0:
            victim = k[0][0]
            await redis.delete(victim)
            logger.info(f"FIFO evicted {victim}")
        sz = await redis.zcard("cache_queue")

# --------- startup/shutdown -------------
async def connect_to_kafka_with_retry():
    max_retries, retry_delay = 15, 5
    for attempt in range(max_retries):
        try:
            test = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
            await test.start(); await test.stop()
            return True
        except Exception as e:
            logger.warning(f"Kafka not ready ({attempt+1}/{max_retries}): {e}")
            await asyncio.sleep(retry_delay)
    return False

@app.on_event("startup")
async def startup_event():
    global redis, producer, consumer, consumer_update
    redis = aioredis.from_url(REDIS_URL, decode_responses=True)

    # Config Redis policy
    try:
        if CACHE_POLICY in ["allkeys-lru", "allkeys-lfu"]:
            await redis.config_set("maxmemory-policy", CACHE_POLICY)
            await redis.config_set("maxmemory", os.getenv("REDIS_MAXMEMORY", "100mb"))
        logger.info(f"Cache policy set to {CACHE_POLICY}")
    except Exception as e:
        logger.warning(f"Could not config Redis: {e}")

    ok = await connect_to_kafka_with_retry()
    if not ok:
        raise RuntimeError("Kafka unavailable")

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    await producer.start()

    consumer = AIOKafkaConsumer(
        TOPIC_REQUESTS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="cache-requests",
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )
    await consumer.start()
    asyncio.create_task(consume_requests_loop())

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
    if consumer: await consumer.stop()
    if consumer_update: await consumer_update.stop()
    if producer: await producer.stop()
    if redis: await redis.close()

# --------- consumers -------------
async def consume_requests_loop():
    async for msg in consumer:
        data = msg.value
        qid, question = data.get("question_id"), data.get("question","")
        k = key_from(data)
        t0 = time.perf_counter()
        cached = await redis.get(k)
        hit = bool(cached)
        if hit:
            final = json.loads(cached)
            resp = {
                "id": data.get("id"),
                "question_id": qid,
                "question": question,
                "final_answer": final.get("final_answer"),
                "cached": True,
                "latency_ms": int((time.perf_counter()-t0)*1000),
                "ts_answered": datetime.utcnow().isoformat()
            }
            await producer.send_and_wait(TOPIC_ANSWERS, resp)
        else:
            await producer.send_and_wait(TOPIC_LLM, data)

        # métricas
        metric = {
            "experiment": os.getenv("EXPERIMENT_ID","default"),
            "key": k, "hit": hit,
            "latency_ms": int((time.perf_counter()-t0)*1000),
            "ts": datetime.utcnow().isoformat()
        }
        await producer.send_and_wait(TOPIC_CACHE_METRICS, metric)

async def consume_updates_loop():
    async for msg in consumer_update:
        data = msg.value
        k = key_from(data)
        final_answer = data.get("final_answer") or data.get("llm_answer") or data.get("reference_answer")
        if k and final_answer:
            payload = {"final_answer": final_answer, "ts": data.get("ts_scored")}
            await redis.set(k, json.dumps(payload), ex=CACHE_TTL)
            if CACHE_POLICY == "fifo":
                await redis.zadd("cache_queue", {k: time.time()})
                await enforce_fifo()

@app.get("/health")
def health():
    return {"ok": True, "policy": CACHE_POLICY}
