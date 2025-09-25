# cache.py
from fastapi import FastAPI
import asyncio, os, json, time
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
CACHE_TTL = int(os.getenv("CACHE_TTL", "86400"))  # segundos

app = FastAPI(title="Cache Service")

producer = None
consumer = None
consumer_update = None
redis = None

@app.on_event("startup")
async def startup_event():
    global redis, producer, consumer, consumer_update
    redis = aioredis.from_url(REDIS_URL, decode_responses=True)
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

    # consumer para actualizaciones de cache (desde score)
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
                # cached contains the final_answer JSON or plain string; try parse
                try:
                    final = json.loads(cached)
                    final_answer = final.get("final_answer", final.get("answer", cached))
                except Exception:
                    final_answer = cached

                resp = {
                    "id": qid,
                    "question": question,
                    "final_answer": final_answer,
                    "cached": True,
                    "latency_ms": int((time.perf_counter()-t0)*1000),
                    "ts_answered": datetime.utcnow().isoformat()
                }
                # publish as an answer (so score/storage can track hits if desired)
                await producer.send_and_wait(TOPIC_ANSWERS, resp)
            else:
                # miss -> forward original payload to LLM topic
                # ensure we pass question_id and reference_answer for scoring later
                await producer.send_and_wait(TOPIC_LLM, data)
        except Exception as e:
            # no queremos parar el loop por un error puntual
            print("cache consume error:", e)

async def consume_updates_loop():
    """Recebe actualizaciones desde Score para refrescar la cache"""
    async for msg in consumer_update:
        try:
            data = msg.value
            question = data.get("question")
            final_answer = data.get("final_answer") or data.get("llm_answer") or data.get("reference_answer")
            if question and final_answer is not None:
                # guardamos como JSON string con metadata
                payload = {"final_answer": final_answer, "ts": data.get("ts_scored")}
                await redis.set(question, json.dumps(payload), ex=CACHE_TTL)
        except Exception as e:
            print("cache update error:", e)

@app.get("/health")
def health():
    return {"ok": True, "bootstrap": KAFKA_BOOTSTRAP, "topics": [TOPIC_REQUESTS, TOPIC_LLM, TOPIC_ANSWERS, TOPIC_CACHE_UPDATE]}
