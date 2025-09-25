# storage.py
from fastapi import FastAPI
import os, asyncio, json
from aiokafka import AIOKafkaConsumer
import asyncpg
from uuid import UUID

DB_URL = os.getenv("DATABASE_URL", "postgresql://app:app@db:5432/qa")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_STORAGE = os.getenv("TOPIC_STORAGE", "storage.persist")

app = FastAPI(title="Storage Service")

async def persist_interaction(pool, payload: dict):
    # safe conversion of id -> UUID
    try:
        iid = UUID(payload.get("id"))
    except Exception:
        iid = None
    async with pool.acquire() as con:
        await con.execute("""
            INSERT INTO interactions(
                id, question_id, question, reference_answer,
                llm_answer, final_answer, cached, latency_ms,
                score, model, dist_label, rate, created_at
            ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12, now())
            ON CONFLICT (id) DO NOTHING
        """,
        iid,
        payload.get("question_id"),
        payload.get("question"),
        payload.get("reference_answer"),
        payload.get("llm_answer"),
        payload.get("final_answer"),
        payload.get("cached", False),
        payload.get("latency_ms"),
        payload.get("score"),
        payload.get("model"),
        payload.get("dist_label"),
        payload.get("rate")
        )

async def consume_loop(pool):
    consumer = AIOKafkaConsumer(
        TOPIC_STORAGE,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="storage-service",
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )
    await consumer.start()
    try:
        async for msg in consumer:
            payload = msg.value
            try:
                await persist_interaction(pool, payload)
            except Exception as e:
                print("persist error:", e)
    finally:
        await consumer.stop()

@app.on_event("startup")
async def startup():
    import time
    max_retries = int(os.getenv("DB_INIT_MAX_RETRIES", "60"))
    delay = float(os.getenv("DB_INIT_RETRY_DELAY", "1.0"))
    last_err = None
    for _ in range(max_retries):
        try:
            pool = await asyncpg.create_pool(dsn=DB_URL, timeout=5.0, ssl=False)
            app.state.pool = pool
            break
        except Exception as e:
            last_err = e
            await asyncio.sleep(delay)
    if not getattr(app.state, "pool", None):
        raise last_err

    # create tables if not present
    async with app.state.pool.acquire() as con:
        await con.execute("""
        CREATE TABLE IF NOT EXISTS interactions (
            id UUID PRIMARY KEY,
            question_id BIGINT,
            question TEXT,
            reference_answer TEXT,
            llm_answer TEXT,
            final_answer TEXT,
            cached BOOLEAN DEFAULT FALSE,
            latency_ms INT,
            score DOUBLE PRECISION,
            model TEXT,
            dist_label TEXT,
            rate DOUBLE PRECISION,
            created_at TIMESTAMPTZ DEFAULT now()
        );
        """)

    asyncio.create_task(consume_loop(app.state.pool))

@app.get("/health")
async def health():
    return {"ok": True}
