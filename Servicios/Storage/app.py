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
    # Conversión segura de ID -> UUID
    try:
        iid = UUID(str(payload.get("id")))
    except Exception:
        iid = None

    # Extraer datos principales
    question_id = payload.get("question_id")
    question_text = payload.get("question")
    reference_answer = payload.get("reference_answer")

    async with pool.acquire() as con:
        # Insertar pregunta si no existe
        if question_id is not None:
            await con.execute(
                """
                INSERT INTO questions (id, question, best_answer)
                VALUES ($1, $2, $3)
                ON CONFLICT (id) DO NOTHING
                """,
                question_id,
                question_text,
                reference_answer,
            )

        # Insertar interacción
        await con.execute(
            """
            INSERT INTO interactions(
                id, question_id, question, reference_answer,
                llm_answer, final_answer, cached, latency_ms,
                score, model, dist_label, rate, times_asked, created_at
            )
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,1, now())
            ON CONFLICT (id) DO UPDATE
            SET times_asked = interactions.times_asked + 1;
            """,
            iid,
            question_id,
            question_text,
            reference_answer,
            payload.get("llm_answer"),
            payload.get("final_answer"),
            payload.get("cached", False),
            payload.get("latency_ms"),
            payload.get("score"),
            payload.get("model"),
            payload.get("dist_label"),
            payload.get("rate"),
        )


async def consume_loop(pool):
    consumer = AIOKafkaConsumer(
        TOPIC_STORAGE,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="storage-service",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    await consumer.start()
    try:
        async for msg in consumer:
            payload = msg.value
            try:
                await persist_interaction(pool, payload)
            except Exception as e:
                print("persist error:", e, "payload:", payload)
    finally:
        await consumer.stop()


@app.on_event("startup")
async def startup():
    app.state.pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=5)

    async with app.state.pool.acquire() as con:
        await con.execute(
            """
            CREATE TABLE IF NOT EXISTS questions (
                id BIGINT PRIMARY KEY,
                question TEXT,
                best_answer TEXT
            );
            """
        )
        await con.execute(
            """
            CREATE TABLE IF NOT EXISTS interactions (
                id UUID PRIMARY KEY,
                question_id BIGINT REFERENCES questions(id),
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
                times_asked INT DEFAULT 1,
                created_at TIMESTAMPTZ DEFAULT now()
            );
            """
        )

    asyncio.create_task(consume_loop(app.state.pool))


@app.get("/health")
async def health():
    return {"ok": True}


@app.on_event("shutdown")
async def shutdown():
    await app.state.pool.close()
