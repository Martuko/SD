from fastapi import FastAPI
import os, asyncio, json, uuid
from aiokafka import AIOKafkaConsumer
import asyncpg

DB_URL = os.getenv("DATABASE_URL", "postgresql://app:app@db:5432/qa")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_STORAGE = os.getenv("TOPIC_STORAGE", "storage.persist")

app = FastAPI(title="Storage Service")

async def persist_interaction(pool, payload: dict):
    try:
        iid = uuid.UUID(str(payload.get("id")))
    except Exception:
        iid = uuid.uuid4()

    qid = payload.get("question_id")
    qtxt = payload.get("question")
    ref_ans = payload.get("reference_answer")

    async with pool.acquire() as con:
        if qid is not None:
            await con.execute(
                """
                INSERT INTO questions (id, question, best_answer)
                VALUES ($1, $2, $3)
                ON CONFLICT (id) DO NOTHING
                """,
                qid, qtxt, ref_ans
            )

        await con.execute(
            """
            INSERT INTO interactions(
                id, question_id, question, reference_answer,
                llm_answer, final_answer, cached, latency_ms,
                score, model, dist_label, rate, times_asked, created_at,
                score_cosine, score_rouge1, score_rougeL, score_bert
            )
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,1,now(),
                   $13,$14,$15,$16)
            ON CONFLICT (id) DO UPDATE
            SET times_asked = interactions.times_asked + 1,
                final_answer = CASE WHEN EXCLUDED.final_answer IS NOT NULL AND EXCLUDED.final_answer <> '' THEN EXCLUDED.final_answer ELSE interactions.final_answer END,
                cached = EXCLUDED.cached;
            """,
            iid, qid, qtxt, ref_ans,
            payload.get("llm_answer"), payload.get("final_answer"),
            bool(payload.get("cached", False)), payload.get("latency_ms"),
            payload.get("score"), payload.get("model"),
            payload.get("dist_label"), payload.get("rate"),
            payload.get("score_cosine"), payload.get("score_rouge1"),
            payload.get("score_rougeL"), payload.get("score_bert")
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
            try:
                await persist_interaction(pool, msg.value)
            except Exception as e:
                print("persist error:", e, "payload:", msg.value)
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
                created_at TIMESTAMPTZ DEFAULT now(),
                score_cosine DOUBLE PRECISION,
                score_rouge1 DOUBLE PRECISION,
                score_rougeL DOUBLE PRECISION,
                score_bert DOUBLE PRECISION
            );
            """
        )
        await con.execute('CREATE INDEX IF NOT EXISTS idx_interactions_cached ON interactions(cached);')
        await con.execute('CREATE INDEX IF NOT EXISTS idx_interactions_created ON interactions(created_at);')
        await con.execute('CREATE INDEX IF NOT EXISTS idx_interactions_qid ON interactions(question_id);')
    asyncio.create_task(consume_loop(app.state.pool))

@app.on_event("shutdown")
async def shutdown():
    await app.state.pool.close()

@app.get("/health")
def health():
    return {"ok": True}
