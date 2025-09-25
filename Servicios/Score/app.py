# score.py
from fastapi import FastAPI
import os, time, asyncio, json, httpx, logging
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("score")

app = FastAPI(title="Score Service")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_IN = os.getenv("TOPIC_ANSWERS", "questions.answers")
TOPIC_STORAGE = os.getenv("TOPIC_STORAGE", "storage.persist")
TOPIC_CACHE_UPDATE = os.getenv("TOPIC_CACHE_UPDATE", "cache.update")

# Embedder
embedder = SentenceTransformer("all-MiniLM-L6-v2")

producer: AIOKafkaProducer | None = None
consumer: AIOKafkaConsumer | None = None

async def calc_score(reference: str, candidate: str) -> float:
    try:
        emb = embedder.encode([reference, candidate])
        return float(cosine_similarity([emb[0]], [emb[1]])[0][0])
    except Exception as e:
        logger.warning("Embedding error: %s", e)
        return 0.0

async def process_msg(msg: dict):
    interaction_id = msg.get("id")
    question = msg.get("question","")
    question_id = msg.get("question_id")
    llm_answer = msg.get("llm_answer", "")
    reference_answer = msg.get("reference_answer") or msg.get("reference") or ""
    ts_start = msg.get("ts_generated") or msg.get("ts_answered") or datetime.utcnow().isoformat()
    # if reference not present, try to fetch from storage API (best-effort)
    if not reference_answer and question_id:
        try:
            async with httpx.AsyncClient(timeout=10) as cli:
                r = await cli.get(f"http://storage:8003/questions/{question_id}")
                if r.status_code == 200:
                    reference_answer = r.json().get("best_answer","")
        except Exception:
            reference_answer = ""

    # decide final answer
    final_answer = ""
    score_val = None
    if reference_answer and llm_answer:
        score_llm = await calc_score(reference_answer, llm_answer)
        # also compute score of reference vs itself (should be 1) but we treat reference as baseline
        score_ref = 1.0
        # choose the answer with higher similarity to reference (here trivial)
        if score_llm >= score_ref:
            final_answer = llm_answer
            score_val = score_llm
        else:
            final_answer = reference_answer
            score_val = score_ref
    elif llm_answer:
        final_answer = llm_answer
        score_val = 0.0
    else:
        final_answer = reference_answer or ""
        score_val = 0.0

    latency_ms = int((time.time() - time.time()) * 1000)  # placeholder, we keep 0 if not measured

    scored = {
        "id": interaction_id,
        "question_id": question_id,
        "question": question,
        "reference_answer": reference_answer,
        "llm_answer": llm_answer,
        "final_answer": final_answer,
        "score": float(score_val),
        "cached": bool(msg.get("cached", False)),
        "model": msg.get("model"),
        "latency_ms": msg.get("latency_ms", 0),
        "ts_scored": datetime.utcnow().isoformat()
    }

    # publish to storage for persistence and to cache for updating
    await producer.send_and_wait(TOPIC_STORAGE, scored)
    await producer.send_and_wait(TOPIC_CACHE_UPDATE, scored)
    logger.info("Scored and published %s", interaction_id)

async def consume_loop():
    global consumer
    consumer = AIOKafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="score-service",
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )
    await consumer.start()
    try:
        async for msg in consumer:
            try:
                payload = msg.value
                await process_msg(payload)
            except Exception as e:
                logger.error("Error processing payload: %s", e)
    finally:
        await consumer.stop()

@app.on_event("startup")
async def startup():
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    await producer.start()
    asyncio.create_task(consume_loop())

@app.on_event("shutdown")
async def shutdown():
    global producer
    if producer:
        await producer.stop()

@app.get("/health")
def health():
    return {"ok": True, "topic_in": TOPIC_IN, "topic_out_storage": TOPIC_STORAGE, "topic_cache_update": TOPIC_CACHE_UPDATE}
