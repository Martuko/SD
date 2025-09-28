from fastapi import FastAPI
import os, asyncio, json, logging
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from datetime import datetime
from rouge_score import rouge_scorer
from bert_score import score as bert_score

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("score")

app = FastAPI(title="Score Service")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_IN = os.getenv("TOPIC_ANSWERS", "questions.answers")
TOPIC_STORAGE = os.getenv("TOPIC_STORAGE", "storage.persist")
TOPIC_CACHE_UPDATE = os.getenv("TOPIC_CACHE_UPDATE", "cache.update")

embedder = SentenceTransformer("all-MiniLM-L6-v2")
rouge_s = rouge_scorer.RougeScorer(['rouge1','rougeL'], use_stemmer=True)

producer: AIOKafkaProducer | None = None

async def calc_cosine(ref, cand):
    try:
        emb = embedder.encode([ref, cand])
        return float(cosine_similarity([emb[0]], [emb[1]])[0][0])
    except Exception as e:
        logger.warning(f"Cosine error: {e}")
        return 0.0

async def calc_rouge(ref, cand):
    try:
        sc = rouge_s.score(ref, cand)
        return sc['rouge1'].fmeasure, sc['rougeL'].fmeasure
    except Exception as e:
        logger.warning(f"ROUGE error: {e}")
        return 0.0, 0.0

async def calc_bert(ref, cand):
    try:
        P, R, F = bert_score([cand], [ref], lang='es', rescale_with_baseline=True)
        return float(F[0])
    except Exception as e:
        logger.warning(f"BERTScore error: {e}")
        return 0.0

async def process_msg(msg: dict):
    ref = msg.get("reference_answer") or ""
    cand = msg.get("llm_answer") or ""
    final_answer = cand or ref
    score_cos = await calc_cosine(ref, cand) if ref and cand else 0.0
    rouge1, rougeL = await calc_rouge(ref, cand) if ref and cand else (0.0,0.0)
    bertF = await calc_bert(ref, cand) if ref and cand else 0.0
    score_combined = 0.6*score_cos + 0.3*bertF + 0.1*rouge1

    scored = {
        "id": msg.get("id"),
        "question_id": msg.get("question_id"),
        "question": msg.get("question"),
        "reference_answer": ref,
        "llm_answer": cand,
        "final_answer": final_answer,
        "score_cosine": score_cos,
        "score_rouge1": rouge1,
        "score_rougeL": rougeL,
        "score_bert": bertF,
        "score": score_combined,
        "cached": bool(msg.get("cached", False)),
        "model": msg.get("model"),
        "latency_ms": msg.get("latency_ms", 0),
        "dist_label": msg.get("dist_label"),
        "rate": msg.get("rate"),
        "ts_scored": datetime.utcnow().isoformat()
    }
    await producer.send_and_wait(TOPIC_STORAGE, scored)
    await producer.send_and_wait(TOPIC_CACHE_UPDATE, scored)
    logger.info(f"Scored {msg.get('id')}")

async def consume_loop():
    consumer = AIOKafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="score-service",
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )
    await consumer.start()
    async for msg in consumer:
        await process_msg(msg.value)

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
    if producer: await producer.stop()

@app.get("/health")
def health():
    return {"ok": True}
