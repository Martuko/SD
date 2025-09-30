# Servicios/Score/app.py
from fastapi import FastAPI
import os, asyncio, json, logging
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from datetime import datetime

# Scores opcionales
ENABLE_EMBEDDINGS = os.getenv("ENABLE_EMBEDDINGS", "1") == "1"
ENABLE_ROUGE = os.getenv("ENABLE_ROUGE", "1") == "1"
ENABLE_BERTSCORE = os.getenv("ENABLE_BERTSCORE", "0") == "1"
SCORE_THRESHOLD = float(os.getenv("SCORE_THRESHOLD", "0.6"))

# Cargas perezosas para acelerar arranque si están deshabilitados
embedder = None
rouge_s = None
bert_score = None

if ENABLE_EMBEDDINGS:
    from sentence_transformers import SentenceTransformer
    from sklearn.metrics.pairwise import cosine_similarity
    embedder = SentenceTransformer("all-MiniLM-L6-v2")

if ENABLE_ROUGE:
    from rouge_score import rouge_scorer
    rouge_s = rouge_scorer.RougeScorer(['rouge1','rougeL'], use_stemmer=True)

if ENABLE_BERTSCORE:
    from bert_score import score as bert_score  # type: ignore

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("score")

app = FastAPI(title="Score Service")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_IN = os.getenv("TOPIC_ANSWERS", "questions.answers")
TOPIC_STORAGE = os.getenv("TOPIC_STORAGE", "storage.persist")
TOPIC_CACHE_UPDATE = os.getenv("TOPIC_CACHE_UPDATE", "cache.update")

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

def choose_final_answer(ref: str, cand: str, score_combined: float) -> str:
    # Regla del enunciado: si score < threshold => usar referencia; si no => LLM
    if cand.strip() and score_combined >= SCORE_THRESHOLD:
        return cand
    # fallback robusto si ref viene vacía
    return cand if cand.strip() and not ref.strip() else ref

async def process_msg(msg: dict):
    # 1) Mensajes cacheados: sólo persistir, NO actualizar caché
    if msg.get("cached"):
        final_answer = (msg.get("final_answer") or "").strip()
        scored = {
            "id": msg.get("id"),
            "question_id": msg.get("question_id"),
            "question": msg.get("question"),
            "reference_answer": msg.get("reference_answer") or "",
            "llm_answer": "",
            "final_answer": final_answer,
            "score_cosine": 0.0,
            "score_rouge1": 0.0,
            "score_rougeL": 0.0,
            "score_bert": 0.0,
            "score": 0.0,
            "cached": True,
            "model": msg.get("model", "cache"),
            "latency_ms": msg.get("latency_ms", 0),
            "dist_label": msg.get("dist_label"),
            "rate": msg.get("rate"),
            "ts_scored": datetime.utcnow().isoformat()
        }
        await producer.send_and_wait(TOPIC_STORAGE, scored)
        logger.info(f"Stored cached hit {msg.get('id')} without cache.update")
        return

    # 2) Flujo normal
    ref = (msg.get("reference_answer") or "").strip()
    cand = (msg.get("llm_answer") or "").strip()

    score_cos = 0.0
    rouge1 = 0.0
    rougeL = 0.0
    bertF = 0.0

    if ref and cand and ENABLE_EMBEDDINGS:
        score_cos = await calc_cosine(ref, cand)
    if ref and cand and ENABLE_ROUGE:
        rouge1, rougeL = await calc_rouge(ref, cand)
    if ref and cand and ENABLE_BERTSCORE:
        bertF = await calc_bert(ref, cand)

    # 60/30/10 como en tu esquema
    weights = (
        (0.6 if ENABLE_EMBEDDINGS else 0.0) +
        (0.3 if ENABLE_BERTSCORE else 0.0) +
        (0.1 if ENABLE_ROUGE else 0.0)
    )
    if weights == 0.0:
        score_combined = 1.0 if cand else 0.0
    else:
        score_combined = (
            (0.6 * score_cos if ENABLE_EMBEDDINGS else 0.0) +
            (0.3 * bertF if ENABLE_BERTSCORE else 0.0) +
            (0.1 * rouge1 if ENABLE_ROUGE else 0.0)
        )

    final_answer = choose_final_answer(ref, cand, score_combined)

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
        "cached": False,
        "model": msg.get("model"),
        "latency_ms": msg.get("latency_ms", 0),
        "dist_label": msg.get("dist_label"),
        "rate": msg.get("rate"),
        "ts_scored": datetime.utcnow().isoformat()
    }
    await producer.send_and_wait(TOPIC_STORAGE, scored)
    # actualizar caché sólo en miss
    await producer.send_and_wait(TOPIC_CACHE_UPDATE, scored)
    logger.info(f"Scored and updated cache for {msg.get('id')}")

async def consume_loop():
    consumer = AIOKafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="score-service",
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )
    await consumer.start()
    try:
        async for msg in consumer:
            await process_msg(msg.value)
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
    if producer: await producer.stop()

@app.get("/health")
def health():
    return {"ok": True, "threshold": SCORE_THRESHOLD,
            "embeddings": ENABLE_EMBEDDINGS, "rouge": ENABLE_ROUGE, "bertscore": ENABLE_BERTSCORE}
