# Servicios/LLM/app.py
from fastapi import FastAPI
import os, time, asyncio, httpx, json, logging
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("llm")

app = FastAPI(title="LLM Service")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_IN    = os.getenv("TOPIC_LLM", "questions.llm")
TOPIC_OUT   = os.getenv("TOPIC_ANSWERS", "questions.answers")
TOPIC_ERR   = os.getenv("TOPIC_ERRORS", "questions.errors")

OLLAMA = os.getenv("OLLAMA_HOST", "http://ollama:11434")
MODEL  = os.getenv("LLM_MODEL", "llama3.1:8b-instruct-q4_K_M")
MAX_TOK = int(os.getenv("LLM_MAX_TOKENS", "512"))
TEMP    = float(os.getenv("LLM_TEMPERATURE", "0.2"))
SYS_PROMPT = os.getenv("SYS_PROMPT", "You are a concise and factual assistant. Answer in English.")
LLM_CONCURRENCY = int(os.getenv("LLM_CONCURRENCY", "2"))

producer: AIOKafkaProducer | None = None
consumer: AIOKafkaConsumer | None = None
kafka_ready = asyncio.Event()
consumption_task: asyncio.Task | None = None
llm_sem = asyncio.Semaphore(LLM_CONCURRENCY)

async def connect_to_kafka_with_retry():
    global producer
    for attempt in range(1, 16):
        try:
            producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            await producer.start()
            await producer.client.bootstrap()
            logger.info("Conexión a Kafka establecida")
            kafka_ready.set()
            return True
        except Exception as e:
            logger.warning(f"Intento Kafka {attempt}/15 falló: {e}")
            if producer:
                await producer.stop()
                producer = None
            await asyncio.sleep(5)
    return False

async def call_ollama_stream(prompt: str, model: str) -> str:
    async with llm_sem:
        try:
            async with httpx.AsyncClient(timeout=None) as client:
                async with client.stream("POST", f"{OLLAMA}/api/generate", json={
                    "model": model,
                    "prompt": prompt,
                    "options": {"temperature": TEMP, "num_predict": MAX_TOK},
                    "stream": True
                }) as resp:
                    resp.raise_for_status()
                    parts = []
                    async for line in resp.aiter_lines():
                        if not line or not line.strip():
                            continue
                        try:
                            data = json.loads(line)
                            if "response" in data:
                                parts.append(data["response"])
                            if data.get("done", False):
                                break
                        except Exception:
                            pass
                    return "".join(parts)
        except httpx.HTTPStatusError as e:
            # 429 → rate limit; 5xx → overload/timeout
            status = e.response.status_code
            if status == 429:
                raise RuntimeError("RATE_LIMIT") from e
            elif 500 <= status < 600:
                raise RuntimeError("OVERLOAD") from e
            raise
        except Exception as e:
            # genérico: timeout/overload
            raise RuntimeError("OVERLOAD") from e

async def publish_error(payload: dict, error_type: str):
    if not producer:
        return
    attempt = int(payload.get("attempt", 0))
    err_msg = {
        "error_type": error_type,
        "attempt": attempt,
        "question_payload": {
            "id": payload.get("id"),
            "question_id": payload.get("question_id"),
            "question": payload.get("question"),
            "reference_answer": payload.get("reference_answer"),
            # conservar reintentos de calidad y metadata del pipeline
            "retries": int(payload.get("retries", 0)),
            "dist_label": payload.get("dist_label"),
            "rate": payload.get("rate"),
            "run_id": payload.get("run_id"),
            "attempt": attempt
        },
        "ts": datetime.utcnow().isoformat()
    }
    await producer.send_and_wait(TOPIC_ERR, err_msg)
    logger.warning(f"Publicado error {error_type} (attempt={attempt}) para id={payload.get('id')}")

async def process_question(payload: dict):
    interaction_id = payload.get("id")
    question = payload.get("question", "")
    question_id = payload.get("question_id")
    reference_answer = payload.get("reference_answer")
    attempt = int(payload.get("attempt", 0))
    retries = int(payload.get("retries", 0))  # <- calidad
    ts_start = time.perf_counter()

    try:
        llm_answer = await call_ollama_stream(
            f"{SYS_PROMPT}\n\nPregunta: {question}\nRespuesta:", MODEL
        )
    except RuntimeError as e:
        await publish_error(payload, str(e))
        return
    except Exception:
        await publish_error(payload, "OVERLOAD")
        return

    if not (llm_answer or "").strip():
        await publish_error(payload, "OVERLOAD")
        return

    latency_ms = int((time.perf_counter() - ts_start) * 1000)
    out_msg = {
        "id": interaction_id,
        "question_id": question_id,
        "question": question,
        "llm_answer": llm_answer or "",
        "reference_answer": reference_answer or "",
        "cached": False,
        "latency_ms": latency_ms,
        "model": MODEL,
        "ts_answered": datetime.utcnow().isoformat(),
        "attempt": attempt,       # reintentos por error
        "retries": retries,       # reintentos por baja calidad (pasa a Score/Flink)
        "dist_label": payload.get("dist_label"),
        "rate": payload.get("rate"),
        "run_id": payload.get("run_id")
    }
    if producer:
        await producer.send_and_wait(TOPIC_OUT, out_msg)
        logger.info("Publicada respuesta para %s", interaction_id)
    else:
        logger.error("Producer no disponible, no se pudo enviar la respuesta")

async def consume_loop():
    global consumer
    consumer = AIOKafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="llm-service",
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )
    await consumer.start()
    logger.info(f"Consumidor escuchando en {TOPIC_IN}")
    try:
        async for msg in consumer:
            asyncio.create_task(process_question(msg.value))
    finally:
        await consumer.stop()

@app.on_event("startup")
async def startup():
    ok = await connect_to_kafka_with_retry()
    if not ok:
        raise RuntimeError("Kafka no disponible")
    global consumption_task
    consumption_task = asyncio.create_task(consume_loop())

@app.on_event("shutdown")
async def shutdown():
    global producer, consumer, consumption_task
    if consumption_task:
        consumption_task.cancel()
    if consumer:
        await consumer.stop()
    if producer:
        await producer.stop()

@app.get("/health")
async def health():
    return {
        "ok": kafka_ready.is_set(),
        "topic_in": TOPIC_IN,
        "topic_out": TOPIC_OUT,
        "topic_err": TOPIC_ERR,
        "ollama": OLLAMA,
        "model": MODEL,
        "concurrency": LLM_CONCURRENCY
    }
