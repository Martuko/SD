# app.py  (Servicio LLM)
from fastapi import FastAPI
import os, time, asyncio, httpx, json, logging
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from datetime import datetime

# Configuración de logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("llm")

# FastAPI app
app = FastAPI(title="LLM Service")

# === Configuración Kafka ===
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_IN = os.getenv("TOPIC_LLM", "questions.llm")
TOPIC_OUT = os.getenv("TOPIC_ANSWERS", "questions.answers")

# === Configuración Ollama ===
OLLAMA = os.getenv("OLLAMA_HOST", "http://ollama:11434")
MODEL  = os.getenv("LLM_MODEL", "llama3.1:8b-instruct-q4_K_M")
MAX_TOK = int(os.getenv("LLM_MAX_TOKENS", "512"))
TEMP    = float(os.getenv("LLM_TEMPERATURE", "0.2"))
SYS_PROMPT = os.getenv("SYS_PROMPT", "Eres un asistente conciso y factual. Responde en español.")

# Variables globales
producer: AIOKafkaProducer | None = None
consumer: AIOKafkaConsumer | None = None
kafka_ready = asyncio.Event()
consumption_task = None


# --- Conexión con reintentos ---
async def connect_to_kafka_with_retry():
    """Conectar a Kafka con reintentos"""
    global producer
    max_retries = 15
    retry_delay = 5

    for attempt in range(max_retries):
        try:
            logger.info(f" Intentando conectar a Kafka... Intento {attempt + 1}/{max_retries}")

            producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            await producer.start()

            # Verificar bootstrap
            await producer.client.bootstrap()
            logger.info(" ✅ Conexión a Kafka establecida exitosamente")
            kafka_ready.set()
            return True

        except Exception as e:
            logger.warning(f" ❌ Intento {attempt + 1} falló: {e}")

            if producer:
                await producer.stop()
                producer = None

            if attempt < max_retries - 1:
                logger.info(f"Reintentando en {retry_delay} segundos...")
                await asyncio.sleep(retry_delay)
            else:
                logger.error(" ❌ No se pudo conectar a Kafka después de todos los intentos")
                return False


# --- Procesar una pregunta con Ollama ---
async def process_question(payload: dict):
    interaction_id = payload.get("id")
    question = payload.get("question", "")
    question_id = payload.get("question_id")
    ts_start = time.perf_counter()

    try:
        async with httpx.AsyncClient(timeout=120) as cli:
            r = await cli.post(f"{OLLAMA}/api/generate", json={
                "model": MODEL,
                "prompt": f"<s>[INST] {question} [/INST]",
                "system": SYS_PROMPT,
                "options": {"temperature": TEMP, "num_predict": MAX_TOK},
                "stream": False
            })
        r.raise_for_status()
        data = r.json()
        llm_answer = data.get("response", "")
    except Exception as e:
        logger.error(" Error llamando a Ollama: %s", e)
        llm_answer = ""

    latency_ms = int((time.perf_counter() - ts_start) * 1000)
    out_msg = {
        "id": interaction_id,
        "question_id": question_id,
        "question": question,
        "llm_answer": llm_answer,
        "cached": False,
        "latency_ms": latency_ms,
        "model": MODEL,
        "ts_answered": datetime.utcnow().isoformat()
    }

    await producer.send_and_wait(TOPIC_OUT, out_msg)
    logger.info(" 📤 Publicada respuesta para %s", interaction_id)


# --- Loop de consumo ---
async def consume_loop():
    global consumer
    consumer = AIOKafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="llm-service",
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )
    await consumer.start()
    logger.info(f" 🟢 Consumidor escuchando en {TOPIC_IN}")

    try:
        async for msg in consumer:
            payload = msg.value
            asyncio.create_task(process_question(payload))
    finally:
        await consumer.stop()


# --- Eventos FastAPI ---
@app.on_event("startup")
async def startup():
    ok = await connect_to_kafka_with_retry()
    if not ok:
        raise RuntimeError("Kafka no disponible después de varios intentos")

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


# --- Health check ---
@app.get("/health")
async def health():
    return {
        "ok": kafka_ready.is_set(),
        "topic_in": TOPIC_IN,
        "topic_out": TOPIC_OUT,
        "ollama": OLLAMA,
        "model": MODEL
    }
