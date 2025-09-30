# Servicios/Traffic/app.py
from fastapi import FastAPI
import asyncio, os, random, time, uuid, json, csv
from datetime import datetime
from aiokafka import AIOKafkaProducer

# === Configuración ===
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_REQUESTS   = os.getenv("TOPIC_REQUESTS", "questions.requests")
CSV_PATH         = os.getenv("CSV_PATH", "/data/test.csv")

DIST        = os.getenv("DIST", "poisson")
RATE        = float(os.getenv("RATE", "5"))
DURATION    = int(os.getenv("DURATION", "3600"))
SAMPLE_LIMIT = int(os.getenv("SAMPLE_LIMIT", "0"))  # 0 = sin límite (por tiempo)
CONCURRENCY = int(os.getenv("CONCURRENCY", "4"))
SEED        = int(os.getenv("SEED", "42"))
RUN_ID      = os.getenv("EXPERIMENT_ID", "run1")
AUTOSTART   = os.getenv("AUTOSTART", "1") == "1"

# Para experimentos: sub-pool repetible
REPEAT_POOL_SIZE = int(os.getenv("REPEAT_POOL_SIZE", "0"))  # 0 = usar todo

random.seed(SEED)
app = FastAPI(title="Traffic Service")

QUESTIONS: list[dict] = []
producer: AIOKafkaProducer | None = None
RUN_TASK: asyncio.Task | None = None
IS_RUNNING = False

# --- Nuevo: contador global de envíos + lock ---
SENT_TOTAL = 0
SENT_LOCK = asyncio.Lock()


# === Helpers ===
def load_csv(path: str) -> list[dict]:
    rows = []
    try:
        with open(path, newline='', encoding='utf-8') as f:
            rdr = csv.reader(f, delimiter=",", quotechar='"')
            for i, r in enumerate(rdr, start=1):
                if len(r) >= 4:
                    # 0: class_id, 1: title, 2: content, 3: best_answer
                    question = (r[2].strip() or r[1].strip())
                    reference = r[3].strip()
                    rows.append({
                        "question_id": i,
                        "question": question,
                        "reference_answer": reference
                    })
        return rows
    except Exception as e:
        print(f"[ERROR] load_csv({path}): {e}")
        return []


async def should_send_more() -> bool:
    """Controla el límite global SAMPLE_LIMIT (no por worker)."""
    global SENT_TOTAL
    async with SENT_LOCK:
        if SAMPLE_LIMIT > 0 and SENT_TOTAL >= SAMPLE_LIMIT:
            return False
        SENT_TOTAL += 1
        return True


async def send_one():
    q = random.choice(QUESTIONS)
    msg = {
        "id": str(uuid.uuid4()),
        "question_id": q.get("question_id"),
        "question": q.get("question"),
        "reference_answer": q.get("reference_answer"),
        "dist_label": DIST,
        "rate": RATE,
        "run_id": RUN_ID,
        "ts_generated": datetime.utcnow().isoformat()
    }
    await producer.send_and_wait(TOPIC_REQUESTS, msg)


async def send_until(stop_at: float):
    """Bucle por worker respetando límite global."""
    while True:
        # Límite global por cantidad
        if SAMPLE_LIMIT > 0:
            ok = await should_send_more()
            if not ok:
                break
        else:
            # Límite por tiempo (si no hay sample_limit)
            if time.time() >= stop_at:
                break

        # Espera según distribución
        if DIST == "poisson":
            await asyncio.sleep(random.expovariate(RATE))
        elif DIST == "uniform":
            await asyncio.sleep(random.uniform(0, 2.0 / RATE))
        else:
            raise ValueError(f"Distribución {DIST} no soportada")

        await send_one()


# === Ciclo principal ===
async def run_generator():
    global IS_RUNNING, SENT_TOTAL
    IS_RUNNING = True
    SENT_TOTAL = 0  # reset
    stop_at = time.time() + DURATION
    tasks = [asyncio.create_task(send_until(stop_at)) for _ in range(CONCURRENCY)]
    try:
        await asyncio.gather(*tasks)
    finally:
        IS_RUNNING = False


# === Startup/Shutdown ===
@app.on_event("startup")
async def startup():
    global QUESTIONS, producer, RUN_TASK, IS_RUNNING
    QUESTIONS = load_csv(CSV_PATH)
    print(f"Traffic cargó {len(QUESTIONS)} preguntas de {CSV_PATH}")
    if REPEAT_POOL_SIZE and REPEAT_POOL_SIZE < len(QUESTIONS):
        QUESTIONS = QUESTIONS[:REPEAT_POOL_SIZE]
        print(f"Traffic limitará el pool a {len(QUESTIONS)} filas para observar cache hits")

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    await producer.start()

    if AUTOSTART and RUN_TASK is None and not IS_RUNNING:
        RUN_TASK = asyncio.create_task(run_generator())


@app.on_event("shutdown")
async def shutdown():
    global RUN_TASK
    if RUN_TASK:
        RUN_TASK.cancel()
        try:
            await RUN_TASK
        except asyncio.CancelledError:
            pass
        RUN_TASK = None
    if producer:
        await producer.stop()


# === Endpoints ===
@app.post("/start")
async def start_traffic(cfg: dict = {}):
    """Permite override de CSV, muestreo y parámetros, y arranca el generador."""
    global RUN_TASK, DIST, RATE, CONCURRENCY, DURATION, RUN_ID, SAMPLE_LIMIT, IS_RUNNING, QUESTIONS, REPEAT_POOL_SIZE, SENT_TOTAL

    if IS_RUNNING:
        return {"running": True, "message": "Traffic generator already running."}

    # Overrides básicos
    DIST = cfg.get("distribution", DIST)
    RATE = float(cfg.get("rate", RATE))
    CONCURRENCY = int(cfg.get("concurrency", CONCURRENCY))
    DURATION = int(cfg.get("duration", DURATION))
    SAMPLE_LIMIT = int(cfg.get("sample_limit", SAMPLE_LIMIT))
    RUN_ID = cfg.get("run_id", RUN_ID)

    # Opcional: recargar dataset
    csv_override = cfg.get("csv_path")
    if csv_override:
        new_rows = load_csv(csv_override)
        if not new_rows:
            return {"running": False, "message": f"csv_path '{csv_override}' vacío o inválido"}
        QUESTIONS = new_rows
        print(f"[start] Dataset recargado desde {csv_override} con {len(QUESTIONS)} filas")

    # Muestreo (POOL_FRACTION → POOL_SIZE)
    POOL_SIZE      = int(cfg.get("POOL_SIZE", 0))
    POOL_FRACTION  = float(cfg.get("POOL_FRACTION", 0))
    REPEAT_POOL_SIZE_LOCAL = int(cfg.get("REPEAT_POOL_SIZE", REPEAT_POOL_SIZE))

    if POOL_FRACTION > 0 and POOL_FRACTION < 1:
        POOL_SIZE = max(1, int(len(QUESTIONS) * POOL_FRACTION))
    if POOL_SIZE and POOL_SIZE < len(QUESTIONS):
        seed_eff = SEED ^ (abs(hash(RUN_ID)) & 0xffffffff)
        rng = random.Random(seed_eff)
        QUESTIONS = rng.sample(QUESTIONS, POOL_SIZE)
        print(f"[start] Pool muestreado a {len(QUESTIONS)} filas con seed derivada de run_id")

    if REPEAT_POOL_SIZE_LOCAL and REPEAT_POOL_SIZE_LOCAL < len(QUESTIONS):
        QUESTIONS = QUESTIONS[:REPEAT_POOL_SIZE_LOCAL]
        print(f"[start] Sub-pool limitado a {len(QUESTIONS)} filas para aumentar hits")

    SENT_TOTAL = 0
    RUN_TASK = asyncio.create_task(run_generator())
    return {
        "running": True,
        "dist": DIST,
        "rate": RATE,
        "concurrency": CONCURRENCY,
        "duration": DURATION,
        "sample_limit": SAMPLE_LIMIT,
        "run_id": RUN_ID,
        "csv_rows": len(QUESTIONS)
    }


@app.post("/ask")
async def ask(payload: dict):
    """Envío ad-hoc de una sola pregunta (útil para pruebas)."""
    if not producer:
        return {"ok": False, "error": "Producer not initialized"}
    payload.setdefault("id", str(uuid.uuid4()))
    payload.setdefault("dist_label", DIST)
    payload.setdefault("rate", RATE)
    payload.setdefault("run_id", RUN_ID)
    payload.setdefault("ts_generated", datetime.utcnow().isoformat())
    await producer.send_and_wait(TOPIC_REQUESTS, payload)
    return {"ok": True, "sent": payload}


@app.post("/stop")
async def stop_traffic():
    """Detiene el generador (pueden quedar mensajes en cola en Kafka)."""
    global RUN_TASK, IS_RUNNING
    if RUN_TASK and not RUN_TASK.done():
        RUN_TASK.cancel()
        try:
            await RUN_TASK
        except asyncio.CancelledError:
            pass
        RUN_TASK = None
        IS_RUNNING = False
        return {"running": False, "message": "Traffic generation stopped."}
    return {"running": IS_RUNNING, "message": "Traffic generator not running."}


@app.get("/status")
def status():
    return {
        "running": IS_RUNNING,
        "sent_total": SENT_TOTAL,
        "distribution": DIST,
        "rate": RATE,
        "concurrency": CONCURRENCY,
        "sample_limit": SAMPLE_LIMIT,
        "autostart": AUTOSTART,
        "csv_rows": len(QUESTIONS)
    }


@app.get("/health")
def health():
    return {"ok": True, "topic_requests": TOPIC_REQUESTS, "bootstrap": KAFKA_BOOTSTRAP}
