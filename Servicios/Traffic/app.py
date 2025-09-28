# Servicios/Traffic/app.py
from fastapi import FastAPI
import asyncio, os, random, time, uuid, json, csv
from datetime import datetime
from aiokafka import AIOKafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_REQUESTS = os.getenv("TOPIC_REQUESTS", "questions.requests")
CSV_PATH = os.getenv("CSV_PATH", "/data/test.csv")
DIST = os.getenv("DIST", "poisson")
RATE = float(os.getenv("RATE", "5"))
DURATION = int(os.getenv("DURATION", "3600"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "4"))
SEED = int(os.getenv("SEED", "42"))
RUN_ID = os.getenv("EXPERIMENT_ID", "run1")
AUTOSTART = os.getenv("AUTOSTART", "1") == "1"

random.seed(SEED)
app = FastAPI(title="Traffic Service")

QUESTIONS = []
producer = None
RUN_TASK = None

def load_csv(path):
    rows = []
    try:
        with open(path, newline='', encoding='utf-8') as f:
            rdr = csv.reader(f, delimiter=",", quotechar='"')
            for i, r in enumerate(rdr, start=1):
                if len(r) >= 4:
                    # usa content si no está vacío, si no, el título
                    question = r[2].strip() if r[2].strip() else r[1].strip()
                    reference = r[3].strip()
                    rows.append({
                        "question_id": i,
                        "question": question,
                        "reference_answer": reference
                    })
        return rows
    except Exception as e:
        print(f"[ERROR] load_csv: {e}")
        return []


@app.on_event("startup")
async def startup():
    global QUESTIONS, producer, RUN_TASK
    QUESTIONS = load_csv(CSV_PATH)
    print(f"Traffic cargó {len(QUESTIONS)} preguntas de {CSV_PATH}")
    for q in QUESTIONS[:5]:
        print(f"Ejemplo: {q}")
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    await producer.start()
    if AUTOSTART and RUN_TASK is None:
        RUN_TASK = asyncio.create_task(run_generator())

@app.on_event("shutdown")
async def shutdown():
    global RUN_TASK
    if RUN_TASK:
        RUN_TASK.cancel()
    if producer:
        await producer.stop()

async def send_one(i:int):
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

async def loop_poisson(stop_at: float):
    i = 0
    while time.time() < stop_at:
        await asyncio.sleep(random.expovariate(RATE))
        await send_one(i)
        i += 1

async def loop_uniform(stop_at: float):
    i = 0
    while time.time() < stop_at:
        await asyncio.sleep(random.uniform(0, 2.0 / RATE))
        await send_one(i)
        i += 1

async def run_generator():
    stop_at = time.time() + DURATION
    if DIST == "poisson":
        tasks = [asyncio.create_task(loop_poisson(stop_at)) for _ in range(CONCURRENCY)]
    elif DIST == "uniform":
        tasks = [asyncio.create_task(loop_uniform(stop_at)) for _ in range(CONCURRENCY)]
    else:
        raise ValueError(f"Distribución {DIST} no soportada")
    await asyncio.gather(*tasks)

@app.post("/start")
async def start_traffic(cfg: dict = {}):
    global RUN_TASK, DIST, RATE, CONCURRENCY, DURATION, RUN_ID
    if RUN_TASK and not RUN_TASK.done():
        return {"running": True}

    DIST = cfg.get("distribution", DIST)
    RATE = float(cfg.get("rate", RATE))
    CONCURRENCY = int(cfg.get("concurrency", CONCURRENCY))
    DURATION = int(cfg.get("duration", DURATION))
    RUN_ID = cfg.get("run_id", RUN_ID)

    RUN_TASK = asyncio.create_task(run_generator())
    return {"running": True, "dist": DIST, "rate": RATE, "concurrency": CONCURRENCY, "duration": DURATION, "run_id": RUN_ID}


@app.post("/ask")
async def ask(payload: dict):
    global producer
    if not producer:
        return {"ok": False, "error": "Producer not initialized"}
    # Ensure experiment metadata present
    payload.setdefault("dist_label", DIST)
    payload.setdefault("rate", RATE)
    payload.setdefault("run_id", RUN_ID)
    payload.setdefault("ts_generated", datetime.utcnow().isoformat())
    await producer.send_and_wait(TOPIC_REQUESTS, payload)
    return {"ok": True, "sent": payload}

@app.get("/health")
def health():
    return {"ok": True, "topic_requests": TOPIC_REQUESTS, "bootstrap": KAFKA_BOOTSTRAP}
