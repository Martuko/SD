# traffic.py
from fastapi import FastAPI
import asyncio, os, random, time, uuid, json, csv
from datetime import datetime
from aiokafka import AIOKafkaProducer

# Config
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_REQUESTS = os.getenv("TOPIC_REQUESTS", "questions.requests")
CSV_PATH = os.getenv("CSV_PATH", "/data/train.csv")  # csv con filas: topic,title,body,best_answer OR id,title,question,answer
DIST = os.getenv("DIST", "poisson")
RATE = float(os.getenv("RATE", "5"))
DURATION = int(os.getenv("DURATION", "3600"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "4"))
SEED = int(os.getenv("SEED", "42"))
AUTOSTART = os.getenv("AUTOSTART", "1") == "1"

random.seed(SEED)
app = FastAPI(title="Traffic Service")

producer: AIOKafkaProducer | None = None
QUESTIONS = []
RUN_TASK = None

def load_csv(path):
    rows = []
    try:
        with open(path, newline='', encoding='utf-8') as f:
            rdr = csv.reader(f)
            for r in rdr:
                # adaptamos: si CSV tiene 4 campos: topic,title,body,best_answer
                if len(r) >= 4:
                    # generate an int id if not present
                    rows.append({
                        "question_id": None,
                        "question": r[2] or r[1],
                        "reference_answer": r[3]
                    })
                elif len(r) == 3:
                    rows.append({
                        "question_id": None,
                        "question": r[1],
                        "reference_answer": r[2]
                    })
                else:
                    continue
    except Exception:
        # fallback mínimo
        rows = [
            {"question_id": 1, "question": "¿Qué es un sistema distribuido?", "reference_answer": "Sistema con múltiples nodos."},
            {"question_id": 2, "question": "¿Qué es LRU?", "reference_answer": "Política que descarta el menos recientemente usado."},
        ]
    return rows

@app.on_event("startup")
async def startup():
    global producer, QUESTIONS, RUN_TASK
    QUESTIONS = load_csv(CSV_PATH)
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    await producer.start()
    if AUTOSTART and RUN_TASK is None:
        RUN_TASK = asyncio.create_task(run_generator())

@app.on_event("shutdown")
async def shutdown():
    global producer, RUN_TASK
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
        "ts_generated": datetime.utcnow().isoformat()
    }
    await producer.send_and_wait(TOPIC_REQUESTS, msg)

async def loop_poisson(stop_at: float):
    i = 0
    while time.time() < stop_at:
        await asyncio.sleep(random.expovariate(RATE))
        await send_one(i)
        i += 1

async def run_generator():
    stop_at = time.time() + DURATION
    tasks = [asyncio.create_task(loop_poisson(stop_at)) for _ in range(CONCURRENCY)]
    await asyncio.gather(*tasks)

@app.post("/start")
async def start_traffic():
    global RUN_TASK
    if RUN_TASK and not RUN_TASK.done():
        return {"running": True}
    RUN_TASK = asyncio.create_task(run_generator())
    return {"running": True}

@app.get("/health")
def health():
    return {"ok": True, "topic_requests": TOPIC_REQUESTS, "bootstrap": KAFKA_BOOTSTRAP}
