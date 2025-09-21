from fastapi import FastAPI
import httpx, os, asyncio, time, random, uuid
from datetime import datetime

# Cache (gateway/orchestrator)
TARGET_URL = os.getenv("TARGET_URL", "http://cache:8001/ask")

# Storage (persistencia)
STORAGE_HOST = os.getenv("STORAGE_HOST", "http://storage:8003")

# Fuente de preguntas
SOURCE = os.getenv("SOURCE", "local")           # local | db
SAMPLE_LIMIT = int(os.getenv("SAMPLE_LIMIT", "500"))

# Parámetros de tráfico
DIST = os.getenv("DIST", "poisson")             # poisson | burst
RATE = float(os.getenv("RATE", "5"))            # req/s (poisson) o tasa en ON (burst)
DURATION = int(os.getenv("DURATION", "60"))     # segundos
CONCURRENCY = int(os.getenv("CONCURRENCY", "4"))
SEED = int(os.getenv("SEED", "42"))
BURST_ON_MS = int(os.getenv("BURST_ON_MS", "5000"))
BURST_OFF_MS = int(os.getenv("BURST_OFF_MS", "5000"))
AUTOSTART = os.getenv("AUTOSTART", "1") == "1"

random.seed(SEED)

app = FastAPI(title="Traffic Service")

QUESTIONS = []
RUN_TASK = None


async def load_questions():
    """Carga preguntas desde Storage o usa un set local mínimo."""
    global QUESTIONS
    if SOURCE == "db":
        url = f"{STORAGE_HOST}/questions/sample?limit={SAMPLE_LIMIT}"
        async with httpx.AsyncClient(timeout=30) as cli:
            r = await cli.get(url)
            r.raise_for_status()
            QUESTIONS = r.json()
    else:
        # Mínimas locales si aún no hay storage cargado
        QUESTIONS = [
            {"id": 1, "title": "¿Qué es un sistema distribuido?", "body": "Explica brevemente.", "best_answer": "Sistema con múltiples nodos coordinados."},
            {"id": 2, "title": "¿Qué es caché LRU?", "body": "Resumen corto.", "best_answer": "Política que descarta el menos recientemente usado."},
            {"id": 3, "title": "¿Qué es consistencia eventual?", "body": "Definición simple.", "best_answer": "Modelo donde las réplicas convergen con el tiempo."},
        ]


def pick_question():
    if not QUESTIONS:
        return {"id": 0, "title": None, "body": "Pregunta de ejemplo", "best_answer": ""}
    return random.choice(QUESTIONS)


async def send_one(cli: httpx.AsyncClient):
    """Envía una consulta a Cache/ask y persiste el resultado en Storage."""
    q = pick_question()
    question_id = q.get("id", 0)
    question_text = q.get("title") or q.get("body") or ""
    reference_from_ds = q.get("best_answer") or ""

    # ID de interacción (útil para correlacionar)
    interaction_id = str(uuid.uuid4())

    # 1) Disparar al Cache (/ask) → pipeline cache → (hit | miss→llm→score)
    req_payload = {
        "id": interaction_id,
        "question": question_text,
        "ts_generated": datetime.utcnow().isoformat()
    }
    r = await cli.post(TARGET_URL, json=req_payload, timeout=60)
    r.raise_for_status()
    data = r.json()

    # 2) Extraer resultados y normalizar llm/ref/final
    score_info = data.get("score_info", {}) or {}
    llm_ans = score_info.get("llm_answer") or data.get("llm_answer") or ""
    ref_ans = score_info.get("reference_answer") or reference_from_ds or ""
    final_ans = data.get("answer", "")
    model = score_info.get("model") or data.get("model") or ""

    # 3) Persistir en Storage (/interactions) con todos los campos requeridos
    store_payload = {
        "id": interaction_id,               # opcional; Storage genera si no llega
        "question_id": question_id,
        "question": question_text,
        "reference_answer": ref_ans,
        "llm_answer": llm_ans,
        "final_answer": final_ans,
        "cached": bool(data.get("cached", False)),
        "latency_ms": int(data.get("latency_ms", 0)),
        "score": float(score_info.get("score", 0.0)) if score_info.get("score") is not None else None,
        "model": model,
        "dist_label": DIST,
        "rate": RATE
    }
    try:
        sr = await cli.post(f"{STORAGE_HOST}/interactions", json=store_payload, timeout=20)
        sr.raise_for_status()
    except Exception:
        # Mantener minimal: sin logs verbosos
        pass


async def loop_poisson(stop_at: float):
    async with httpx.AsyncClient() as cli:
        while time.time() < stop_at:
            await asyncio.sleep(random.expovariate(RATE))
            await send_one(cli)


async def loop_burst(stop_at: float):
    async with httpx.AsyncClient() as cli:
        while time.time() < stop_at:
            t_on = time.time() + BURST_ON_MS / 1000
            while time.time() < t_on and time.time() < stop_at:
                await send_one(cli)
                await asyncio.sleep(1.0 / max(RATE, 1e-6))
            await asyncio.sleep(BURST_OFF_MS / 1000)


async def run_generator():
    await load_questions()
    stop_at = time.time() + DURATION
    workers = []
    for _ in range(CONCURRENCY):
        workers.append(asyncio.create_task(loop_burst(stop_at) if DIST == "burst" else loop_poisson(stop_at)))
    await asyncio.gather(*workers)


@app.on_event("startup")
async def on_start():
    global RUN_TASK
    if AUTOSTART and RUN_TASK is None:
        RUN_TASK = asyncio.create_task(run_generator())


@app.get("/health")
def health():
    return {
        "ok": True, "target": TARGET_URL, "dist": DIST, "rate": RATE,
        "duration": DURATION, "concurrency": CONCURRENCY
    }


@app.post("/start")
async def start():
    global RUN_TASK
    if RUN_TASK and not RUN_TASK.done():
        return {"running": True}
    RUN_TASK = asyncio.create_task(run_generator())
    return {"running": True}
