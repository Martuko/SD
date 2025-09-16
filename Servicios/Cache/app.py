from fastapi import FastAPI
import httpx, os, redis, time

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
LLM_HOST = os.getenv("LLM_HOST", "http://llm:8000")
SCORE_HOST = os.getenv("SCORE_HOST", "http://score:8002")

app = FastAPI()
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

@app.post("/ask")
async def ask(payload: dict):
    question = payload["question"]

    # 1. Revisar si está en cache
    cached_answer = r.get(question)
    if cached_answer:
        return {"answer": cached_answer, "cached": True}

    # 2. Si no está en cache, consultar al LLM
    t0 = time.perf_counter()
    async with httpx.AsyncClient(timeout=60) as cli:
        res = await cli.post(f"{LLM_HOST}/generate", json={"question": question})
    res.raise_for_status()
    llm_data = res.json()
    llm_answer = llm_data.get("answer", "")

    # 3. Enviar pregunta + respuesta al Score
    async with httpx.AsyncClient(timeout=60) as cli:
        res = await cli.post(f"{SCORE_HOST}/score", json={
            "question": question,
            "llm_answer": llm_answer
        })
    res.raise_for_status()
    score_data = res.json()

    # 4. Decidir mejor respuesta
    final_answer = llm_answer
    if score_data["score"] < 0.6:  # umbral configurable
        final_answer = score_data["reference_answer"]

    # 5. Guardar en Redis
    r.set(question, final_answer)

    latency_ms = int((time.perf_counter() - t0) * 1000)

    return {
        "answer": final_answer,
        "cached": False,
        "latency_ms": latency_ms,
        "score_info": score_data
    }
