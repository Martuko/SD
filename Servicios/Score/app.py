# Servicios/Score/app.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx, os, time
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

app = FastAPI(title="Score Service")

# Embeddings
embedder = SentenceTransformer("all-MiniLM-L6-v2")

# Hosts de otros servicios
LLM_HOST = os.getenv("LLM_HOST", "http://llm:8000")
STORAGE_HOST = os.getenv("STORAGE_HOST", "http://storage:8003")

class ScoreRequest(BaseModel):
    question: str
    llm_answer: str

class ScoreResponse(BaseModel):
    question: str
    llm_answer: str
    reference_answer: str
    score: float
    latency_ms: int
    model: str | None = None

@app.post("/score", response_model=ScoreResponse)
async def score(req: ScoreRequest):
    t0 = time.time()

    # 1) Buscar reference_answer en Storage
    try:
        async with httpx.AsyncClient(timeout=30) as cli:
            r = await cli.get(f"{STORAGE_HOST}/questions/random?limit=1")
            r.raise_for_status()
            q = r.json()[0]  # agarramos una del dataset
            reference_answer = q["best_answer"]
    except Exception as e:
        raise HTTPException(500, f"Error obteniendo reference_answer: {e}")

    # 2) Calcular embeddings
    try:
        emb = embedder.encode([reference_answer, req.llm_answer])
        score_val = cosine_similarity([emb[0]], [emb[1]])[0][0]
    except Exception as e:
        raise HTTPException(500, f"Error calculando embeddings: {e}")

    latency_ms = int((time.time() - t0) * 1000)

    return ScoreResponse(
        question=req.question,
        llm_answer=req.llm_answer,
        reference_answer=reference_answer,
        score=float(score_val),
        latency_ms=latency_ms,
        model="all-MiniLM-L6-v2"
    )
