from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import time
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

app = FastAPI(title="Score Service")

embedder = SentenceTransformer("all-MiniLM-L6-v2")

LLM_HOST = "http://llm:8000"  

class ScoreRequest(BaseModel):
    question: str
    reference_answer: str

class ScoreResponse(BaseModel):
    question: str
    llm_answer: str
    reference_answer: str
    score: float
    latency_ms: int

@app.post("/score", response_model=ScoreResponse)
async def score(req: ScoreRequest):
    start_time = time.time()

    try:
        async with httpx.AsyncClient() as client:
            r = await client.post(
                f"{LLM_HOST}/generate",
                json={"question": req.question},
                timeout=60.0
            )
            r.raise_for_status()
            llm_data = r.json()
            llm_answer = llm_data.get("answer", "")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error contacting LLM: {str(e)}")

    try:
        embeddings = embedder.encode([req.reference_answer, llm_answer])
        score_val = cosine_similarity([embeddings[0]], [embeddings[1]])[0][0]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error computing similarity: {str(e)}")

    latency_ms = int((time.time() - start_time) * 1000)

    return ScoreResponse(
        question=req.question,
        llm_answer=llm_answer,
        reference_answer=req.reference_answer,
        score=float(score_val),
        latency_ms=latency_ms
    )
