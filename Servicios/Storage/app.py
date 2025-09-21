# Servicios/Storage/app.py
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import os, asyncpg

DB_URL = os.getenv("DATABASE_URL", "postgresql://app:app@db:5432/qa")
app = FastAPI(title="Storage API")

class SaveRecordIn(BaseModel):
    question_id: int
    llm_answer: str
    human_answer: str
    score: float

class QuestionOut(BaseModel):
    id: int
    title: str
    body: str
    topic: str | None = None
    best_answer: str

@app.on_event("startup")
async def startup():
    import asyncio
    max_retries = int(os.getenv("DB_INIT_MAX_RETRIES", "60"))
    delay = float(os.getenv("DB_INIT_RETRY_DELAY", "1.0"))  # en segundos

    # Intentar crear el pool con reintentos y sin SSL
    last_err = None
    for i in range(1, max_retries + 1):
        try:
            app.state.pool = await asyncpg.create_pool(dsn=DB_URL, timeout=5.0, ssl=False)
            break
        except Exception as e:
            last_err = e
            await asyncio.sleep(delay)
    if not getattr(app.state, "pool", None):
        # Si no se logró, re-lanzar el último error para ver el stack
        raise last_err

    # Migraciones mínimas
    async with app.state.pool.acquire() as con:
        await con.execute("""
        CREATE TABLE IF NOT EXISTS questions (
          id BIGSERIAL PRIMARY KEY,
          title  TEXT NOT NULL,
          body   TEXT NOT NULL,
          topic  TEXT,
          best_answer TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS answers (
          id BIGSERIAL PRIMARY KEY,
          question_id BIGINT NOT NULL REFERENCES questions(id) ON DELETE CASCADE,
          body TEXT NOT NULL,
          author_type TEXT NOT NULL CHECK (author_type IN ('human','llm')),
          source TEXT NOT NULL,
          created_at TIMESTAMPTZ DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS qa_stats (
          question_id BIGINT PRIMARY KEY REFERENCES questions(id) ON DELETE CASCADE,
          times_asked INT NOT NULL DEFAULT 0,
          last_score DOUBLE PRECISION
        );
        CREATE INDEX IF NOT EXISTS idx_answers_qid ON answers(question_id);
        """)


@app.get("/health")
async def health():
    async with app.state.pool.acquire() as con:
        await con.execute("SELECT 1;")
    return {"ok": True}

@app.get("/questions/random", response_model=list[QuestionOut])
async def random_questions(limit: int = Query(default=1, ge=1, le=100)):
    async with app.state.pool.acquire() as con:
        rows = await con.fetch(
            "SELECT id, title, body, topic, best_answer FROM questions ORDER BY random() LIMIT $1",
            limit
        )
    return [QuestionOut(**dict(r)) for r in rows]

@app.get("/questions/{qid}", response_model=QuestionOut)
async def get_question(qid: int):
    async with app.state.pool.acquire() as con:
        row = await con.fetchrow(
            "SELECT id, title, body, topic, best_answer FROM questions WHERE id=$1", qid
        )
    if not row:
        raise HTTPException(404, "Question not found")
    return QuestionOut(**dict(row))

@app.post("/records/save")
async def save_record(item: SaveRecordIn):
    async with app.state.pool.acquire() as con:
        await con.execute(
            "INSERT INTO answers(question_id, body, author_type, source) VALUES($1,$2,'llm','generated')",
            item.question_id, item.llm_answer
        )
        await con.execute("""
            INSERT INTO qa_stats(question_id, times_asked, last_score)
            VALUES($1,1,$2)
            ON CONFLICT (question_id)
            DO UPDATE SET times_asked=qa_stats.times_asked+1, last_score=$2
        """, item.question_id, item.score)
    return {"ok": True}

class SeedStatus(BaseModel):
    inserted: int

@app.post("/seed", response_model=SeedStatus)
async def seed_from_csv(path: str = "/data/test.csv", limit: int | None = None):
    import csv
    inserted = 0
    async with app.state.pool.acquire() as con:
        existing = await con.fetchval("SELECT COUNT(*) FROM questions")
        #if existing and existing >= 10000:
        #   return SeedStatus(inserted=0)
        async with con.transaction():
            with open(path, newline='', encoding="utf-8") as f:
                rdr = csv.reader(f)
                for row in rdr:
                    if len(row) < 4:
                        continue
                    topic, title, body, best = row[0], row[1], row[2], row[3]
                    qid = await con.fetchval(
                        "INSERT INTO questions(title, body, topic, best_answer) VALUES($1,$2,$3,$4) RETURNING id",
                        title, body, topic, best
                    )
                    await con.execute(
                        "INSERT INTO answers(question_id, body, author_type, source) VALUES($1,$2,'human','import')",
                        qid, best
                    )
                    inserted += 1
                    total = existing + inserted

                    if inserted % 500 == 0: 
                        print(f"{existing + inserted}preguntas totales insertadas...")

                    
    return SeedStatus(inserted=inserted)
