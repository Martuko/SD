# Servicios/ErrorHandler/app.py
import os, json, asyncio, random
from datetime import datetime
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

BOOT = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_IN  = os.getenv("TOPIC_ERRORS", "questions.errors")
TOPIC_OUT = os.getenv("TOPIC_REQ",    "questions.requests")
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "3"))
RATE_LIMIT_DELAY = int(os.getenv("RATE_LIMIT_DELAY", "60"))

async def reenqueue(producer, payload: dict, attempt: int):
    new_msg = payload["question_payload"]
    new_msg["attempt"] = attempt + 1
    new_msg["ts_retried"] = datetime.utcnow().isoformat()
    await producer.send_and_wait(TOPIC_OUT, new_msg)

async def handle(msg, producer):
    payload = msg.value
    err = (payload.get("error_type") or "").upper()
    attempt = int(payload.get("attempt", 0))

    if attempt >= MAX_ATTEMPTS:
        # Aquí podrías publicar a una DLQ si lo deseas
        return

    if err in {"OVERLOAD", "TIMEOUT"}:
        backoff = min(2 ** attempt, 32) + random.random()  # exponencial + jitter
        await asyncio.sleep(backoff)
        await reenqueue(producer, payload, attempt)

    elif err == "RATE_LIMIT":
        await asyncio.sleep(RATE_LIMIT_DELAY)
        await reenqueue(producer, payload, attempt)

    else:
        # Errores no recuperables (p.ej., BAD_REQUEST): no reintentar
        return

async def main():
    consumer = AIOKafkaConsumer(
        TOPIC_IN, bootstrap_servers=BOOT,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="error-handler"
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOT, value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    await producer.start(); await consumer.start()
    try:
        async for msg in consumer:
            await handle(msg, producer)
    finally:
        await consumer.stop(); await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())
