FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY wait-for-it.sh .
RUN chmod +x wait-for-it.sh
EXPOSE 8000

CMD ["sh", "./wait-for-it.sh", "kafka:9092", "--","uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]

