# Servicios/ErrorHandler/Dockerfile
FROM python:3.11-slim
RUN pip install --no-cache-dir aiokafka
WORKDIR /app
COPY app.py /app/app.py
CMD ["python", "app.py"]
