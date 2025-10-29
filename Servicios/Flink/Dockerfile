# Servicios/Flink/Dockerfile
FROM flink:1.18.0-java11

# ---- Sistema ----
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip python3-venv ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

# ---- PyFlink alineado con 1.18 ----
RUN pip3 install --no-cache-dir "apache-flink==1.18.*"

# ---- Copiamos el job ----
COPY job.py /opt/flink/job.py

# ---- Conectores Kafka exactos ----
ARG KAFKA_CONNECTOR_VERSION=3.1.0-1.18
ARG KAFKA_CLIENTS_VERSION=3.5.1
RUN set -eux; \
  cd /opt/flink/lib; \
  curl -fL -o flink-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar; \
  curl -fL -o kafka-clients-${KAFKA_CLIENTS_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar

# ---- Config Flink ----
# * NO pipeline.jars
# * ClassLoader parent-first para Kafka
# * Python explícito para cliente y workers
RUN set -eux; { \
    echo ""; \
    echo "classloader.resolve-order: parent-first"; \
    echo "classloader.parent-first-patterns.additional: org.apache.kafka,org.apache.kafka.*"; \
    echo "python.client.executable: /usr/bin/python3"; \
    echo "python.executable: /usr/bin/python3"; \
  } >> /opt/flink/conf/flink-conf.yaml

# Sanity
RUN python3
