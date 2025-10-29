# Servicios/Flink/job.py
import json, os
from datetime import datetime

from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.functions import MapFunction, FilterFunction
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaOffsetsInitializer,
    KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee
)
from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy

# --- Config ---
KAFKA = "kafka:9092"
TOPIC_IN_RAW   = "questions.answers"   # crudo (LLM y cache hits)
TOPIC_IN_SCORE = "answers.scored"      # evaluados por Score

TOPIC_OK       = "storage.persist"     # persistir
TOPIC_CACHEUP  = "cache.update"        # calentar caché con válidos
TOPIC_RETRY    = "questions.requests"  # reintentar pipeline inicial
TOPIC_ERR      = "questions.errors"    # auditoría de descartes/errores

SCORE_THRESHOLD  = float(os.getenv("ROUTER_SCORE_THRESHOLD", "0.6"))
MAX_RETRIES      = int(os.getenv("ROUTER_MAX_RETRIES", "3"))

def _now():
    return datetime.utcnow().isoformat()

class RouteResult(MapFunction):
    """
    Recibe JSON desde ambos tópicos.
    Devuelve (tag, payload_str), tag ∈ {"ok","retry","err","skip"}.
    """
    def map(self, value):
        # 1) parse
        try:
            data = json.loads(value)
        except Exception:
            out = {"raw": value, "reason": "bad_json", "ts": _now()}
            return ("err", json.dumps(out, ensure_ascii=False))

        # 2) cache hits (vía Cache -> questions.answers)
        if data.get("cached") and (data.get("final_answer") or "").strip():
            return ("ok", json.dumps(data, ensure_ascii=False))

        # 3) mensajes scored (vía Score -> answers.scored)
        if "score" in data:
            score = float(data.get("score") or 0.0)
            final_answer = (data.get("final_answer") or "").strip()
            ref = (data.get("reference_answer") or "").strip()

            # Aceptar si supera umbral o coincide con referencia (fallback del scorer)
            if final_answer and (score >= SCORE_THRESHOLD or (ref and final_answer == ref)):
                return ("ok", json.dumps(data, ensure_ascii=False))

            # controlar reintentos (fallback si scorer no propaga "retries")
            raw_r = data.get("retries", None)
            try:
                retries = int(raw_r)
            except (TypeError, ValueError):
                # Si no viene, asumimos agotado para cortar loops
                retries = MAX_RETRIES

            if retries >= MAX_RETRIES:
                err = {"drop": data, "reason": "max_retries", "ts": _now()}
                return ("err", json.dumps(err, ensure_ascii=False))

            # Reinyectar lo mínimo para reintentar
            req = {
                "id": data.get("id"),
                "question_id": data.get("question_id"),
                "question": data.get("question"),
                "reference_answer": data.get("reference_answer"),
                "dist_label": data.get("dist_label"),
                "rate": data.get("rate"),
                "run_id": data.get("run_id"),
                "retries": retries + 1,
                "ts_requeued": _now()
            }
            return ("retry", json.dumps(req, ensure_ascii=False))

        # 4) respuestas crudas de LLM (sin score): ignora, Score se encarga
        if "llm_answer" in data and "score" not in data:
            return ("skip", json.dumps({"info": "raw_llm_ignored"}, ensure_ascii=False))

        # 5) resto -> err
        out = {"raw": data, "reason": "unhandled", "ts": _now()}
        return ("err", json.dumps(out, ensure_ascii=False))


class OnlyTag(FilterFunction):
    def __init__(self, tag):
        self.tag = tag
    def filter(self, value):
        return value[0] == self.tag


def kafka_source():
    # Consumimos de 2 tópicos de entrada
    return KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA) \
        .set_topics(TOPIC_IN_RAW, TOPIC_IN_SCORE) \
        .set_group_id("flink-answers-router") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()


def kafka_sink(topic, guarantee=DeliveryGuarantee.AT_LEAST_ONCE):
    return KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
        .set_delivery_guarantee(guarantee) \
        .build()


def tag_src(s, src="flink-router"):
    try:
        o = json.loads(s)
        o["_src"] = src
        return json.dumps(o, ensure_ascii=False)
    except Exception:
        return s


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)
    env.enable_checkpointing(10_000)

    ds = env.from_source(
        kafka_source(),
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="answers-mixed",
        type_info=Types.STRING()
    )

    routed = ds.map(RouteResult(), output_type=Types.TUPLE([Types.STRING(), Types.STRING()]))

    ok_stream    = routed.filter(OnlyTag("ok")).map(lambda t: t[1], output_type=Types.STRING())
    retry_stream = routed.filter(OnlyTag("retry")).map(lambda t: t[1], output_type=Types.STRING())
    err_stream   = routed.filter(OnlyTag("err")).map(lambda t: t[1], output_type=Types.STRING())
    # skip_stream = routed.filter(OnlyTag("skip"))

    # OK -> persist + cache.update
    ok_stream.sink_to(kafka_sink(TOPIC_OK, DeliveryGuarantee.AT_LEAST_ONCE))
    ok_stream.map(lambda x: tag_src(x), output_type=Types.STRING()) \
             .sink_to(kafka_sink(TOPIC_CACHEUP, DeliveryGuarantee.AT_LEAST_ONCE))

    # RETRY -> questions.requests
    retry_stream.map(lambda x: tag_src(x), output_type=Types.STRING()) \
                .sink_to(kafka_sink(TOPIC_RETRY, DeliveryGuarantee.NONE))

    # ERR -> questions.errors (auditables)
    err_stream.map(lambda x: tag_src(x), output_type=Types.STRING()) \
              .sink_to(kafka_sink(TOPIC_ERR, DeliveryGuarantee.NONE))

    env.execute("answers-router")


if __name__ == "__main__":
    main()
