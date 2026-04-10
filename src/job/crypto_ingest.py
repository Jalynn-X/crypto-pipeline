# /opt/src/job/crypto_ingest.py
import json
import os
from datetime import datetime

from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction
from pyflink.common import Configuration

# -----------------------------
# CONFIG
# -----------------------------
KAFKA_TOPIC = "crypto"
KAFKA_SERVERS = "redpanda:29092"
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET")
# Ensure the path has the gs:// prefix and points to the right bucket
GCS_CHECKPOINT_PATH = os.getenv("GCS_CHECKPOINT_PATH", f"gs://{GCS_BUCKET_NAME}/checkpoints")

# REDUCED FOR TESTING
FLUSH_EVERY_SECONDS = 30  

# -----------------------------
# GCS SINK
# -----------------------------
class GCSJsonlSink(MapFunction):
    def __init__(self, bucket_name, flush_every_seconds=30):
        self.bucket_name = bucket_name
        self.flush_every_seconds = flush_every_seconds
        self._buffer = []
        self._last_flush = None
        self._client = None
        self._bucket = None

    def _get_bucket(self):
        if self._client is None:
            from google.cloud import storage
            self._client = storage.Client()
            self._bucket = self._client.bucket(self.bucket_name)
        return self._bucket

    def open(self, runtime_context):
        self._last_flush = datetime.utcnow()
        print(f"GCSJsonlSink opened — flushing every {self.flush_every_seconds}s "
              f"to gs://{self.bucket_name}")

    def map(self, value):
        self._buffer.append(value)
        now = datetime.utcnow()
        elapsed = (now - self._last_flush).total_seconds()
        if elapsed >= self.flush_every_seconds:
            self._flush(now)
        return value

    def _flush(self, now=None):
        if not self._buffer:
            return
        if now is None:
            now = datetime.utcnow()
        try:
            bucket = self._get_bucket()
            blob_path = (
                f"bronze/crypto/"
                f"{now.strftime('%Y/%m/%d/%H')}/"
                f"crypto_{now.strftime('%Y%m%d_%H%M%S_%f')}.jsonl"
            )
            content = "\n".join(self._buffer) + "\n"
            blob = bucket.blob(blob_path)
            blob.upload_from_string(content, content_type="application/json")
            print(f"Flushed {len(self._buffer)} records → gs://{self.bucket_name}/{blob_path}")
            self._buffer = []
            self._last_flush = now
        except Exception as e:
            print(f"GCS write failed: {e}")

# -----------------------------
# FLINK ENVIRONMENT CONFIGURATION
# -----------------------------
config = Configuration()

# 1. Restart Strategy
config.set_string("restart-strategy", "fixed-delay")
config.set_string("restart-strategy.fixed-delay.attempts", "3")
config.set_string("restart-strategy.fixed-delay.delay", "10s")

# 2. Checkpointing Storage & Directory (This replaces the broken lines)
config.set_string("state.checkpoints.dir", GCS_CHECKPOINT_PATH)
config.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
config.set_string("execution.checkpointing.interval", "60000")
config.set_string("execution.checkpointing.timeout", "120000")
config.set_string("execution.checkpointing.min-pause", "30000")
config.set_string("execution.checkpointing.externalized-checkpoint-retention", "RETAIN_ON_CANCELLATION")

# Initialize Environment with the config object
env = StreamExecutionEnvironment.get_execution_environment(config)
env.set_parallelism(1)

# ── JARs ──────────────────────────────────────────────────────────────────────
env.add_jars(
    "file:///opt/flink/usrlib/flink-connector-kafka-1.17.2.jar",
    "file:///opt/flink/usrlib/kafka-clients-3.4.0.jar",
)

# -----------------------------
# KAFKA SOURCE
# -----------------------------
kafka_consumer = FlinkKafkaConsumer(
    topics=KAFKA_TOPIC,
    deserialization_schema=SimpleStringSchema(),
    properties={
        "bootstrap.servers": KAFKA_SERVERS,
        "group.id": "flink-crypto-ingest-group",
        "auto.offset.reset": "earliest",
    },
)

# Crucial for stateful Kafka consuming
kafka_consumer.set_commit_offsets_on_checkpoints(True)

stream = env.add_source(kafka_consumer)

# -----------------------------
# BRONZE SINK → GCS
# -----------------------------
stream.map(
    GCSJsonlSink(GCS_BUCKET_NAME, flush_every_seconds=FLUSH_EVERY_SECONDS),
    output_type=Types.STRING()
)

# -----------------------------
# EXECUTE
# -----------------------------
if __name__ == '__main__':
    env.execute("Crypto Ingestion Job")
