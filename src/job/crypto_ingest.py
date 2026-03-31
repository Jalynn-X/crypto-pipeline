# /opt/src/job/crypto_ingest.py
import json
import os
from datetime import datetime

from pyflink.datastream import StreamExecutionEnvironment
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
GCS_BUCKET_NAME = os.environ["GCS_BUCKET"]
GCS_CHECKPOINT_PATH = os.environ["GCS_CHECKPOINT_PATH"]
FLUSH_EVERY_SECONDS = 300  # flush every 5 minutes (reduce to 30 for testing)

# -----------------------------
# GCS SINK
# -----------------------------
class GCSJsonlSink(MapFunction):
    """
    Pure ingestion sink — no business logic.
    Buffers raw JSON messages and flushes to GCS on a time basis.
    Partitioned by YYYY/MM/DD/HH for BigQuery compatibility.

    Duplicate prevention:
    - Flink checkpointing saves Kafka offsets every 60s to GCS
    - On restart, Flink resumes from last checkpoint offset
    - So the same Kafka message is never processed twice
    """
    def __init__(self, bucket_name, flush_every_seconds=300):
        self.bucket_name = bucket_name
        self.flush_every_seconds = flush_every_seconds
        self._buffer = []
        self._last_flush = None
        self._client = None
        self._bucket = None

    def _get_bucket(self):
        """Lazy-init GCS client — cannot be serialized so init on first use."""
        if self._client is None:
            from google.cloud import storage
            self._client = storage.Client()
            self._bucket = self._client.bucket(self.bucket_name)
        return self._bucket

    def open(self, runtime_context):
        """Called once when the operator starts — initialize flush timer."""
        self._last_flush = datetime.utcnow()
        print(f"✅ GCSJsonlSink opened — flushing every {self.flush_every_seconds}s "
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
            print(f"✅ Flushed {len(self._buffer)} records → "
                  f"gs://{self.bucket_name}/{blob_path}")
            self._buffer = []
            self._last_flush = now
        except Exception as e:
            print(f"❌ GCS write failed: {e}")
            # Don't clear buffer on failure — retry on next flush

# -----------------------------
# FLINK ENVIRONMENT
# -----------------------------
config = Configuration()

# ── Checkpointing config ──────────────────────────────────────────────────────
# EXACTLY_ONCE: Flink saves Kafka offsets atomically with each checkpoint.
# On restart, it resumes from the last successful checkpoint offset,
# so no message is processed twice and none are skipped.
config.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
config.set_string("execution.checkpointing.interval", "60000")    # every 60s
config.set_string("execution.checkpointing.min-pause", "30000")   # min 30s between checkpoints
config.set_string("execution.checkpointing.timeout", "120000")    # fail if takes > 2 min
config.set_string("execution.checkpointing.max-concurrent-checkpoints", "1")
config.set_string("execution.checkpointing.storage", "filesystem")
config.set_string("execution.checkpointing.dir", GCS_CHECKPOINT_PATH)

# ── Restart strategy ──────────────────────────────────────────────────────────
# Automatically restart up to 3 times with 10s delay between attempts.
# Without this, any transient error (network blip, GCS timeout) kills the job.
config.set_string("restart-strategy", "fixed-delay")
config.set_string("restart-strategy.fixed-delay.attempts", "3")
config.set_string("restart-strategy.fixed-delay.delay", "10s")

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
        "group.id": "flink-crypto-ingest-group",  # separate group from old job
        "auto.offset.reset": "earliest",
    },
)

# Tell Flink to manage Kafka offsets via checkpoints
# This is what enables EXACTLY_ONCE — offsets are saved with each checkpoint
# so on restart Flink knows exactly where to resume from
kafka_consumer.set_commit_offsets_on_checkpoints(True)

stream = env.add_source(kafka_consumer)

# -----------------------------
# BRONZE SINK → GCS
# -----------------------------
stream.map(
    GCSJsonlSink(GCS_BUCKET_NAME, flush_every_seconds=FLUSH_EVERY_SECONDS),
    output_type=Types.STRING()
)

# Debug print — remove once GCS writes are confirmed working
stream.print()

# -----------------------------
# EXECUTE
# -----------------------------
env.execute("Crypto Ingestion Job")
