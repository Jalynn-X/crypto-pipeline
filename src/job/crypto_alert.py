# /opt/src/job/crypto_alert.py
import json
import os
from datetime import datetime

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.window import SlidingProcessingTimeWindows
from pyflink.common.time import Time
from pyflink.datastream.functions import ProcessWindowFunction, SinkFunction
from pyflink.common import Configuration

# -----------------------------
# CONFIG (from environment)
# -----------------------------
KAFKA_TOPIC = "crypto"
KAFKA_SERVERS = "redpanda:29092"

GCS_BUCKET_NAME = os.environ["GCS_BUCKET"]          # e.g. crypto-pipeline-491522-bucket
GCS_CHECKPOINT_PATH = os.environ["GCS_CHECKPOINT_PATH"]  # e.g. gs://bucket/checkpoints
GCS_BRONZE_PATH = os.environ["GCS_BRONZE_PATH"]          # e.g. gs://bucket/bronze/crypto

WINDOW_SIZE_SEC = 300   # 5 min
WINDOW_SLIDE_SEC = 60   # slide every 1 min

# Testing thresholds
DROP_THRESHOLD = -0.001
INCREASE_THRESHOLD = 0.001

# -----------------------------
# CUSTOM GCS SINK
# -----------------------------
class GCSJsonlSink(SinkFunction):
    """
    Writes raw JSON messages directly to GCS using the Python GCS client.
    Flushes every FLUSH_EVERY messages so files appear quickly during testing.
    """
    FLUSH_EVERY = 10  # lower = more frequent writes, good for testing

    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self._buffer = []
        self._client = None
        self._bucket = None

    def _get_bucket(self):
        """Lazy-init GCS client — can't be serialized so init on first use."""
        if self._client is None:
            from google.cloud import storage
            self._client = storage.Client()
            self._bucket = self._client.bucket(self.bucket_name)
        return self._bucket

    def invoke(self, value, context):
        self._buffer.append(value)
        if len(self._buffer) >= self.FLUSH_EVERY:
            self._flush()

    def _flush(self):
        if not self._buffer:
            return
        try:
            bucket = self._get_bucket()
            now = datetime.utcnow()
            # Partitioned by date/hour for easy BigQuery loading later
            blob_path = (
                f"bronze/crypto/"
                f"{now.strftime('%Y/%m/%d/%H')}/"
                f"crypto_{now.strftime('%Y%m%d_%H%M%S_%f')}.jsonl"
            )
            content = "\n".join(self._buffer) + "\n"
            blob = bucket.blob(blob_path)
            blob.upload_from_string(content, content_type="application/json")
            print(f"✅ Wrote {len(self._buffer)} records → gs://{self.bucket_name}/{blob_path}")
            self._buffer = []
        except Exception as e:
            print(f"❌ GCS write failed: {e}")

# -----------------------------
# FUNCTIONS
# -----------------------------
def parse_message(msg):
    """Parse raw Kafka JSON string into (pair, price) tuple."""
    data = json.loads(msg)
    pair = data["pair"]
    price = float(data["data"]["c"][0])
    return pair, price

def compute_alert(pair_prices):
    """Compute % change across window and return alert strings."""
    if len(pair_prices) < 2:
        return []
    prices = [p for _, p in pair_prices]
    oldest_price = prices[0]
    latest_price = prices[-1]
    if oldest_price == 0:
        return []
    percent_change = (latest_price / oldest_price - 1) * 100
    pair = pair_prices[0][0]
    alerts = []
    if percent_change <= DROP_THRESHOLD:
        alerts.append(f"DROP ALERT: {pair} dropped {percent_change:.2f}% in last 5 min")
    elif percent_change >= INCREASE_THRESHOLD:
        alerts.append(f"SPIKE ALERT: {pair} increased {percent_change:.2f}% in last 5 min")
    return alerts

# -----------------------------
# PROCESS WINDOW FUNCTION
# -----------------------------
class AlertWindowFunction(ProcessWindowFunction):
    def process(self, key, context, elements):
        elements_list = list(elements)
        for alert in compute_alert(elements_list):
            yield alert

# -----------------------------
# FLINK ENVIRONMENT
# -----------------------------
config = Configuration()
config.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
config.set_string("execution.checkpointing.interval", "60000")
config.set_string("execution.checkpointing.min-pause", "30000")
config.set_string("execution.checkpointing.timeout", "120000")
config.set_string("execution.checkpointing.max-concurrent-checkpoints", "1")
config.set_string("execution.checkpointing.storage", "filesystem")
config.set_string("execution.checkpointing.dir", GCS_CHECKPOINT_PATH)

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
        "group.id": "flink-crypto-group",
        "auto.offset.reset": "earliest",
    },
)

stream = env.add_source(kafka_consumer)

# -----------------------------
# BRONZE SINK → GCS
# -----------------------------
# Write all raw messages to GCS partitioned by date/hour
from pyflink.datastream.functions import MapFunction

class GCSJsonlSink(MapFunction):
    FLUSH_EVERY = 10

    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self._buffer = []
        self._client = None
        self._bucket = None

    def _get_bucket(self):
        if self._client is None:
            from google.cloud import storage
            self._client = storage.Client()
            self._bucket = self._client.bucket(self.bucket_name)
        return self._bucket

    def map(self, value):
        self._buffer.append(value)
        if len(self._buffer) >= self.FLUSH_EVERY:
            self._flush()
        return value  # pass through so stream continues

    def _flush(self):
        if not self._buffer:
            return
        try:
            bucket = self._get_bucket()
            now = datetime.utcnow()
            blob_path = (
                f"bronze/crypto/"
                f"{now.strftime('%Y/%m/%d/%H')}/"
                f"crypto_{now.strftime('%Y%m%d_%H%M%S_%f')}.jsonl"
            )
            content = "\n".join(self._buffer) + "\n"
            blob = bucket.blob(blob_path)
            blob.upload_from_string(content, content_type="application/json")
            print(f"✅ Wrote {len(self._buffer)} records → gs://{self.bucket_name}/{blob_path}")
            self._buffer = []
        except Exception as e:
            print(f"❌ GCS write failed: {e}")

# Apply the sink as a map (returns value unchanged, side-effects write to GCS)
stream.map(GCSJsonlSink(GCS_BUCKET_NAME), output_type=Types.STRING())

# -----------------------------
# ALERT PROCESSING PIPELINE
# -----------------------------
pair_price_stream = stream.map(
    parse_message,
    output_type=Types.TUPLE([Types.STRING(), Types.FLOAT()])
)

alerts_stream = (
    pair_price_stream
    .key_by(lambda x: x[0])
    .window(SlidingProcessingTimeWindows.of(
        Time.seconds(WINDOW_SIZE_SEC),
        Time.seconds(WINDOW_SLIDE_SEC),
    ))
    .process(AlertWindowFunction(), output_type=Types.STRING())
)

alerts_stream.print()

# -----------------------------
# EXECUTE
# -----------------------------
env.execute("Crypto Alert Flink Job")