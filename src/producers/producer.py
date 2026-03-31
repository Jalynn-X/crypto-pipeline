import time
import json
import os
import requests
from kafka import KafkaProducer
from datetime import datetime

# Configurable interval
INTERVAL = int(os.getenv("INTERVAL", 60))

pairs = ["XBTUSD", "ETHUSD"]
TOPIC = "crypto"
SERVER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=[SERVER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def get_price(pair):
    url = f"https://api.kraken.com/0/public/Ticker?pair={pair}"
    response = requests.get(url)
    data = response.json()

    if data["error"]:
        print("Error:", data["error"])
        return None

    result = data["result"]
    key = list(result.keys())[0]

    return result[key]


while True:
    for pair in pairs:
        data = get_price(pair)

        if data is None:
            continue

        message = {
            "pair": pair,
            "data": data,  # full raw JSON (Bronze layer)
            "ingestion_time": datetime.utcnow().isoformat()
        }

        print("Sending:", message)
        producer.send(TOPIC, value=message)

    producer.flush()
    time.sleep(INTERVAL)
    