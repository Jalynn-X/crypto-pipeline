import json
import time
from kafka import KafkaConsumer
from collections import defaultdict, deque

WINDOW_SIZE = 300  # 5 minutes
DROP_THRESHOLD = 1
INCREASE_THRESHOLD = 0.1

consumer = KafkaConsumer(
    'crypto',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# store per crypto
price_windows = defaultdict(deque)

for message in consumer:
    data = message.value

    pair = data["pair"]
    price = float(data["data"]["c"][0])
    timestamp = time.time()

    window = price_windows[pair]

    window.append((timestamp, price))

    while window and (timestamp - window[0][0] > WINDOW_SIZE):
        window.popleft()

    if len(window) > 1:
        oldest_price = window[0][1]
        percent = (price / oldest_price - 1) * 100

        # DEBUG PRINT
        print(f"{pair} | price: {price:.2f} | change: {percent:.4f}%")

        if percent < 0 and percent <= -DROP_THRESHOLD:
            print(f"🚨 ALERT: {pair} dropped {percent:.2f}% in last 5 min")
        elif percent > 0  and percent >= INCREASE_THRESHOLD:
            print(f"🚨 ALERT: {pair} increased {percent:.2f}% in last 5 min")