from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'crypto',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    consumer_timeout_ms=3000
)

msgs = list(consumer)
print(f"Total messages: {len(msgs)}")
for m in msgs[:5]:
    print(m.value.decode())

consumer.close()