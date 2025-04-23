from kafka import KafkaConsumer
import json
from datetime import datetime
import os

# Kafka Consumer Config
consumer = KafkaConsumer(
    bootstrap_servers="cvlbcp4geec79i9lhn20.any.ap-south-1.mpx.prd.cloud.redpanda.com:9092",
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username=dbutils.secrets.get(scope="sent-an8", key="username"), 
    sasl_plain_password=dbutils.secrets.get(scope="sent-an8", key="my-key"),
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    consumer_timeout_ms=10000,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    key_deserializer=lambda m: m.decode("utf-8") if m else None,
)

# Subscribe to the topic
consumer.subscribe(["social_media_data"])

# Collect messages
messages = []
for message in consumer:
    messages.append(message.value)
    print(f"📥 topic: {message.topic} ({message.partition}|{message.offset})")
    print(f"🔑 key: {message.key}")
    print("📦 value:")
    print(json.dumps(message.value, indent=2))

# Save messages as a single file to landing folder
if messages:
    now = datetime.utcnow()
    timestamp_str = now.strftime("%Y-%m-%d_%H-%M-%S")

    # ADLS Gen2 mounted path
    landing_dir = f"/dbfs/mnt/capstone-group-8/landing/post_detail/"
    os.makedirs(landing_dir, exist_ok=True)

    # Add timestamp to filename to prevent overwrite
    filepath = os.path.join(landing_dir, f"post_detail_{timestamp_str}.json")
    with open(filepath, "w") as f:
        json.dump(messages, f, indent=2)

    print(f"✅ File saved at: {filepath}")
else:
    print("⚠️ No messages received from Kafka topic.")
