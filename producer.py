import os
import json
import socket
from kafka import KafkaProducer

# Redpanda (Kafka) Producer Config
producer = KafkaProducer(
    bootstrap_servers="<server-name>",
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username="sentiment_analysis_group_consumer_good",
    sasl_plain_password="<pass-word>", #
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8")
)

# Directory containing JSON files
json_dir = "json_data"  # Update to your folder path
hostname = socket.gethostname()

# Send each JSON file as one Kafka message
for filename in os.listdir(json_dir):
    if filename.endswith(".json"):
        file_path = os.path.join(json_dir, filename)
        with open(file_path, "r", encoding="utf-8") as f:
            try:
                json_data = json.load(f)
                producer.send("social_media_data", key=hostname, value=json_data)
                print(f" Sent file '{filename}' to topic 'social_media_data'")
            except Exception as e:
                print(f" Failed to send '{filename}': {e}")

producer.flush()
producer.close()
print(" All files processed.")
