from kafka import KafkaProducer
import json
import time

from config.kafka_config import RAW_LOGS_TOPIC

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

log_entries = ['192.9.240.104 - - [07/Mar/2025:10:33:34 +0000] "DELETE /favicon.ico HTTP/1.1" 404 1309',
    '217.195.5.3 - - [07/Mar/2025:10:56:01 +0000] "GET /search?q=\' UNION SELECT username, password FROM users-- HTTP/1.1" 500 943 "-" "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36" SQL Injection Attack Detected']

for log in log_entries:
    future = producer.send(RAW_LOGS_TOPIC, log)  # Send each log separately
    try:
        record_metadata = future.get(timeout=10)
        print(f"Log successfully sent to {record_metadata.topic} partition {record_metadata.partition}")
    except Exception as e:
        print(f"Error sending log: {e}")

producer.flush()
time.sleep(2)

# future = producer.send(RAW_LOGS_TOPIC, log_entry)
#
# try:
#     record_metadata = future.get(timeout=10)  # Block until message is acknowledged
#     print(f"Log successfully sent to {record_metadata.topic} partition {record_metadata.partition}")
# except Exception as e:
#     print(f"Error sending log: {e}")
#
# producer.flush()  # Ensure the message is actually sent before the script exits
#
# time.sleep(2)  # Give Kafka time to process
