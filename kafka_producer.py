from kafka import KafkaProducer
import json
import time

from config.kafka_config import RAW_LOGS_TOPIC

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

log_entries = ['192.168.1.151 - - [2025-03-01T06:05:04.657701700] "GET /admin HTTP/1.1" 200 3367',
    '192.168.1.211 - - [2025-04-01T06:09:33.874498100] "POST /student HTTP/1.1" 404 2523',
    '192.168.1.18 - - [2025-05-01T06:09:33.874498100] "PUT /user HTTP/1.1" 500 249']

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
