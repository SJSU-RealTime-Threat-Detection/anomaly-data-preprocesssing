from kafka import KafkaProducer
import json
import time

from config.kafka_config import RAW_LOGS_TOPIC

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

log_entries = [
    '192.168.12.10 - - [22/Apr/2025:10:12:34 +0000] "GET /docs/help?topic=alert_usage HTTP/1.1" 200 1340 "-" "Mozilla/5.0"',
    '10.0.10.12 - - [22/Apr/2025:11:15:45 +0000] "GET /api/v1/users?name=shyam HTTP/1.1" 200 980 "-" "Mozilla/5.0"',
    '172.16.1.15 - - [22/Apr/2025:12:17:03 +0000] "GET /product/details?id=12345&param=select HTTP/1.1" 200 1120 "-" "Mozilla/5.0"',
    '198.51.100.8 - - [22/Apr/2025:13:20:01 +0000] "POST /upload/image?caption=DROP+it+like+its+hot HTTP/1.1" 201 820 "-" "Mozilla/5.0"',
    '203.0.113.200 - - [22/Apr/2025:13:35:12 +0000] "GET /music/search?q=curl+of+the+waves HTTP/1.1" 200 740 "-" "Mozilla/5.0"',
    '192.0.2.22 - - [22/Apr/2025:14:10:05 +0000] "GET /legal/terms?doc=union_clauses_guide.pdf HTTP/1.1" 200 1600 "-" "Mozilla/5.0"',
    '198.18.0.1 - - [22/Apr/2025:15:44:09 +0000] "GET /wiki/bash_script_basics HTTP/1.1" 200 1280 "-" "Mozilla/5.0"',
    '198.51.100.25 - - [22/Apr/2025:16:22:18 +0000] "GET /downloads/sh_toolkit.zip HTTP/1.1" 200 2000 "-" "Mozilla/5.0"',
    '203.0.113.88 - - [22/Apr/2025:17:00:00 +0000] "GET /profile?bio=I+love+using+%3Ccode%3Eblocks%3C/code%3E HTTP/1.1" 200 990 "-" "Mozilla/5.0"',
    '192.168.100.100 - - [22/Apr/2025:17:30:22 +0000] "GET /config/settings?value=1%3D1%3Fmaybe HTTP/1.1" 200 1100 "-" "Mozilla/5.0"'
]


for log in log_entries:
    future = producer.send(RAW_LOGS_TOPIC, log)  # Send each log separately
    try:
        record_metadata = future.get(timeout=10)
        print(f"Log successfully sent to {record_metadata.topic} partition {record_metadata.partition}")
    except Exception as e:
        print(f"Error sending log: {e}")

producer.flush()
time.sleep(2)

