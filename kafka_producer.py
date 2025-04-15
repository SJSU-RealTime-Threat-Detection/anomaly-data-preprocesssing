from kafka import KafkaProducer
import json
import time

from config.kafka_config import RAW_LOGS_TOPIC

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

log_entries = ['192.68.1.10 - - [15/Apr/2025:12:00:01 +0000] "GET /home HTTP/1.1" 200 1024 "-" "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"',
    '10.0.10.5 - - [15/Apr/2025:12:01:15 +0000] "POST /login HTTP/1.1" 302 512 "https://example.com" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"',
'203.0.183.99 - - [15/Apr/2025:12:06:11 +0000] "GET /search?q=\' UNION SELECT password FROM users-- HTTP/1.1" 500 943 "-" "Mozilla/5.0" SQL Injection Attempt',
    '192.168.20.33 - - [15/Apr/2025:12:07:23 +0000] "GET /profile?img=<script>alert(1)</script> HTTP/1.1" 403 622 "-" "Mozilla/5.0" XSS Detected',
    '172.16.10.2 - - [15/Apr/2025:12:02:45 +0000] "GET /dashboard HTTP/1.1" 200 2048 "-" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64)"',
'10.10.100.10 - - [15/Apr/2025:12:08:45 +0000] "GET /cmd?exec=sh+-c+whoami HTTP/1.1" 503 887 "-" "curl/7.64.1" Command Injection Attempt',
    '198.5.100.42 - - [15/Apr/2025:12:09:55 +0000] "POST /reset-password HTTP/1.1" 403 890 "-" "Mozilla/5.0" CSRF Possible - Mismatched origin',
    '203.70.113.55 - - [15/Apr/2025:12:03:59 +0000] "PUT /user/settings HTTP/1.1" 201 768 "-" "Mozilla/5.0 (iPhone; CPU iPhone OS 14_4 like Mac OS X)"',
    '198.81.100.25 - - [15/Apr/2025:12:05:10 +0000] "GET /api/data HTTP/1.1" 200 1440 "-" "Mozilla/5.0 (Linux; Android 11; Pixel 5)"',
               '192.20.2.99 - - [15/Apr/2025:12:10:31 +0000] "GET /api?param=%3Cscript%3Ealert(123)%3C/script%3E HTTP/1.1" 500 678 "-" "Mozilla/5.0" XSS Attempt'
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

