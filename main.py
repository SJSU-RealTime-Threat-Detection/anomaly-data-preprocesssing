from fastapi import FastAPI
import threading
# from kafka_consumer import process_log_messages

app = FastAPI()

@app.get("/")
def health_check():
    return {"status": "Running"}

# Run Kafka consumer in a separate thread
# threading.Thread(target=process_log_messages, daemon=True).start()
