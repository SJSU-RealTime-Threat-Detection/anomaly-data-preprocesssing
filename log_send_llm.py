import json
import requests
from pyspark.sql.functions import col

API_ENDPOINT = "http://127.0.0.1:5000/gemini"


def send_anomalous_logs_to_api(batch_df, batch_id):
    """
    This function is designed to be used with Spark Structured Streaming's foreachBatch.
    If the batch contains even one anomaly (anomaly_label == 1), the entire batch is sent to the API.
    """
    if batch_df.filter(col("anomaly_label") == 1).count() > 0:
        try:
            logs = batch_df.toJSON().map(lambda j: json.loads(j)).collect()
            payload = {"anomalous_logs": logs}

            response = requests.post(API_ENDPOINT, json=payload)
            print(f"[Batch {batch_id}] Sent {len(logs)} logs - Status: {response.status_code}")

        except Exception as e:
            print(f"[Batch {batch_id}] Failed to send logs: {e}")
    else:
        print(f"[Batch {batch_id}] No anomalies found, nothing sent.")

