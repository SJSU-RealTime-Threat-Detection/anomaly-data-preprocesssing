from kafka import KafkaConsumer, KafkaProducer
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import Row
import re
from processing.preprocess import preprocess_logs
from processing.enrich import enrich_logs
from config.kafka_config import KAFKA_BROKER, RAW_LOGS_TOPIC, PROCESSED_LOGS_TOPIC

# Initialize Spark Structured Streaming
spark = SparkSession.builder \
    .appName("CybersecurityLogProcessing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# Define schema for logs
log_schema = StructType([
    StructField("client_ip", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("http_method", StringType(), True),
    StructField("url", StringType(), True),
    StructField("response_code", IntegerType(), True),
    StructField("response_size", IntegerType(), True),
    StructField("referrer", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("extra_info", StringType(), True)

])

# Read logs from Kafka
raw_logs = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", RAW_LOGS_TOPIC) \
    .option("failOnDataLoss", "false") \
    .option("startingOffsets", "latest") \
    .load()

# Convert Kafka value to string
logs_df = raw_logs.selectExpr("CAST(value AS STRING) as log_entry")

logs_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Log regex pattern

LOG_PATTERN = re.compile(
    r'(\S+) - - \[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} [+-]\d{4})\] '  # IP and timestamp
    r'"(GET|POST|PUT|DELETE|HEAD|OPTIONS|PATCH)? ([^"]*?) HTTP/\d\.\d" '  # HTTP method and URL
    r'(\d{3}) (\d+|-) '  # Response code and response size
    r'"([^"]*|-)" '  # Referrer (can be "-")
    r'"([^"]*)"'  # User-Agent
    r'(?:\s(.+))?'  # Optional extra info (without forcing a space)
)

def parse_log(log_message):
    print(log_message)

    print(f"Raw Log: {repr(log_message)}")

    if log_message.startswith('"') and log_message.endswith('"'):
        log_message = log_message[1:-1]

    log_message = log_message.replace('\\"', '"')

    print(f"Processed Log: {repr(log_message)}")

    match = LOG_PATTERN.match(log_message)
    if match:
        print("Match Found")
        log_dict = {
            "client_ip": match.group(1),
            "timestamp": match.group(2),
            "http_method": match.group(3) if match.group(3) else "UNKNOWN",
            "url": match.group(4) if match.group(4) else "UNKNOWN",
            "response_code": int(match.group(5)),
            "response_size": int(match.group(6)) if match.group(6).isdigit() else 0,
            "referrer": match.group(7) if match.group(7) and match.group(7) != "-" else "UNKNOWN",
            "user_agent": match.group(8) if match.group(8) else "UNKNOWN",
            "extra_info": match.group(9) if match.group(9) else "NONE"
        }
        return json.dumps(log_dict)
    print("Match Not Found")
    return None

# Define UDF for parsing logs
parse_udf = udf(parse_log, StringType())
# Apply parsing UDF
logs_df = logs_df.withColumn("parsed", parse_udf(col("log_entry")))

# Convert parsed JSON string to structured columns
parsed_logs = logs_df.withColumn("parsed_struct", from_json(col("parsed"), log_schema)) \
                     .select("parsed_struct.*")

# Write parsed logs to console for debugging
parsed_logs.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Apply preprocessing
preprocessed_logs = preprocess_logs(parsed_logs)
print("Logs preprocessed!!!")

# Apply enrichment
enriched_logs = enrich_logs(preprocessed_logs)
print("logs enriched!!!")

# Convert to JSON and write back to Kafka
query = enriched_logs.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", PROCESSED_LOGS_TOPIC) \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .start()

query.awaitTermination()
