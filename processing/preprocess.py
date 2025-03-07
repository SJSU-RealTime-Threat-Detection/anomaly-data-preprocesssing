from pyspark.sql.functions import col, to_timestamp, regexp_extract, when, trim

def preprocess_logs(df):
    # Convert timestamp to standard format, allowing flexible parsing
    # df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

    # Remove duplicates based on client_ip and timestamp
    df = df.dropDuplicates(["client_ip", "timestamp"])

    # Fill missing values
    df = df.fillna({"response_code": 200, "client_ip": "0.0.0.0", "http_method": ""})

    # Extract HTTP method (GET, POST, etc.) - case-insensitive
    # df = df.withColumn("http_method", trim(regexp_extract(col("request"), r'(?i)\b(GET|POST|PUT|DELETE|HEAD|OPTIONS|PATCH)\b', 0)))
    #
    # # Extract URL from the request field
    # df = df.withColumn("url", trim(regexp_extract(col("request"), r'\"(?:GET|POST|PUT|DELETE|HEAD|OPTIONS|PATCH)\s+([^ ]+)', 1)))

    # Categorize response codes (e.g., 2xx = success, 4xx = client error, 5xx = server error)
    df = df.withColumn("response_category",
                       when(col("response_code").between(200, 299), "Success")
                       .when(col("response_code").between(400, 499), "Client Error")
                       .when(col("response_code").between(500, 599), "Server Error")
                       .otherwise("Other"))

    return df
