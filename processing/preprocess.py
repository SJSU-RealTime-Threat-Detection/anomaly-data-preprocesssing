from pyspark.sql.functions import col, when, trim, lower


def preprocess_logs(df):
    # Normalize and trim URL
    df = df.withColumn("url", trim(lower(col("url"))))

    # Remove duplicates based on client_ip and timestamp
    df = df.dropDuplicates(["client_ip", "timestamp", "http_method", "url"])

    # Fill missing values
    df = df.fillna({"response_code": 200, "client_ip": "0.0.0.0", "http_method": ""})


    # Categorize response codes (e.g., 2xx = success, 4xx = client error, 5xx = server error)
    df = df.withColumn("response_category",
                       when(col("response_code").between(200, 299), "Success")
                       .when(col("response_code").between(400, 499), "Client Error")
                       .when(col("response_code").between(500, 599), "Server Error")
                       .otherwise("Other"))

    return df
