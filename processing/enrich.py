from pyspark.sql import Window
from pyspark.sql.functions import udf, col, length, size, split, expr, count, avg, lag, unix_timestamp, when, lower, \
    to_timestamp, window
from pyspark.sql.types import StringType, IntegerType
import geoip2.database

# Load GeoIP database once
GEOIP_DB_PATH = "/Users/poorvaagarwal/PycharmProjects/anomaly-data-preprocesssing/GeoLite2-City.mmdb"
geo_reader = geoip2.database.Reader(GEOIP_DB_PATH)

def get_geo_location(ip):
    try:
        # Check if the IP is in correct format
        ip = ip.strip('""')
        # print(f"Looking up IP: {ip}")
        response = geo_reader.city(ip)
        city = response.city.name or "Unknown"
        country = response.country.name or "Unknown"
        # print(f"City: {city}, Country: {country}")  # Debugging output
        return f"{city}, {country}"
    except Exception as e:
        # Log the error message
        print(f"Error: {e}")
        return "Unknown"

geo_udf = udf(get_geo_location, StringType())

def enrich_logs(df):
    # df.show()  # Print out the dataframe before applying enrichment
    # df.writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .start()

    enriched_df = df.withColumn("geo_location", geo_udf(df.client_ip)) \
        .withColumn("request_length", length(col("url"))) \
        .withColumn("num_numeric_chars", expr("length(regexp_replace(url, '[^0-9]', ''))")) \
        .withColumn("num_words", size(split(col("url"), "[/?&=]"))) \
        .withColumn("has_sql_keyword",
                    expr("CASE WHEN lower(url) RLIKE 'select|union|insert|drop|or 1=1|--|update|delete|from|where' THEN 1 ELSE 0 END")) \
        .withColumn("has_xss_keyword",
                    expr("CASE WHEN lower(url) RLIKE '<script>|onerror|alert\\\\(|<img|javascript:' THEN 1 ELSE 0 END")) \
        .withColumn("has_cmd_keyword", expr(
        "CASE WHEN lower(url) RLIKE 'wget|curl|nc|bash|\\bsh\\b|whoami|ls|cat|ping|;|&&|\\|\\|' THEN 1 ELSE 0 END")) \
        .withColumn("has_encoded_char",
                    expr("CASE WHEN lower(url) RLIKE '%27|%3c|%3e|%3b|%26|%24|%60|%7c|%22|%23|%2f|%5c' THEN 1 ELSE 0 END")) \
        .withColumn("requests_in_5_min", expr("1"))


    enriched_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    return enriched_df
