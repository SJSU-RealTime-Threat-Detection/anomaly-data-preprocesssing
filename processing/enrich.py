import re
import urllib

import geoip2.database
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType, StructType, StructField

# Load GeoIP database once
GEOIP_DB_PATH = "GeoLite2-City.mmdb"
geo_reader = geoip2.database.Reader(GEOIP_DB_PATH)

threat_schema = StructType([
    StructField("has_sql_keyword", IntegerType(), True),
    StructField("has_xss_keyword", IntegerType(), True),
    StructField("has_cmd_keyword", IntegerType(), True),
    StructField("has_encoded_char", IntegerType(), True)
])

def detect_threat_type(url):
    if not url:
        return {"has_sql_keyword": 0, "has_xss_keyword": 0, "has_cmd_keyword": 0, "has_encoded_char": 0}

    decoded = urllib.parse.unquote_plus(url.lower())

    sql_patterns = r"(select|union|insert|drop|delete|update|from|where|--|\bor\b|\band\b)"
    xss_patterns = r"(<script>|onerror|alert\(|<img|javascript:)"
    cmd_patterns = r"(wget|curl|nc|bash|\bsh\b|whoami|ls|cat|ping|sleep|chmod|chown|rm|cp|mv|;|&&|\|\|)"
    encoded_patterns = r"(%27|%3c|%3e|%3b|%26|%24|%60|%7c|%22|%23|%2f|%5c|%3d)"

    return {
        "has_sql_keyword": int(bool(re.search(sql_patterns, decoded))),
        "has_xss_keyword": int(bool(re.search(xss_patterns, decoded))),
        "has_cmd_keyword": int(bool(re.search(cmd_patterns, decoded))),
        "has_encoded_char": int(bool(re.search(encoded_patterns, url)))
    }
threat_udf = udf(detect_threat_type, threat_schema)

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
        # print(f"Error: {e}")
        return "Unknown"

geo_udf = udf(get_geo_location, StringType())

def enrich_logs(df):

    df_with_threat = df.withColumn("threat_features", threat_udf(F.col("url")))

    enriched_df = df_with_threat \
        .withColumn("has_sql_keyword", F.col("threat_features.has_sql_keyword")) \
        .withColumn("has_xss_keyword", F.col("threat_features.has_xss_keyword")) \
        .withColumn("has_cmd_keyword", F.col("threat_features.has_cmd_keyword")) \
        .withColumn("has_encoded_char", F.col("threat_features.has_encoded_char")) \
        .drop("threat_features")

    enriched_df = enriched_df.withColumn("geo_location", geo_udf(df.client_ip))

    enriched_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    return enriched_df
