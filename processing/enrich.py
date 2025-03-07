from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import geoip2.database

# Load GeoIP database once
GEOIP_DB_PATH = "/Users/poorvaagarwal/PycharmProjects/anomaly-data-preprocesssing/GeoLite2-City.mmdb"
geo_reader = geoip2.database.Reader(GEOIP_DB_PATH)

def get_geo_location(ip):
    try:
        # Check if the IP is in correct format
        ip = ip.strip('""')
        print(f"Looking up IP: {ip}")
        response = geo_reader.city(ip)
        city = response.city.name or "Unknown"
        country = response.country.name or "Unknown"
        print(f"City: {city}, Country: {country}")  # Debugging output
        return f"{city}, {country}"
    except Exception as e:
        # Log the error message
        print(f"Error: {e}")
        return "Unknown"

geo_udf = udf(get_geo_location, StringType())

def enrich_logs(df):
    # df.show()  # Print out the dataframe before applying enrichment
    df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    enriched_df = df.withColumn("geo_location", geo_udf(df.client_ip))

    enriched_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    return enriched_df
