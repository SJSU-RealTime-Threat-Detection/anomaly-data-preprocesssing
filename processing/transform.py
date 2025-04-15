from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, MinMaxScaler

def transform_logs(df):

    df = df.drop("client_ip", "timestamp", "user_agent", "referrer","url","extra_info","geo_location", "response_category")


    return df
