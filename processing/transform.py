from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, MinMaxScaler

def transform_logs(df):
    indexer = StringIndexer(inputCol="http_method", outputCol="http_method_index")
    encoder = OneHotEncoder(inputCol="http_method_index", outputCol="http_method_encoded")
    df = indexer.fit(df).transform(df)
    df = encoder.fit(df).transform(df)

    assembler = VectorAssembler(inputCols=["response_size"], outputCol="features")
    df = assembler.transform(df)

    scaler = MinMaxScaler(inputCol="features", outputCol="scaled_features")
    df = scaler.fit(df).transform(df)

    return df
