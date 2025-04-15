import pickle
import numpy as np
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import IntegerType

with open("models/anomaly_detection_model.pkl", "rb") as f:
    model = pickle.load(f)

with open("models/anomaly_detection_scaler.pkl", "rb") as f:
    scaler = pickle.load(f)

with open("models/anomaly_detection_http_encoder.pkl", "rb") as f:
    encoder = pickle.load(f)

def apply_model(df):
    @pandas_udf(IntegerType())
    def predict_udf(
        http_method, response_code, response_size, request_length,
        num_numeric_chars, num_words, has_sql_keyword,
        has_xss_keyword, has_cmd_keyword, has_encoded_char,
        requests_in_5_min
    ):
        # Encode http_method
        http_method_encoded = encoder.transform(http_method)

        # Stack and scale selected features
        numeric_features = np.stack([
            response_code, response_size, request_length,
            num_numeric_chars, num_words, requests_in_5_min
        ], axis=1)

        scaled = scaler.transform(numeric_features)

        # # Combine with the binary (non-scaled) features
        final_features = np.concatenate([
            scaled,
            np.array([http_method_encoded, has_sql_keyword, has_xss_keyword, has_cmd_keyword, has_encoded_char]).T
        ], axis=1)


        predictions = model.predict(final_features)
        return pd.Series(predictions.astype(int))

    return df.withColumn("anomaly_label", predict_udf(
        df["http_method"], df["response_code"], df["response_size"], df["request_length"],
        df["num_numeric_chars"], df["num_words"], df["has_sql_keyword"], df["has_xss_keyword"],
        df["has_cmd_keyword"], df["has_encoded_char"], df["requests_in_5_min"]
    ))
