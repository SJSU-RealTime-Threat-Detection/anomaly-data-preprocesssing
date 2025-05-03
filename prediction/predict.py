import pickle
import numpy as np
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import IntegerType, StructType, FloatType, StructField

with open("/app/models/anomaly_detection_model.pkl", "rb") as f:
    model = pickle.load(f)

with open("/app/models/anomaly_detection_http_encoder.pkl", "rb") as f:
    encoder = pickle.load(f)

# Define output schema
schema = StructType([
    StructField("anomaly_label", IntegerType(), True),
    StructField("anomaly_score", FloatType(), True)
])

def apply_model(df):
    @pandas_udf(schema)
    def predict_udf(
        http_method, has_sql_keyword,
        has_xss_keyword, has_cmd_keyword, has_encoded_char
    ):
        # Encode http_method
        http_method_encoded = encoder.transform(http_method)

        # # Combine with the binary (non-scaled) features
        final_features = np.concatenate([
            np.array([http_method_encoded, has_sql_keyword, has_xss_keyword, has_cmd_keyword, has_encoded_char]).T
        ], axis=1)

        X_df = pd.DataFrame(final_features, columns=[
            "http_method", "has_sql_keyword","has_xss_keyword", "has_cmd_keyword", "has_encoded_char",
        ])

        probs = model.predict_proba(X_df)
        predictions = np.argmax(probs, axis=1)
        confidence_scores = np.max(probs, axis=1)

        return pd.DataFrame({
            "anomaly_label": predictions.astype(int),
            "anomaly_score": confidence_scores.astype(float)
        })

    result_struct = predict_udf(
        df["http_method"],
         df["has_sql_keyword"], df["has_xss_keyword"],
        df["has_cmd_keyword"], df["has_encoded_char"]
    )

    return df.withColumns({
        "anomaly_label": result_struct["anomaly_label"],
        "anomaly_score": result_struct["anomaly_score"]
    })
