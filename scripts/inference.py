import json
import joblib
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
import os

AGG_TOPIC = "aggregated_events"
ANOMALY_TOPIC = "anomalies"
BOOTSTRAP = "localhost:9092"
MODEL_PATH = "random_forest_hdfs.pkl"

class StreamingInferenceService:
    def __init__(self):
        # Load model
        if os.path.exists(MODEL_PATH):
            self.model = joblib.load(MODEL_PATH)
            print(f"Loaded model from {MODEL_PATH}")
        else:
            raise FileNotFoundError(f"No model found at {MODEL_PATH}. Train a model first.")

        # Kafka consumer for aggregated events
        self.consumer = KafkaConsumer(
            AGG_TOPIC,
            bootstrap_servers=BOOTSTRAP,
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
        )

        # Kafka producer for anomalies
        self.producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

        # Counters
        self.total_inferences = 0
        self.anomaly_count = 0

    def run_inference(self, row, threshold=0.75):
        """Run inference on a single aggregated row with probability threshold"""
        self.total_inferences += 1

        # Reindex features to match training columns
        columns = joblib.load("models/model_columns_v1.pkl")
        features = pd.DataFrame([row]).reindex(columns=columns, fill_value=0)

        # Get probability of class 1 (anomaly)
        prob = self.model.predict_proba(features)[0][1]

        if prob >= threshold:
            self.anomaly_count += 1
            result = {
                "BlockId": row.get("BlockId"),
                "prediction": 1,
                "probability": float(prob),
                "model_version": "latest"
            }
            self.producer.send(ANOMALY_TOPIC, result)
            print(f"[Inference #{self.total_inferences}] Anomaly #{self.anomaly_count}: {result}")
        else:
            print(f"[Inference #{self.total_inferences}] Normal row (prob={prob:.3f}), no anomaly.")


    def run(self):
        print("Starting inference ...")
        for msg in self.consumer:
            row = msg.value
            self.run_inference(row)


if __name__ == "__main__":
    service = StreamingInferenceService()
    service.run()
