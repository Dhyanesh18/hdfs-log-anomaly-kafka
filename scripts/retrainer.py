import os
import json
import joblib
import pandas as pd
from kafka import KafkaConsumer
from utils.train_ae import train_autoencoder


class StreamingRetrainerAE:
    def __init__(self, topic, bootstrap_servers='localhost:9092',
                    batch_size=5000, device="cpu"):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m)
        )
        self.buffer = []
        self.batch_size = batch_size
        self.version_counter = 1
        self.device = device

    def process_message(self, message):
        if isinstance(message, list):
            self.buffer.extend(message)
        else:
            self.buffer.append(message)

        if len(self.buffer) >= self.batch_size:
            self.retrain()

    def retrain(self):
        print(f"Retraining AE model v{self.version_counter}... Buffer size: {len(self.buffer)}")

        df_batch = pd.DataFrame(self.buffer)
        self.buffer = []

        # Load old data
        if os.path.exists("preprocessed/Event_occurrence_matrix_train.csv"):
            df_old = pd.read_csv("preprocessed/Event_occurrence_matrix_train.csv")
            df_combined = pd.concat([df_old, df_batch], ignore_index=True)
        else:
            df_combined = df_batch

        # Train
        ae, report, threshold = train_autoencoder(
            df_combined,
            version=f"v{self.version_counter}",
            device=self.device
        )

        # Save combined dataset
        os.makedirs("preprocessed", exist_ok=True)
        df_combined.to_csv("preprocessed/Event_occurrence_matrix_train.csv", index=False)

        # Save "latest" pointer
        joblib.dump(ae, "models/ae_model_latest.pkl")

        self.version_counter += 1
        return ae, report, threshold

    def run(self):
        print("Starting Kafka consumer for AE retraining...")
        for message in self.consumer:
            self.process_message(message.value)


if __name__ == "__main__":
    retrainer = StreamingRetrainerAE(
        topic="hdfs_logs",
        bootstrap_servers="localhost:9092",
        batch_size=5000,
        device="cpu"
    )
    retrainer.run()
