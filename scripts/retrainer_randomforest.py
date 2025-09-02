import pandas as pd
import json
import joblib
from kafka import KafkaConsumer
from utils.train import train_model
import os

class StreamingRetrainer:
    def __init__(self, topic, bootstrap_servers='localhost:9092',
                    batch_size=5000):
        # Kafka consumer for incoming data
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m)
        )

        self.buffer = []
        self.batch_size = batch_size
        self.version_counter = 1

    def process_message(self, message):
        # Add incoming row(s) to buffer
        if isinstance(message, list):
            self.buffer.extend(message)
        else:
            self.buffer.append(message)

        # Retrain once batch is full
        if len(self.buffer) >= self.batch_size:
            self.retrain()

    def retrain(self):
        print(f"Retraining on batch {self.version_counter}... "
              f"Buffer size: {len(self.buffer)}")

        df_batch = pd.DataFrame(self.buffer)
        self.buffer = []  # clear buffer

        # Load previous dataset
        df_old = pd.read_csv("preprocessed/Event_occurrence_matrix_train.csv")

        # Combine with new batch
        df_combined = pd.concat([df_old, df_batch], ignore_index=True)

        # Retrain on combined dataset
        clf, report = train_model(
            df_combined,
            version=f"v{self.version_counter}",
        )

        # Save updated combined dataset
        df_combined.to_csv("preprocessed/Event_occurrence_matrix_train.csv", index=False)

        # Save new model and also update the "latest" pointer
        model_path = f"models/model_v{self.version_counter}.joblib"
        os.makedirs("models", exist_ok=True)
        joblib.dump(clf, model_path)
        joblib.dump(clf, "models/model_base.joblib")  # overwrite latest

        # Increment version counter
        self.version_counter += 1

        return clf, report

    def run(self):
        print("Starting Kafka consumer for retraining...")
        for message in self.consumer:
            self.process_message(message.value)


if __name__ == "__main__":
    retrainer = StreamingRetrainer(
        topic='hdfs_logs',
        bootstrap_servers='localhost:9092',
        batch_size=5000,
    )
    retrainer.run()
