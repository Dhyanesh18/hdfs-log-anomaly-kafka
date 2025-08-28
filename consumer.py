import pandas as pd
import json
import joblib
from kafka import KafkaConsumer, KafkaProducer
from train import train_model
import os

class StreamingRetrainer:
    def __init__(self, topic, bootstrap_servers='localhost:9092',
                    batch_size=5000):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m)
        )
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        self.buffer = []
        self.batch_size = batch_size
        self.version_counter = 1

        # Load latest trained model 
        self.model_path = "models/model_base.joblib"
        if os.path.exists(self.model_path):
            self.model = joblib.load(self.model_path)
            print("Loaded existing model for inference.")
        else:
            self.model = None
            print("No model found, please train an initial model first.")

    def process_message(self, message):
        # Add incoming row(s) to buffer
        if isinstance(message, list):
            self.buffer.extend(message)
        else:
            self.buffer.append(message)

        # Run inference if model exists
        if self.model is not None:
            self.run_inference(message)

        # Retrain once batch is full
        if len(self.buffer) >= self.batch_size:
            self.retrain()

    def run_inference(self, row):
        """Run inference on a single row and stream prediction to Kafka."""
        if isinstance(row, list):
            for r in row:
                self._predict_and_send(r)
        else:
            self._predict_and_send(row)

    def _predict_and_send(self, row):
        features = pd.DataFrame([row]).drop(columns=['BlockId', 'Label', 'Type'],
                                            errors='ignore').astype('float64')
        pred = int(self.model.predict(features)[0])

        # Only stream if anomaly is detected
        if pred == 1:   # assuming 1 = anomaly, 0 = normal
            result = {
                "BlockId": row.get("BlockId"),
                "prediction": pred,
                "model_version": f"v{self.version_counter}"
            }

            # Send prediction to another topic
            self.producer.send("hdfs_predictions", result)
            print(f"Anomaly streamed: {result}")


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
        joblib.dump(clf, model_path)
        joblib.dump(clf, self.model_path)  # overwrite latest

        # Reload model for inference
        self.model = clf

        # Increment version counter
        self.version_counter += 1

        return clf, report

    def run(self):
        print("Starting Kafka consumer...")
        for message in self.consumer:
            self.process_message(message.value)


if __name__ == "__main__":
    retrainer = StreamingRetrainer(
        topic='hdfs_logs',
        bootstrap_servers='localhost:9092',
        batch_size=5000,
    )
    retrainer.run()
