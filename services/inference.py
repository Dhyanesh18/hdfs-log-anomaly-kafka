import json
import joblib
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
import os
import torch
import torch.nn as nn
import numpy as np

# Kafka setup
AGG_TOPIC = "aggregated_events"
ANOMALY_TOPIC = "anomalies"
BOOTSTRAP = "localhost:9092"


AE_MODEL_PATH = "models/ae_model_v1.pt"
AE_COLUMNS_PATH = "models/ae_model_columns_v1.pkl"


# Autoencoder class
class Autoencoder(nn.Module):
    def __init__(self, input_dim, hidden_dim=128, bottleneck_dim=32):
        super(Autoencoder, self).__init__()
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, bottleneck_dim),
            nn.ReLU()
        )
        self.decoder = nn.Sequential(
            nn.Linear(bottleneck_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, input_dim)
        )

    def forward(self, x):
        z = self.encoder(x)
        return self.decoder(z)



class StreamingInferenceService:
    def __init__(self, device="cpu"):
        self.device = device

        if not os.path.exists(AE_MODEL_PATH):
            raise FileNotFoundError(f"No AE model found at {AE_MODEL_PATH}")

        checkpoint = torch.load(AE_MODEL_PATH, map_location=device)
        self.model = Autoencoder(input_dim=checkpoint["input_dim"])
        self.model.load_state_dict(checkpoint["model_state"])
        self.model.to(device)
        self.model.eval()

        self.columns = joblib.load(AE_COLUMNS_PATH)
        self.mean = checkpoint["mean"]
        self.std = checkpoint["std"]
        self.threshold = checkpoint["threshold"]
        print(f"Loaded Autoencoder model from {AE_MODEL_PATH}")

        # Kafka
        self.consumer = KafkaConsumer(
            AGG_TOPIC,
            bootstrap_servers=BOOTSTRAP,
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
        )
        self.producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

        # Counters
        self.total_inferences = 0
        self.anomaly_count = 0

    def run_inference(self, row, threshold=0.75):
        """Run inference on a single aggregated row"""
        self.total_inferences += 1

        # Reindex features
        features = pd.DataFrame([row]).reindex(columns=self.columns, fill_value=0)

        x = (features.values - self.mean) / self.std
        x_tensor = torch.tensor(x, dtype=torch.float32).to(self.device)

        with torch.no_grad():
            recon = self.model(x_tensor)
            error = ((recon - x_tensor) ** 2).mean().item()

        prob = error  # treat error as anomaly score
        is_anomaly = error > self.threshold


        if is_anomaly:
            self.anomaly_count += 1
            result = {
                "BlockId": row.get("BlockId"),
                "prediction": 1,
                "score": float(prob),
                "model_type": self.model_type,
                "model_version": "v1"
            }
            self.producer.send(ANOMALY_TOPIC, result)
            print(f"[Inference #{self.total_inferences}] Anomaly #{self.anomaly_count}: {result}")
        else:
            print(f"[Inference #{self.total_inferences}] Normal row (score={prob:.3f}), no anomaly.")

    def run(self):
        print(f"Starting inference with {self.model_type.upper()} model ...")
        for msg in self.consumer:
            row = msg.value
            self.run_inference(row)


if __name__ == "__main__":
    service = StreamingInferenceService()
    service.run()
