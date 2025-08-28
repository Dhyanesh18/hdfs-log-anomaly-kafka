from kafka import KafkaProducer
import pandas as pd
import json
import time

# Load dataset
df = pd.read_csv("preprocessed/Event_occurrence_matrix_test.csv")

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

mini_batch_size = 50   # smaller batches for smoother streaming
stream_delay = 0.01    # 10ms between mini-batches

num_batches = (len(df) + mini_batch_size - 1) // mini_batch_size

for batch_idx in range(num_batches):
    start_idx = batch_idx * mini_batch_size
    end_idx = min(start_idx + mini_batch_size, len(df))
    mini_batch = df.iloc[start_idx:end_idx].to_dict(orient="records")
    
    producer.send('hdfs_logs', mini_batch)
    print(f"Sent mini-batch {batch_idx + 1}/{num_batches}, size: {len(mini_batch)}")
    
    time.sleep(stream_delay)  # simulate real-time streaming

producer.flush()
print("All data streamed.")
