from kafka import KafkaProducer
import re
import time

# Regex to extract block IDs
block_pattern = re.compile(r"blk_[0-9\-]+")

# Kafka producer with string key + string value
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    value_serializer=lambda v: v.encode("utf-8"),
)

log_file = "HDFS_v1/HDFS.log"
topic = "raw_logs"
lines_per_second = 5

with open(log_file, "r") as f:
    batch = []
    for line in f:
        line = line.strip()
        if not line:
            continue

        # Extract block ID
        match = block_pattern.search(line)
        block_id = match.group(0) if match else "unknown"

        # Send to Kafka with key=block_id
        future = producer.send(topic, key=block_id, value=line)
        record_metadata = future.get(timeout=10)
        print(f"Sent log (key={block_id}) to partition {record_metadata.partition}, offset {record_metadata.offset}")


        batch.append(line)

        # Sleep after sending N lines
        if len(batch) >= lines_per_second:
            time.sleep(1)
            batch = []

producer.flush()
print("Finished streaming logs")
