# HDFS Log Anomaly Detection Project

## Overview

This project implements a **streaming anomaly detection pipeline** for HDFS logs using a **RandomForest-based supervised model**, Kafka for streaming, and MLflow for experiment tracking.

The system processes **raw HDFS logs line-by-line**, aggregates them per block, performs **anomaly detection**, and supports **incremental retraining**.

### Key Features:

- Dual-mode: can handle **preprocessed block features** or **raw logs**.
- **Aggregator service** to convert raw logs → block-wise event distributions.
- **Streaming inference** with a RandomForest model.
- **Streaming retrainer** for incremental model updates.
- Detailed logging and metrics tracking.

---

## Kafka Topics

| Topic               | Partitions | Purpose                                                        |
| ------------------- | ---------- | -------------------------------------------------------------- |
| `raw_logs`          | 50         | Raw HDFS logs (key=BlockId)                                    |
| `aggregated_events` | 1          | Aggregated event distributions per block                       |
| `anomalies`         | 1          | Anomalous blocks flagged by the model                          |
| `hdfs_logs`         | 1          | Preprocessed block-wise feature rows (optional test streaming) |

---

## Project Components

### 1. **Raw Log Producer (`raw_producer.py`)**

- Reads HDFS logs line-by-line (`HDFS_v1/HDFS.log`).
- Extracts **block IDs** using regex (`blk_*`).
- Streams logs to Kafka `raw_logs` topic (key=BlockId).
- Supports throttling to simulate real-time streaming.

### 2. **Aggregator (`aggregate_consumer.py`)**

- Consumes `raw_logs`.
- Buffers logs per block, maps to **event IDs (E1–E29 + UNKNOWN)** using `EventExtractor`.
- Flushes:

  - **Normal flush:** `MIN_LOGS_PER_BLOCK` logs collected.
  - **Timeout flush:** `BLOCK_TIMEOUT` seconds elapsed.

- Publishes aggregated events to `aggregated_events`.

### 3. **Streaming Inference (`inference.py`)**

- Consumes `aggregated_events`.
- Loads a **trained RandomForest model** (`random_forest_hdfs.pkl`).
- Performs anomaly detection with probability threshold (default 0.75).
- Sends anomalies to `anomalies` topic.

### 4. **Mini-batch / CSV Producer (`stream_test_data.py`)**

- Streams preprocessed CSV (`Event_occurrence_matrix_test.csv`) for testing.
- Supports **mini-batch streaming** with delays.

### 5. **Streaming Retrainer (`retrainer.py`)**

- Consumes either `raw_logs` or `hdfs_logs` for retraining.
- Buffers incoming rows in memory until `batch_size` is reached.
- Retrains RandomForest on **combined dataset** (historical + new batch).
- Updates latest model (`models/model_base.joblib`).

---

## Kafka Setup

### 1. Start Kafka & Zookeeper

```bash
docker-compose up -d
```

### 2. Create Topics

```bash
# Raw HDFS logs
docker exec -it kafka kafka-topics \
  --create \
  --topic raw_logs \
  --bootstrap-server localhost:9092 \
  --partitions 50 \
  --replication-factor 1

# Aggregated event distributions
docker exec -it kafka kafka-topics \
  --create \
  --topic aggregated_events \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

# Anomalous blocks
docker exec -it kafka kafka-topics \
  --create \
  --topic anomalies \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

# Optional test streaming (preprocessed CSV)
docker exec -it kafka kafka-topics \
  --create \
  --topic hdfs_logs \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

### 3. Verify Topics

```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

Expected output:

```
raw_logs
aggregated_events
anomalies
hdfs_logs
```

---

## Workflow Diagram

```
Raw HDFS Logs (line by line)
            |
            v
    Raw Log Producer → Kafka raw_logs (50 partitions)
            |
            v
     Aggregator Service
 - Buffers logs per block
 - Maps logs → event IDs
 - Normal / Timeout flush
            |
            v
Kafka aggregated_events (1 partition)
            |
            v
Streaming Inference Service
 - Run RandomForest
 - Detect anomalies
 - Send anomalies → Kafka anomalies (1 partition)
            |
            v
Optional: Streaming Retrainer
 - Append rows to training CSV
 - Retrain model periodically
 - Update models/model_base.joblib
```

---

## Usage

### 1. Start Kafka & Zookeeper

```bash
docker-compose up -d
```

### 2. Train initial model

```bash
python first_model.py
```

### 3. Start Raw Log Producer (optional if testing with CSV)

```bash
python scripts/raw_producer.py
```

### 4. Start Aggregator Service

```bash
python scripts/aggregate_consumer.py
```

### 5. Start Streaming Inference

```bash
python scripts/inference.py
```

### 6. Stream Preprocessed CSV (for testing)

```bash
python scripts/labelled_producer.py
```

### 7. Start Streaming Retrainer

```bash
python scripts/retrainer.py
```

---

## Notes & Future Improvements

1. **Incremental models:**

   - Future: Replace RandomForest with incremental models using `partial_fit`.

2. **Scaling:**

   - Raw logs use 50 Kafka partitions → can scale multiple aggregators.
   - Aggregated events and anomalies are single-partitioned but can be increased for production.

3. **Monitoring & Visualization:**

   - Stream anomalies to dashboards (Grafana / Plotly) for operational monitoring.

---

## Directory Structure

```
├── HDFS_v1/
│   └── HDFS.log
├── preprocessed/
│   ├── Event_occurrence_matrix_train.csv
│   └── Event_occurrence_matrix_test.csv
├── models/
│   └── random_forest_hdfs.pkl
├── scripts/
│   ├── aggregate_consumer.py
│   ├── inference.py
│   ├── labelled_producer.py
│   ├── raw_producer.py
│   └── retrainer.py
├── Dockerfile.*
├── docker-compose.yml
├── requirements.txt
└── README.md
```
