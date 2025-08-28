# HDFS Log Anomaly Detection Project

## Overview

This project implements a **streaming anomaly detection pipeline** for HDFS logs using a **RandomForest-based supervised model**, Kafka for streaming, and MLflow for experiment tracking.

The system is designed to detect anomalous block behaviors based on **event distributions** extracted from logs. While the current implementation assumes preprocessed **block-wise event distributions**, the pipeline is adaptable to **raw log streaming**, where logs are processed per line to extract event distributions dynamically.

---

## Project Assumptions

1. **Block-wise aggregation for features**

   * The current dataset (`Event_occurrence_matrix_train.csv`) contains **event counts per block**.
   * Each row represents a block with counts for each HDFS event (E1–E29).

2. **Raw log processing (production scenario)**

   * HDFS logs are typically individual lines, each representing an event.
   * In a production scenario, a **preprocessing step** could:

     1. Extract the **event ID** from each log line.
     2. Accumulate logs per block to create a **block-wise event distribution vector**.
     3. Once a block has enough logs (e.g., 20), stream its distribution vector to Kafka for inference.

> **Important:** In this project, we have **assumed that block-wise event distributions are already preprocessed**. We directly stream these distributions to Kafka instead of processing raw log lines. This simplifies the workflow while still allowing the core inference and retraining logic to be tested and demonstrated.

3. **Streaming setup**

   * Kafka topics simulate streaming:

     * `hdfs_logs` → Preprocessed per-block event distributions.
     * `hdfs_predictions` → Anomalies flagged by the model which can be consumed for sending alerts or dashboard data.

4. **Inference and retraining**

   * The consumer reads block distributions from Kafka, runs inference, and flags anomalies.
   * New block distributions are appended to the training CSV.
   * The model retrains after a configurable batch size (currently 5000 blocks).

---

## Project Components

### 1. **Training Script (`train.py`)**

* Trains a **RandomForestClassifier** on block-wise event distributions.
* Logs parameters, metrics, and models to **MLflow**.
* Saves the latest model as `models/model_base.joblib`.

**Key features:**

* Balanced class handling (`Success` vs `Fail`)
* Train/test split with stratification
* Retraining on updated dataset

---

### 2. **Streaming Retrainer (`streaming_retrainer.py`)**

* Kafka consumer reads mini-batches of preprocessed blocks.
* Maintains an **in-memory buffer** for batch retraining.
* Runs inference on incoming data: only **anomalous blocks are sent** to `hdfs_predictions`.
* Retrains model after buffer reaches the batch size.

**Workflow:**

1. Receive block-wise feature rows via Kafka.
2. Run model inference for anomaly detection.
3. Stream anomalies to Kafka.
4. Append new rows to CSV for retraining.

---

### 3. **Kafka Streaming Script (`stream_to_kafka.py`)**

* Streams preprocessed block-wise features or mini-batches to Kafka.
* Supports small batch sizes and simulated delays to mimic real-time streaming.

---

## Kafka Setup

1. **Start Kafka and Zookeeper**

```bash
docker-compose up -d
```

2. **Create Kafka Topics**

```bash
# Topic for incoming preprocessed block/event distributions
docker exec -it kafka kafka-topics \
  --create \
  --topic hdfs_logs \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

# Topic for anomalies detected by the model
docker exec -it kafka kafka-topics \
  --create \
  --topic hdfs_predictions \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

3. **Verify Topics**

```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

Expected output:

```
hdfs_logs
hdfs_predictions
```

> Notes:
>
> * `--partitions 1` is sufficient for local testing.
> * `--replication-factor 1` is required for single-broker setup.
> * In production, consider multiple partitions for parallelism and higher replication for fault tolerance.

---

## Workflow Diagram

```
Raw HDFS Logs (line by line)
            |
            v
     Preprocessing Script
  (extract event IDs per log)
            |
            v
   Block-wise aggregation buffer
  (wait until N logs per block)
            |
            v
Kafka Producer → Topic: hdfs_logs
            |
            v
Streaming Retrainer / Inference Consumer
  - Run RandomForest inference
  - Stream anomalies → Topic: hdfs_predictions
  - Append block to training CSV
            |
            v
Periodic Retraining (every batch_size blocks)
```

---

## Usage

1. **Start Kafka & Zookeeper**

```bash
docker-compose up -d
```

2. **Train initial model**

```bash
python first_model.py
```

3. **Create topics in Kafka**
```
docker exec -it kafka kafka-topics \
  --create \
  --topic hdfs_logs \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

# Topic for anomalies detected by the model
docker exec -it kafka kafka-topics \
  --create \
  --topic hdfs_predictions \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

5. **Stream data to Kafka**

```bash
python producer.py
```

5. **Run streaming retrainer / inference consumer**

```bash
python consumer.py
```

---

## Notes & Future Improvements

1. **Raw log ingestion:**

   * Currently, we assume preprocessed block distributions.
   * In production, raw logs can be streamed line-by-line, then aggregated per block dynamically.

2. **Sliding-window aggregation:**

   * Maintain a rolling window of recent logs per block for more adaptive features.

3. **Incremental model training:**

   * Future versions could replace RandomForest with other models that use partial_fit or are incrementaly trainable instead of retraining with whole dataset.

4. **Dynamic retraining:**

   * Retraining frequency can be based on number of anomalies or stream volume.

5. **Scaling:**

   * Multiple Kafka partitions and distributed consumers can handle large HDFS clusters efficiently.

---

## Directory Structure

```
├── preprocessed/
│   ├── Event_occurrence_matrix_train.csv
│   └── Event_occurrence_matrix_test.csv
├── models/
│   └── model_base.joblib
├── train.py
├── streaming_retrainer.py
├── stream_to_kafka.py
├── docker-compose.yml
└── README.md
```

---

This version now **explicitly acknowledges the assumption about raw log streaming** while explaining how it could be done in a production scenario.

---

If you want, I can also **add a section with example Kafka producer/consumer commands** for testing streaming end-to-end. It’s useful for someone who wants to quickly verify the pipeline.

Do you want me to add that?
