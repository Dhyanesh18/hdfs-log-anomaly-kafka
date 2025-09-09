# HDFS Log Anomaly Detection Project

<img width="676" height="721" alt="Untitled Diagram drawio" src="https://github.com/user-attachments/assets/cc0c6ba9-4d03-4d59-9f67-d936dfd95c2b" />


## Overview

This project implements a **modular streaming anomaly detection pipeline** for HDFS logs using **Kafka for streaming** and **MLflow for experiment tracking**.

The system supports **multiple anomaly detection backends** (currently RandomForest and Autoencoder) and provides services for **real-time inference** and **incremental retraining**.

### Key Features

* **Modular pipeline** – interchangeable detection models (RandomForest, Autoencoder, future extensions).
* Dual-mode input:

  * **Raw logs** → block-wise event distributions.
  * **Preprocessed features** (CSV).
* **Aggregator service** to convert raw logs into per-block event distributions.
* **Streaming inference service** with threshold-based anomaly detection.
* **Retrainer service** for incremental or batch updates.
* **Metrics tracking** (ROC AUC, Precision, Recall, F1, Confusion Matrix).
* Scalable Kafka-based architecture.

---

## Kafka Topics

| Topic               | Partitions | Purpose                                                    |
| ------------------- | ---------- | ---------------------------------------------------------- |
| `raw_logs`          | 50         | Raw HDFS logs (key = BlockId).                             |
| `aggregated_events` | 1          | Aggregated event distributions per block.                  |
| `anomalies`         | 1          | Blocks flagged as anomalous.                               |
| `hdfs_logs`         | 1          | Preprocessed block features (optional for test streaming). |

---

## Project Components

### 1. **Raw Log Producer (`raw_producer.py`)**

* Reads raw `HDFS.log` line-by-line.
* Extracts **block IDs** (`blk_*`).
* Streams logs to `raw_logs` (Kafka).
* Supports **throttling** to simulate real-time streaming.

### 2. **Aggregator (`aggregate_consumer.py`)**

* Consumes from `raw_logs`.
* Buffers per block and maps logs to **event IDs (E1–E29 + UNKNOWN)**.
* Flushes based on:

  * **Min logs per block** (normal flush).
  * **Timeout** (delayed block closure).
* Publishes **event occurrence vectors** to `aggregated_events`.

### 3. **Streaming Inference (`inference.py`)**

* Consumes `aggregated_events`.
* Loads a chosen model:

  * **RandomForest (`random_forest_hdfs.pkl`)**
  * **Autoencoder (`autoencoder_hdfs.pth`)**
* Detects anomalies based on:

  * RF: probability threshold (default = 0.75).
  * AE: reconstruction error percentile (configurable).
* Publishes anomalies to `anomalies`.

### 4. **Mini-batch / CSV Producer (`stream_test_data.py`)**

* Streams **preprocessed CSV test data** (`Event_occurrence_matrix_test.csv`).
* Supports **mini-batches** and **delayed streaming** for evaluation.

### 5. **Streaming Retrainer (`retrainer.py`)**

* Consumes `raw_logs` or `hdfs_logs`.
* Buffers rows until `batch_size` is reached.
* Retrains models:

  * RF: refits on combined dataset.
  * AE: continues training with new batches.
* Updates model checkpoint in `models/`.

---

## Workflow Diagram

```
Raw HDFS Logs
    |
    v
Raw Log Producer  → Kafka raw_logs (50 partitions)
    |
    v
Aggregator
 - Buffer logs per block
 - Map → event IDs
 - Flush (min logs / timeout)
    |
    v
Kafka aggregated_events
    |
    v
Streaming Inference
 - RandomForest or Autoencoder
 - Threshold anomaly detection
 - Send to Kafka anomalies
    |
    v
Streaming Retrainer
 - Batch updates
 - Save new model in models/
```

---

## Usage

### 1. Train initial model

RandomForest:

```bash
python utils/train.py
```

Autoencoder:

```bash
python utils/train_ae.py
```

### 2. Start Kafka & Zookeeper

```bash
docker-compose up -d
```

### 3. Check logs

```bash
docker-compose logs -f inference
docker-compose logs -f aggregate-consumer
```
---

## Evaluation

* **RandomForest:** Precision, Recall.
* **Autoencoder:** Reconstruction error distribution, ROC AUC, percentile thresholding.
* Default evaluation uses `sklearn.metrics` and reports confusion matrix + ROC curve.

---

## Notes & Future Work

1. **Incremental models:**

   * Autoencoder can train online in mini-batches.
   * RF retrains in batch; future: switch to `SGDClassifier` / online ensembles.

2. **Scalability:**

   * Raw logs → 50 partitions = horizontal scaling.
   * Aggregated events = 1 partition (can scale later).

3. **Monitoring & Visualization:**

   * Stream anomalies to Grafana or Plotly dashboards.
   * Log MLflow metrics per model version.

4. **Extensions:**

   * Add **Transformer-based anomaly detection**.
   * Deploy inference as REST/gRPC microservice.

---

## Directory Structure

```
├── HDFS_v1/
│   └── HDFS.log
├── preprocessed/
│   ├── Event_occurrence_matrix_train.csv
│   └── Event_occurrence_matrix_test.csv
├── models/
│   ├── random_forest_hdfs.pkl
│   └── autoencoder_hdfs.pth
├── services/
│   ├── aggregate_consumer.py
│   ├── inference.py
│   ├── labelled_producer.py
│   ├── raw_producer.py
│   ├── retrainer_randomforest.py
│   └── retrainer.py
├── docker/
│   └── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```
