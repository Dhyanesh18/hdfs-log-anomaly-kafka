from kafka import KafkaConsumer, KafkaProducer
import json
import time
import threading
import logging
from collections import defaultdict, Counter
from utils.event_extractor import EventExtractor

# --- Config ---
RAW_TOPIC = "raw_logs"
AGG_TOPIC = "aggregated_events"
BOOTSTRAP = "localhost:9092"
GROUP_ID = "aggregator_group"
MIN_LOGS_PER_BLOCK = 100   # flush after 100 logs
BLOCK_TIMEOUT = 300        # flush partial blocks after 300 seconds

# --- Logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

extractor = EventExtractor()

# --- Kafka setup ---
consumer = KafkaConsumer(
    RAW_TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: v.decode("utf-8"),
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# --- Block buffer ---
block_buffers = defaultdict(lambda: {"events": Counter(), "count": 0, "start_time": time.time()})
buffer_lock = threading.Lock() 

def create_aggregated_row(block_id, events_counter, total_count):
    """Create the aggregated row with proper event mapping"""
    row = {"BlockId": block_id, "total_logs": total_count}
    
    # Initialize all event types to 0
    for i in range(1, 30):
        row[f"E{i}"] = 0

    row["UNKNOWN"] = 0

    classified_count = 0
    unknown_count = 0
    
    for event_id, count in events_counter.items():
        if event_id.startswith('E') and event_id[1:].isdigit():  # E1-E29 events
            if event_id in row:
                row[event_id] = count
                classified_count += count
        elif event_id == "UNKNOWN":
            row["UNKNOWN"] = count
            unknown_count = count
        else:
            # Log truly unexpected event types
            logging.warning(f"Unexpected event type '{event_id}' found for block {block_id}: {count} occurrences")
    
    # Verify totals match
    total_classified = classified_count + unknown_count
    if total_classified != total_count:
        logging.error(f"Block {block_id}: Mismatch! total_logs={total_count}, classified+unknown={total_classified}")
    
    # Debug: Log the events distribution (only non-zero)
    non_zero_events = {k: v for k, v in events_counter.items() if v > 0}
    if non_zero_events:
        logging.info(f"Block {block_id} event distribution: {dict(non_zero_events)}")
    
    return row

# --- Timeout flusher ---
def flush_expired_blocks():
    while True:
        now = time.time()
        expired = []
        with buffer_lock:
            for block_id, buf in block_buffers.items():
                age_seconds = now - buf["start_time"]
                if age_seconds >= BLOCK_TIMEOUT:
                    expired.append(block_id)
                    # Debug: Log why this block is being flushed
                    logging.info(f"Block {block_id} expired: age={age_seconds:.1f}s, threshold={BLOCK_TIMEOUT}s, logs={buf['count']}")
            
            for block_id in expired:
                buf = block_buffers.pop(block_id)
                
                row = create_aggregated_row(block_id, buf["events"], buf["count"])
                
                try:
                    future = producer.send(AGG_TOPIC, row)
                    future.get(timeout=10)
                    logging.info(f"Timeout flush block {block_id}: {buf['count']} logs after {now - buf['start_time']:.1f} seconds")
                except Exception as e:
                    logging.error(f"Failed to send timeout-flushed block {block_id}: {e}")
        
        time.sleep(1)

# Start timeout thread
threading.Thread(target=flush_expired_blocks, daemon=True).start()
logging.info("Aggregator started...")

# Debug counter to track processed messages
processed_count = 0

# --- Main consumer loop ---
for msg in consumer:
    log_line = msg.value
    block_id = msg.key or "unknown"
    
    # Extract event ID and add debugging
    event_id = extractor.extract_event_id(log_line)
    
    # Debug: Print first few extractions
    processed_count += 1
    if processed_count <= 10:
        logging.info(f"Sample extraction #{processed_count}: block='{block_id}', event='{event_id}', log='{log_line[:100]}...'")
    
    with buffer_lock:
        buf = block_buffers[block_id]
        buf["events"][event_id] += 1
        buf["count"] += 1
        
        # Debug: Log when blocks are close to flushing
        if buf["count"] in [5, 10, 15]:
            age = time.time() - buf["start_time"]
            logging.info(f"Block {block_id} has {buf['count']} logs, age: {age:.1f}s")
        
        # Flush if enough logs
        if buf["count"] >= MIN_LOGS_PER_BLOCK:
            row = create_aggregated_row(block_id, buf["events"], buf["count"])
            
            try:
                future = producer.send(AGG_TOPIC, row)
                future.get(timeout=10)
                age = time.time() - buf["start_time"]
                logging.info(f"Normal flush block {block_id}: {buf['count']} logs after {age:.1f} seconds")
                logging.info(f"Event distribution: {dict(buf['events'])}")
                
                # Show the final row being sent (limited for readability)
                non_zero_events = {k: v for k, v in row.items() if k.startswith('E') and v > 0}
                logging.info(f"Non-zero events in row: {non_zero_events}")
                
            except Exception as e:
                logging.error(f"Failed to send flushed block {block_id}: {e}")
            
            block_buffers.pop(block_id, None)