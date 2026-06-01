# Spark Structured Streaming + Kafka - Complete Cheatsheet

## Table of Contents
- [1. Core Concepts](#1-core-concepts)
- [2. Kafka Architecture](#2-kafka-architecture)
- [3. Reading from Kafka](#3-reading-from-kafka)
- [4. Writing to Kafka](#4-writing-to-kafka)
- [5. Output Modes](#5-output-modes)
- [6. Triggers](#6-triggers)
- [7. Watermarking](#7-watermarking)
- [8. Checkpointing](#8-checkpointing)
- [9. Stateful Operations](#9-stateful-operations)
- [10. Streaming Joins](#10-streaming-joins)
- [11. foreachBatch](#11-foreachbatch)
- [12. Exactly-Once Semantics](#12-exactly-once-semantics)
- [13. Optimization](#13-optimization)
- [14. Monitoring](#14-monitoring)
- [15. Interview Questions](#15-interview-questions)

---

## 1. Core Concepts

```
Streaming = Unbounded Table being continuously appended

spark.read      → Batch (bounded)
spark.readStream → Streaming (unbounded)

df.write        → Batch output
df.writeStream  → Streaming output
```

| Concept | Description |
|---------|-------------|
| Micro-batch | Default execution model. Processes data in small batches. |
| Trigger | Controls WHEN each batch executes |
| Output Mode | Controls WHAT rows are written to sink |
| Checkpoint | Stores progress for fault tolerance |
| Watermark | Defines how long to wait for late data |
| State | Data maintained across batches (aggregations, joins) |

---

## 2. Kafka Architecture

```
Topic: "orders" (logical channel)
├── Partition 0: [msg0][msg1][msg2][msg3]...  (ordered within partition)
├── Partition 1: [msg0][msg1][msg2]...
└── Partition 2: [msg0][msg1][msg2][msg3][msg4]...

Key → determines partition (hash(key) % num_partitions)
Offset → position within partition (like a bookmark)
Consumer Group → Spark acts as a consumer group
```

**Kafka message columns in Spark:**
| Column | Type | Description |
|--------|------|-------------|
| key | binary | Message key (cast to string) |
| value | binary | Message payload (deserialize!) |
| topic | string | Topic name |
| partition | int | Partition number |
| offset | long | Offset within partition |
| timestamp | timestamp | Message timestamp |

---

## 3. Reading from Kafka

```python
# Streaming read
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092") \
    .option("subscribe", "topic1,topic2") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 10000) \
    .option("failOnDataLoss", "false") \
    .load()

# Batch read (for testing/backfill)
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "my-topic") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()
```

### Deserialization Pattern
```python
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

schema = StructType([
    StructField("user_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("event_time", StringType())
])

df_parsed = df.select(
    col("key").cast("string").alias("key"),
    from_json(col("value").cast("string"), schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select("key", "data.*", "kafka_timestamp")
```

### Subscribe Options (use ONE)
| Option | Example | Use Case |
|--------|---------|----------|
| `subscribe` | `"topic1,topic2"` | Specific topics |
| `subscribePattern` | `"events-.*"` | Regex pattern |
| `assign` | `'{"topic":[0,1,2]}'` | Specific partitions |

### Starting Offsets
| Value | Behavior |
|-------|----------|
| `"earliest"` | Read from beginning (backfill) |
| `"latest"` | Read only new messages (default for streaming) |
| JSON | Specific offsets per partition |

---

## 4. Writing to Kafka

```python
# Prepare: Must have 'value' column (required), 'key' optional
df_output = df.select(
    col("user_id").alias("key"),
    to_json(struct("*")).alias("value")
)

# Streaming write
query = df_output.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "output-topic") \
    .option("checkpointLocation", "/checkpoints/my-query") \
    .outputMode("append") \
    .start()

# Batch write
df_output.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "output-topic") \
    .save()
```

---

## 5. Output Modes

| Mode | What's Written | Use With | Sink Support |
|------|---------------|----------|--------------|
| **append** | Only NEW rows | No agg, or agg+watermark | All sinks |
| **complete** | ENTIRE result table | Aggregations | Console, Memory, Kafka |
| **update** | Only CHANGED rows | Aggregations | Console, Memory, Kafka, Foreach |

```python
# Append (simple transforms, no aggregation)
query = df.writeStream.outputMode("append")...

# Complete (aggregations, full result each time)
query = df.groupBy("key").count().writeStream.outputMode("complete")...

# Update (aggregations, only deltas)
query = df.groupBy("key").count().writeStream.outputMode("update")...
```

**Compatibility:**
- No aggregation → append or update
- Aggregation without watermark → complete or update
- Aggregation with watermark → append, complete, or update

---

## 6. Triggers

```python
# Default: ASAP (lowest latency)
.trigger()

# Fixed interval
.trigger(processingTime="30 seconds")

# Once: Process all available, then stop (for scheduled jobs)
.trigger(once=True)

# Available now: Process all in multiple batches, then stop (Spark 3.3+)
.trigger(availableNow=True)
```

| Trigger | Latency | Throughput | Use Case |
|---------|---------|------------|----------|
| Default | Lowest | Medium | Real-time dashboards |
| Fixed interval | Medium | High | Most production jobs |
| Once | High | Highest | Scheduled batch (cron) |
| AvailableNow | High | Highest | Backfill/catch-up |

---

## 7. Watermarking

```python
# Watermark = max_event_time - threshold
df.withWatermark("event_time", "10 minutes") \
    .groupBy(window("event_time", "5 minutes"), "category") \
    .agg(count("*").alias("cnt"))
```

**How it works:**
```
max_event_time seen = 10:30
watermark threshold = 10 minutes
watermark = 10:20

Events with event_time < 10:20 → DROPPED (too late)
Events with event_time >= 10:20 → ACCEPTED
Windows ending before 10:20 → FINALIZED (output in append mode)
```

**Window Types:**
```python
# Tumbling (non-overlapping)
window("event_time", "5 minutes")

# Sliding (overlapping)
window("event_time", "10 minutes", "5 minutes")

# Session (gap-based, Spark 3.2+)
session_window("event_time", "10 minutes")
```

**Why watermark is needed:**
- Without: State grows FOREVER → OOM
- With: State bounded, old data cleaned up
- Enables append mode with aggregations

---

## 8. Checkpointing

```python
query = df.writeStream \
    .option("checkpointLocation", "hdfs:///checkpoints/my-query-v1") \
    ...
```

**What's stored:**
```
/checkpoints/my-query/
├── offsets/     # Kafka offsets per batch
├── commits/    # Completed batch markers
├── state/      # Stateful operation state
└── metadata    # Query metadata
```

**Rules:**
- Each query needs UNIQUE checkpoint location
- Use reliable storage (HDFS/S3/GCS, NOT local disk)
- Never share between queries
- Delete only for incompatible changes

**On restart:** Resumes from last committed batch (exactly-once).

---

## 9. Stateful Operations

| Operation | State Stored | Needs Watermark? |
|-----------|-------------|-----------------|
| `groupBy().agg()` | Running aggregation per key | YES (for cleanup) |
| `window().agg()` | Per-window aggregation | YES |
| `dropDuplicates()` | Set of seen keys | YES |
| Stream-Stream join | Buffered rows both sides | YES |

```python
# Deduplication
df.withWatermark("event_time", "10 minutes") \
    .dropDuplicates(["event_id"])

# Running aggregation
df.withWatermark("event_time", "10 minutes") \
    .groupBy("user_id") \
    .agg(count("*"), sum("amount"))
```

**State Store:** Use RocksDB for large state:
```python
spark.conf.set("spark.sql.streaming.stateStore.providerClass",
    "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
```

---

## 10. Streaming Joins

### Stream-Static Join
```python
# No watermark needed, static side is a lookup table
df_stream.join(broadcast(df_static), "key", "left")
```

### Stream-Stream Join
```python
# MUST have watermark + time constraint
df_left.withWatermark("left_time", "10 minutes") \
    .join(
        df_right.withWatermark("right_time", "10 minutes"),
        expr("""
            left.key = right.key AND
            right_time >= left_time AND
            right_time <= left_time + interval 5 minutes
        """),
        "inner"
    )
```

| Join Type | Stream-Static | Stream-Stream |
|-----------|:---:|:---:|
| Inner | ✓ | ✓ (watermark + time) |
| Left Outer | ✓ | ✓ (watermark on right) |
| Right Outer | ✓ | ✓ (watermark on left) |
| Full Outer | ✗ | ✗ |

---

## 11. foreachBatch

```python
def process_batch(batch_df: DataFrame, batch_id: int):
    # Write to multiple sinks
    batch_df.write.mode("append").parquet("/data/lake")
    
    # Aggregation to serving layer
    agg = batch_df.groupBy("category").agg(sum("amount"))
    agg.write.mode("overwrite").parquet("/data/serving")
    
    # Alerts to Kafka
    alerts = batch_df.filter(col("amount") > 1000)
    alerts.select(to_json(struct("*")).alias("value")) \
        .write.format("kafka").option("topic", "alerts").save()

query = df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/checkpoints/pipeline") \
    .trigger(processingTime="1 minute") \
    .start()
```

**Why foreachBatch:**
- Write to multiple sinks in one function
- Use any batch DataFrame operation
- Database upserts/merges
- Custom file management (coalesce)
- Idempotent writes using batch_id

---

## 12. Exactly-Once Semantics

```
Exactly-Once = Replayable Source + Checkpoint + Idempotent Sink
```

| Component | Requirement | How |
|-----------|-------------|-----|
| Source | Replayable | Kafka (re-read from offset) |
| Processing | Checkpointed | Spark checkpoint |
| Sink | Idempotent | Upsert, atomic write, idempotent producer |

**Idempotent Sink Patterns:**
- File sink: Built-in (atomic rename)
- Kafka sink: `kafka.enable.idempotence=true`
- Database: UPSERT / MERGE (not INSERT)
- Delta Lake: MERGE operation

---

## 13. Optimization

### Critical Settings for Streaming
```python
spark.sql.shuffle.partitions = 8          # Reduce from 200!
spark.sql.adaptive.enabled = true
maxOffsetsPerTrigger = 50000              # Backpressure
spark.sql.streaming.stateStore.providerClass = RocksDB  # Large state
```

### Performance Checklist
1. ✅ Kafka partitions >= Spark cores (read parallelism)
2. ✅ Reduce shuffle partitions (8-16 for streaming)
3. ✅ Broadcast small lookup tables
4. ✅ Always set watermark (bound state)
5. ✅ Use maxOffsetsPerTrigger (backpressure)
6. ✅ Use RocksDB for large state
7. ✅ Coalesce in foreachBatch before file writes
8. ✅ Parse JSON schema once (don't infer per batch)

### Key Rule
```
Processing Rate > Input Rate (otherwise backlog grows!)
```

---

## 14. Monitoring

```python
# Active queries
spark.streams.active

# Query progress
query.lastProgress
query.recentProgress
query.status

# Key metrics
progress = query.lastProgress
progress['inputRowsPerSecond']      # Input rate
progress['processedRowsPerSecond']  # Processing rate
progress['batchDuration']           # Batch time (ms)
progress['stateOperators'][0]['numRowsTotal']  # State size
```

### Red Flags
| Metric | Problem | Fix |
|--------|---------|-----|
| processedRows < inputRows | Falling behind | More resources, simplify query |
| batchDuration > trigger | Can't keep up | Increase trigger, optimize |
| numRowsTotal growing | Unbounded state | Add watermark! |
| inputRows = 0 | No data flowing | Check Kafka source |

---

## 15. Interview Questions

### Q: How does Spark Structured Streaming work?
**A:** It treats a stream as an unbounded table. Each trigger processes a micro-batch of new data using the same DataFrame API as batch. Checkpointing provides fault tolerance and exactly-once semantics.

### Q: What is a watermark and why is it needed?
**A:** A watermark defines how long to wait for late data (max_event_time - threshold). Without it, state grows forever causing OOM. With it, old state is cleaned up and windows can be finalized for append mode output.

### Q: Explain exactly-once semantics in Spark + Kafka.
**A:** Requires three things: 1) Replayable source (Kafka can re-read from any offset), 2) Checkpointed processing (Spark tracks offsets and state), 3) Idempotent sink (writing same data twice produces same result). If any breaks, you get at-least-once.

### Q: Difference between output modes?
**A:** Append outputs only new rows (no aggregation, or finalized windows with watermark). Complete outputs the entire result table every batch (for aggregations). Update outputs only changed rows (efficient for large aggregations).

### Q: Stream-stream join vs stream-static join?
**A:** Stream-static: one side is a regular DataFrame (lookup table), no watermark needed, no state for join. Stream-stream: both sides are streaming, requires watermark + time constraint, maintains state for both sides (buffered rows waiting for matches).

### Q: What happens when a streaming job fails?
**A:** On restart, Spark reads the checkpoint, finds the last committed batch, and replays from there. Kafka is replayable so the same offsets are re-read. The sink must be idempotent to handle the duplicate write.

### Q: How to handle late data?
**A:** Use `withWatermark("event_time", "threshold")`. Events within the threshold are accepted and update results. Events beyond the threshold are dropped. The threshold is a trade-off between accuracy and resource usage.

### Q: foreachBatch vs foreach?
**A:** foreachBatch receives a full DataFrame per batch (efficient, batch-level operations). foreach processes one row at a time (inefficient, per-record logic). Always prefer foreachBatch unless you need per-row processing.

### Q: How to write to multiple sinks?
**A:** Use foreachBatch. Inside the function, write the batch DataFrame to multiple destinations (data lake, database, Kafka, etc.) using standard batch write operations.

### Q: trigger(once=True) vs regular batch?
**A:** trigger(once=True) uses the streaming API but processes all available data in one batch then stops. Benefits: automatic offset tracking, exactly-once semantics, incremental processing. Perfect for scheduled jobs (cron/Airflow) that need streaming guarantees.

### Q: How to handle schema evolution in streaming?
**A:** Use permissive mode for JSON parsing (malformed → null). Use Avro with schema registry for backward/forward compatibility. For breaking changes, version your checkpoint path and accept reprocessing.

---

## Quick Reference: Complete Pipeline Template

```python
spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "input-topic") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 50000) \
    .option("failOnDataLoss", "false") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp("event_time")) \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(window("event_time", "5 minutes"), "category") \
    .agg(count("*").alias("count"), sum("amount").alias("revenue")) \
    .writeStream \
    .foreachBatch(my_sink_function) \
    .option("checkpointLocation", "hdfs:///checkpoints/my-pipeline-v1") \
    .trigger(processingTime="1 minute") \
    .start() \
    .awaitTermination()
```

---

## Dependencies

```bash
# spark-submit with Kafka package
spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    my_streaming_app.py

# For Avro support
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0
```

---

*All scripts write DataFrames to `/shared` path. Generated for Spark Streaming + Kafka interview preparation.*
