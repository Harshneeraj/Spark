"""
Topic: Spark Structured Streaming Fundamentals
================================================

Structured Streaming treats a live data stream as an unbounded table
that is being continuously appended to.

Spark UI Behavior:
- Streaming tab appears in Spark UI with active queries.
- Each micro-batch = 1 job in Spark UI.
- Metrics: Input rate, processing rate, batch duration, latency.
- Stages per batch depend on query complexity (same as batch).
- "Streaming Query" panel shows: inputRowsPerSecond, processedRowsPerSecond.

Key Interview Points:
- Structured Streaming is built on the Spark SQL engine (same optimizer).
- Micro-batch processing (not true record-by-record like Flink).
- Exactly-once semantics with proper checkpointing.
- Same DataFrame/Dataset API as batch (unified programming model).
- Fault-tolerant via checkpointing (offsets + state).
- Sources: Kafka, Files, Socket, Rate (testing).
- Sinks: Kafka, Files, Console, Memory, Foreach, ForeachBatch.

Execution Model:
┌─────────────────────────────────────────────────────────────────┐
│  Unbounded Input Table (stream)                                  │
│  ┌──────┬──────┬──────┬──────┬──────┬─────────                 │
│  │Batch0│Batch1│Batch2│Batch3│Batch4│ ...                       │
│  └──────┴──────┴──────┴──────┴──────┴─────────                 │
│       │                                                          │
│       ▼                                                          │
│  ┌─────────────────────────────────────┐                        │
│  │  Query (same as batch DataFrame)     │                        │
│  │  filter -> groupBy -> agg            │                        │
│  └─────────────────────────────────────┘                        │
│       │                                                          │
│       ▼                                                          │
│  Result Table (updated with each batch)                          │
│       │                                                          │
│       ▼                                                          │
│  Output Sink (Kafka, Files, Console)                             │
└─────────────────────────────────────────────────────────────────┘
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr

spark = SparkSession.builder \
    .appName("01_Streaming_Fundamentals") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# ============ STREAMING vs BATCH COMPARISON ============
"""
┌─────────────────────┬──────────────────────┬──────────────────────┐
│ Aspect              │ Batch Processing     │ Structured Streaming │
├─────────────────────┼──────────────────────┼──────────────────────┤
│ Input               │ Bounded (fixed)      │ Unbounded (growing)  │
│ Execution           │ Run once, complete   │ Continuous/triggered │
│ API                 │ spark.read           │ spark.readStream     │
│ Output              │ df.write             │ df.writeStream       │
│ State               │ None                 │ Checkpointed         │
│ Fault Tolerance     │ Rerun from start     │ Resume from checkpoint│
│ Latency             │ Minutes-hours        │ Seconds-minutes      │
│ Semantics           │ Exactly-once (retry) │ Exactly-once (ckpt)  │
└─────────────────────┴──────────────────────┴──────────────────────┘
"""

# ============ BASIC STREAMING CONCEPTS ============
"""
THREE CORE CONCEPTS:

1. INPUT SOURCE (where data comes from):
   - Kafka: Most common in production
   - File source: Monitor directory for new files
   - Socket: TCP socket (testing only)
   - Rate: Generate data at fixed rate (testing)

2. QUERY (what transformation to apply):
   - Same as batch: select, filter, groupBy, join, window
   - Additional: watermark, deduplication, stateful operations

3. OUTPUT SINK (where results go):
   - Kafka: Write back to Kafka topics
   - File: Write to Parquet/JSON/CSV files
   - Console: Print to stdout (debugging)
   - Memory: Store in-memory table (debugging)
   - Foreach/ForeachBatch: Custom logic per row/batch

OUTPUT MODES:
   - Append: Only new rows (no aggregation, or with watermark)
   - Complete: Entire result table (aggregations)
   - Update: Only changed rows (aggregations)

TRIGGERS:
   - Default: Process ASAP after previous batch
   - Fixed interval: processingTime="10 seconds"
   - Once: Process all available, then stop (batch-like)
   - AvailableNow: Process all available in multiple batches, then stop
"""

# ============ RATE SOURCE (for testing without Kafka) ============
# Generates rows with (timestamp, value) at specified rate

print("=== Rate Source Demo (testing) ===")
df_rate = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 5) \
    .option("numPartitions", 2) \
    .load()

print("Rate source schema:")
df_rate.printSchema()
# root
#  |-- timestamp: timestamp
#  |-- value: long

# Apply transformations (same as batch!)
df_transformed = df_rate \
    .withColumn("category", (col("value") % 3).cast("string")) \
    .withColumn("processed_at", current_timestamp()) \
    .filter(col("value") > 2)

# Write to console (for debugging)
# NOTE: In production, this would run continuously
# query = df_transformed.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .trigger(processingTime="5 seconds") \
#     .start()
# query.awaitTermination(30)  # Run for 30 seconds
# query.stop()

# ============ CHECKING IF DATAFRAME IS STREAMING ============
print(f"\nIs df_rate streaming? {df_rate.isStreaming}")

# ============ BATCH EQUIVALENT (for understanding) ============
# Same logic in batch mode
print("\n=== Batch equivalent ===")
df_batch = spark.createDataFrame([
    ("2024-01-01 10:00:00", 1),
    ("2024-01-01 10:00:01", 2),
    ("2024-01-01 10:00:02", 3),
    ("2024-01-01 10:00:03", 4),
    ("2024-01-01 10:00:04", 5),
], ["timestamp", "value"])

df_batch.withColumn("category", (col("value") % 3).cast("string")) \
    .filter(col("value") > 2) \
    .show()

print("""
KEY TAKEAWAY: The transformation logic is IDENTICAL between batch and streaming.
Only the read (spark.read vs spark.readStream) and write (df.write vs df.writeStream) differ.
""")

spark.stop()
