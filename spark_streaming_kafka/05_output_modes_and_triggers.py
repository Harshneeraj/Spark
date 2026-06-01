"""
Topic: Output Modes and Triggers
==================================

Output modes control WHAT is written, triggers control WHEN.

Spark UI Behavior:
- Each trigger fires a micro-batch -> 1 job in Spark UI.
- processingTime="10 seconds": Job every 10 seconds (if data available).
- once=True: Single job, then query stops.
- Default trigger: Jobs fire back-to-back (ASAP).
- Complete mode: Each batch outputs FULL result (more data written).
- Append mode: Each batch outputs only NEW rows (less data written).

Key Interview Points:
- Output mode determines what rows are written to sink.
- Not all modes work with all operations.
- Append: Only new rows. Cannot use with aggregations (without watermark).
- Complete: Full result table. Only for aggregations.
- Update: Only changed rows. For aggregations.
- Trigger determines when each batch executes.
- Checkpoint is REQUIRED for fault tolerance.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, count, sum, avg, current_timestamp,
    from_json, to_json, struct, expr
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder \
    .appName("05_Output_Modes_Triggers") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# ============ OUTPUT MODES EXPLAINED ============
"""
┌──────────────┬────────────────────────────────────────────────────────────────┐
│ Mode         │ Behavior                                                       │
├──────────────┼────────────────────────────────────────────────────────────────┤
│ APPEND       │ Only NEW rows added since last trigger are written.            │
│              │ Rows once written are never changed.                           │
│              │ Use for: Simple transforms (filter, map), no aggregation.      │
│              │ With aggregation: Only with watermark (finalized windows).     │
│              │                                                                │
│ COMPLETE     │ ENTIRE result table is written every trigger.                  │
│              │ Previous output is overwritten.                                │
│              │ Use for: Aggregations where you need full result.              │
│              │ Warning: Result table grows unbounded without watermark!       │
│              │                                                                │
│ UPDATE       │ Only CHANGED/NEW rows are written.                             │
│              │ More efficient than Complete for large state.                  │
│              │ Use for: Aggregations where you only need deltas.             │
│              │ Not supported by all sinks (e.g., file sink doesn't support). │
└──────────────┴────────────────────────────────────────────────────────────────┘

COMPATIBILITY MATRIX:
┌─────────────────────────────┬────────┬──────────┬────────┐
│ Query Type                  │ Append │ Complete │ Update │
├─────────────────────────────┼────────┼──────────┼────────┤
│ No aggregation (map/filter) │ ✓      │ ✗        │ ✓      │
│ Aggregation (no watermark)  │ ✗      │ ✓        │ ✓      │
│ Aggregation (with watermark)│ ✓      │ ✓        │ ✓      │
│ mapGroupsWithState          │ ✓      │ ✗        │ ✓      │
│ flatMapGroupsWithState      │ ✓/✓    │ ✗        │ ✓      │
└─────────────────────────────┴────────┴──────────┴────────┘
"""

# Sample streaming-like data
events = [
    ("2024-01-01 10:00:00", "user_1", "click", 1.0),
    ("2024-01-01 10:00:30", "user_2", "click", 1.0),
    ("2024-01-01 10:01:00", "user_1", "purchase", 50.0),
    ("2024-01-01 10:01:30", "user_3", "click", 1.0),
    ("2024-01-01 10:02:00", "user_2", "purchase", 30.0),
    ("2024-01-01 10:02:30", "user_1", "click", 1.0),
    ("2024-01-01 10:03:00", "user_4", "purchase", 75.0),
    ("2024-01-01 10:03:30", "user_2", "click", 1.0),
    ("2024-01-01 10:04:00", "user_3", "purchase", 100.0),
    ("2024-01-01 10:05:00", "user_1", "purchase", 25.0),
]

from pyspark.sql.functions import to_timestamp
df = spark.createDataFrame(events, ["event_time", "user_id", "event_type", "value"])
df = df.withColumn("event_time", to_timestamp("event_time"))

# ============ APPEND MODE EXAMPLE ============
"""
# Simple transformation - no aggregation -> APPEND works

query_append = df_stream \
    .filter(col("event_type") == "purchase") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Each batch outputs ONLY the new purchase events from that batch.
# Previously output rows are never repeated.
"""

print("=== APPEND Mode: Only new rows (no aggregation) ===")
df_append = df.filter(col("event_type") == "purchase")
df_append.show()
print("In streaming: Each batch would output only NEW purchases since last batch.\n")

# ============ COMPLETE MODE EXAMPLE ============
"""
# Aggregation -> COMPLETE outputs full result every time

query_complete = df_stream \
    .groupBy("event_type") \
    .agg(count("*").alias("total_count"), sum("value").alias("total_value")) \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Every batch outputs the ENTIRE aggregation result.
# Batch 1: {click: 2, purchase: 1}
# Batch 2: {click: 4, purchase: 3}  <- FULL result, not just delta
"""

print("=== COMPLETE Mode: Full result table every batch ===")
df_complete = df.groupBy("event_type").agg(
    count("*").alias("total_count"),
    sum("value").alias("total_value")
)
df_complete.show()
print("In streaming: ENTIRE result table output every batch (grows over time).\n")

# ============ UPDATE MODE EXAMPLE ============
"""
# Aggregation -> UPDATE outputs only changed rows

query_update = df_stream \
    .groupBy("event_type") \
    .agg(count("*").alias("total_count")) \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

# Each batch outputs only rows whose aggregation CHANGED.
# Batch 1: {click: 2, purchase: 1}  <- both new
# Batch 2: {click: 4}               <- only click changed this batch
"""

print("=== UPDATE Mode: Only changed rows ===")
print("In streaming: Only rows with updated aggregation values are output.\n")

# ============ TRIGGERS ============
"""
┌─────────────────────────────────────┬─────────────────────────────────────────┐
│ Trigger                             │ Behavior                                │
├─────────────────────────────────────┼─────────────────────────────────────────┤
│ (default - no trigger specified)    │ Process next batch ASAP after previous  │
│                                     │ completes. Lowest latency.              │
├─────────────────────────────────────┼─────────────────────────────────────────┤
│ .trigger(processingTime="10 seconds")│ Process batch every 10 seconds.        │
│                                     │ If processing takes > 10s, next batch   │
│                                     │ starts immediately after.               │
├─────────────────────────────────────┼─────────────────────────────────────────┤
│ .trigger(once=True)                 │ Process ALL available data in ONE batch │
│                                     │ then STOP. Like a batch job.            │
│                                     │ Good for: Scheduled runs (cron).        │
├─────────────────────────────────────┼─────────────────────────────────────────┤
│ .trigger(availableNow=True)         │ Process all available data in MULTIPLE  │
│                                     │ batches, then STOP. (Spark 3.3+)        │
│                                     │ Better than once for large backlogs.    │
├─────────────────────────────────────┼─────────────────────────────────────────┤
│ .trigger(continuous="1 second")     │ Continuous processing (experimental).   │
│                                     │ ~1ms latency. Limited operations.       │
└─────────────────────────────────────┴─────────────────────────────────────────┘
"""

# ============ TRIGGER EXAMPLES ============
"""
# Default trigger (ASAP)
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Fixed interval
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime="30 seconds") \
    .start()

# Once (batch-like, for scheduled jobs)
query = df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/shared/output") \
    .option("checkpointLocation", "/shared/checkpoint") \
    .trigger(once=True) \
    .start()
query.awaitTermination()  # Waits until the single batch completes

# Available now (Spark 3.3+)
query = df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/shared/output") \
    .option("checkpointLocation", "/shared/checkpoint") \
    .trigger(availableNow=True) \
    .start()
query.awaitTermination()
"""

# ============ CHOOSING OUTPUT MODE ============
"""
Decision Guide:

1. Simple ETL (filter, transform, no aggregation):
   → APPEND mode
   → Each new record processed once and written

2. Real-time dashboard (running counts/sums):
   → COMPLETE mode (if result is small)
   → UPDATE mode (if result is large, sink supports it)

3. Windowed aggregation with late data handling:
   → APPEND mode + watermark (output finalized windows)
   → UPDATE mode (output partial results as they change)

4. Writing to Kafka:
   → APPEND for simple transforms
   → UPDATE for aggregations (Kafka supports update)
   → COMPLETE rarely used with Kafka (rewrites everything)

5. Writing to files (Parquet/JSON):
   → APPEND only (files are immutable, can't update)
   → COMPLETE not supported for file sinks
"""

# ============ PRACTICAL: TRIGGER ONCE FOR SCHEDULED JOBS ============
"""
Pattern: Use Structured Streaming API with trigger(once=True) for
scheduled batch jobs that need exactly-once semantics.

Benefits over regular batch:
1. Automatic offset tracking (no manual bookkeeping)
2. Exactly-once semantics via checkpointing
3. Same code for both streaming and batch
4. Incremental processing (only new data since last run)

# Scheduled job (runs via cron/airflow every hour)
spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "events") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .writeStream \
    .format("parquet") \
    .option("path", "/shared/events_table") \
    .option("checkpointLocation", "/shared/checkpoints/events") \
    .trigger(once=True) \
    .start() \
    .awaitTermination()
"""

# Write demo
df.write.mode("overwrite").parquet("/shared/output_modes_demo")

spark.stop()
