"""
Topic: Structured Streaming Basics
====================================

Spark Structured Streaming treats streaming data as an unbounded table.

Spark UI Behavior:
- Streaming queries show up in Spark UI -> Structured Streaming tab.
- Each micro-batch = 1 job in Spark UI.
- Stages per batch depend on the query (same as batch processing).
- Metrics visible: input rate, processing rate, batch duration.
- Streaming queries run continuously until stopped.

Key Interview Points:
- Structured Streaming = micro-batch processing (not true record-by-record).
- Treats stream as an unbounded DataFrame/table.
- Same API as batch DataFrames (select, filter, groupBy, join).
- Output modes: append (new rows only), complete (full result), update (changed rows).
- Triggers: default (ASAP), fixed interval, once (single batch), available-now.
- Checkpointing: Required for fault tolerance (stores offsets + state).
- Watermarking: Handles late data in event-time processing.
- Sources: Kafka, files, socket, rate (testing).
- Sinks: Kafka, files, console, memory, foreach.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, sum, current_timestamp, expr

spark = SparkSession.builder \
    .appName("32_Structured_Streaming_Basics") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# ============ BATCH SIMULATION (for learning) ============
# In production, you'd read from Kafka/files. Here we simulate with rate source.

"""
# Reading from a streaming source (rate source for testing)
df_stream = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 10) \
    .load()

# This creates a streaming DataFrame with columns: timestamp, value

# Apply transformations (same as batch!)
df_processed = df_stream \
    .withColumn("category", (col("value") % 5).cast("string")) \
    .filter(col("value") > 5)

# Write to console sink (for debugging)
query = df_processed.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime="5 seconds") \
    .start()

# query.awaitTermination()  # Blocks until stopped
# query.stop()
"""

# ============ OUTPUT MODES ============
"""
1. APPEND mode (default):
   - Only NEW rows added to result table are output.
   - Use for: Simple transformations without aggregation.
   - Cannot use with: aggregations (unless with watermark).

2. COMPLETE mode:
   - Entire result table is output after every trigger.
   - Use for: Aggregations where you need full result.
   - Requires: All data fits in memory (result table grows).

3. UPDATE mode:
   - Only CHANGED rows are output.
   - Use for: Aggregations where you only need updates.
   - More efficient than complete for large state.
"""

# ============ TRIGGERS ============
"""
Trigger Types:
1. Default (unspecified): Process next batch ASAP after previous completes.
2. Fixed interval: .trigger(processingTime="10 seconds")
   Process batch every 10 seconds.
3. Once: .trigger(once=True)
   Process ALL available data in one batch, then stop.
   Good for: Scheduled batch jobs that use streaming API.
4. Available-now: .trigger(availableNow=True)  (Spark 3.3+)
   Process all available data in multiple batches, then stop.
   Better than once for large backlogs.
"""

# ============ WATERMARKING (Late Data Handling) ============
"""
Watermark defines how long to wait for late data.

Example: Events with event_time, watermark of 10 minutes
- Current max event_time = 12:30
- Watermark = 12:30 - 10min = 12:20
- Events with event_time < 12:20 are DROPPED (too late)
- Events with event_time >= 12:20 are processed

df_stream \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(window("event_time", "5 minutes")) \
    .count()

Without watermark: State grows forever (OOM eventually)
With watermark: Old state is cleaned up (bounded memory)
"""

# ============ STREAMING JOINS ============
"""
Stream-Stream Join:
- Both sides are streaming
- Requires watermark on both sides for state cleanup
- Supports: inner, left outer, right outer

Stream-Static Join:
- One side is streaming, other is a regular DataFrame
- No watermark needed
- Static side is re-read each micro-batch (or cached)

Example:
df_stream.join(df_static, "key", "inner")  # Stream-static
df_stream1.join(df_stream2, "key", "inner")  # Stream-stream (needs watermark)
"""

# ============ CHECKPOINTING ============
"""
Checkpoint stores:
1. Offsets: Which data has been processed (Kafka offsets, file list)
2. State: Aggregation state for stateful operations
3. Committed offsets: Which batches have been committed to sink

Required for:
- Fault tolerance (restart from where it left off)
- Exactly-once semantics
- Stateful operations (aggregations, dedup, joins)

query = df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("checkpointLocation", "/shared/checkpoint/my_query") \
    .option("path", "/shared/output/my_query") \
    .start()

IMPORTANT: Checkpoint location must be unique per query!
"""

# ============ PRACTICAL EXAMPLE (Batch simulation) ============

# Simulate what a streaming pipeline would look like in batch
print("=== Simulated Streaming Pipeline (batch mode) ===")

# Simulate event data
events = [
    ("2024-01-01 10:00:00", "click", "user_1", 1),
    ("2024-01-01 10:01:00", "click", "user_2", 1),
    ("2024-01-01 10:02:00", "purchase", "user_1", 50),
    ("2024-01-01 10:05:00", "click", "user_3", 1),
    ("2024-01-01 10:06:00", "purchase", "user_2", 30),
    ("2024-01-01 10:10:00", "click", "user_1", 1),
    ("2024-01-01 10:12:00", "purchase", "user_3", 75),
]

from pyspark.sql.types import TimestampType
from pyspark.sql.functions import to_timestamp

df_events = spark.createDataFrame(events, ["event_time", "event_type", "user_id", "value"])
df_events = df_events.withColumn("event_time", to_timestamp("event_time"))

# Windowed aggregation (same logic works in streaming with watermark)
print("=== 5-minute window aggregation ===")
df_windowed = df_events \
    .groupBy(
        window("event_time", "5 minutes"),
        "event_type"
    ).agg(
        count("*").alias("event_count"),
        sum("value").alias("total_value")
    )

df_windowed.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    "event_type", "event_count", "total_value"
).orderBy("window_start", "event_type").show(truncate=False)

# Write
df_windowed.write.mode("overwrite").parquet("/shared/streaming_demo")

spark.stop()
