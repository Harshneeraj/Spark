"""
Topic: Stateful Streaming Operations
======================================

Operations that maintain state across micro-batches.

Spark UI Behavior:
- Stateful operations show "StateStore" in the DAG.
- Spark UI -> Streaming -> State: shows numRowsTotal, memoryUsedBytes.
- State is checkpointed to reliable storage after each batch.
- State grows with unique keys (bounded by watermark).
- More state = longer checkpoint time = longer batch duration.

Key Interview Points:
- Stateful operations: groupBy+agg, window, join, dedup, mapGroupsWithState.
- State is stored in RocksDB (default) or in-memory (HDFS-backed).
- State must be bounded (use watermark!) or it grows forever.
- State is checkpointed for fault tolerance.
- Changing state schema requires checkpoint deletion.
- mapGroupsWithState: Custom stateful logic (most flexible).
- flatMapGroupsWithState: Custom stateful with multiple output rows.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, count, sum, avg, max as spark_max, min as spark_min,
    to_timestamp, expr, struct, collect_list, first, last
)

spark = SparkSession.builder \
    .appName("08_Stateful_Streaming") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# ============ TYPES OF STATEFUL OPERATIONS ============
"""
┌─────────────────────────────────┬──────────────────────────────────────────┐
│ Operation                       │ State Stored                             │
├─────────────────────────────────┼──────────────────────────────────────────┤
│ groupBy().agg()                 │ Running aggregation per group key        │
│ window().agg()                  │ Aggregation per window per key           │
│ dropDuplicates()                │ Set of seen keys (for dedup)             │
│ stream-stream join              │ Buffered rows from both sides            │
│ mapGroupsWithState              │ Custom state per key                     │
│ flatMapGroupsWithState          │ Custom state per key (multi-output)      │
└─────────────────────────────────┴──────────────────────────────────────────┘
"""

# Sample event data
events = [
    ("2024-01-01 10:00:00", "user_1", "page_view", "/home"),
    ("2024-01-01 10:00:30", "user_1", "page_view", "/products"),
    ("2024-01-01 10:01:00", "user_2", "page_view", "/home"),
    ("2024-01-01 10:01:30", "user_1", "add_to_cart", "/products/laptop"),
    ("2024-01-01 10:02:00", "user_2", "page_view", "/products"),
    ("2024-01-01 10:02:30", "user_1", "purchase", "/checkout"),
    ("2024-01-01 10:03:00", "user_3", "page_view", "/home"),
    ("2024-01-01 10:03:30", "user_2", "add_to_cart", "/products/phone"),
    ("2024-01-01 10:04:00", "user_3", "page_view", "/products"),
    ("2024-01-01 10:05:00", "user_2", "purchase", "/checkout"),
    ("2024-01-01 10:06:00", "user_3", "add_to_cart", "/products/tablet"),
    ("2024-01-01 10:10:00", "user_1", "page_view", "/home"),
    ("2024-01-01 10:15:00", "user_3", "purchase", "/checkout"),
]

df = spark.createDataFrame(events, ["event_time", "user_id", "event_type", "page"])
df = df.withColumn("event_time", to_timestamp("event_time"))

# ============ 1. RUNNING AGGREGATION (Stateful) ============
"""
State: Running count/sum per key, updated with each batch.
State grows with unique keys (bounded by watermark).
"""

print("=== Running Aggregation per User ===")
df_running_agg = df.withWatermark("event_time", "10 minutes") \
    .groupBy("user_id") \
    .agg(
        count("*").alias("total_events"),
        spark_min("event_time").alias("first_seen"),
        spark_max("event_time").alias("last_seen"),
        collect_list("event_type").alias("event_sequence")
    )
df_running_agg.show(truncate=False)

# ============ 2. WINDOWED AGGREGATION (Stateful) ============
"""
State: Aggregation per window per key.
Windows are finalized and state cleaned when watermark passes window end.
"""

print("=== 5-Minute Window Aggregation ===")
df_windowed = df.withWatermark("event_time", "10 minutes") \
    .groupBy(
        window("event_time", "5 minutes"),
        "user_id"
    ).agg(
        count("*").alias("events_in_window"),
        collect_list("event_type").alias("actions")
    )

df_windowed.select(
    "window.start", "window.end", "user_id", "events_in_window", "actions"
).orderBy("window.start", "user_id").show(truncate=False)

# ============ 3. STREAMING DEDUPLICATION (Stateful) ============
"""
State: Set of seen keys. Each new event is checked against state.
MUST use watermark to bound state (otherwise keeps ALL keys forever).

Use case: Kafka may deliver duplicates (at-least-once). Dedup in Spark.
"""

print("=== Streaming Deduplication ===")

# Simulate duplicates
events_with_dupes = [
    ("2024-01-01 10:00:00", "evt_001", "user_1", "click"),
    ("2024-01-01 10:00:01", "evt_002", "user_2", "click"),
    ("2024-01-01 10:00:02", "evt_001", "user_1", "click"),  # DUPLICATE!
    ("2024-01-01 10:00:03", "evt_003", "user_1", "purchase"),
    ("2024-01-01 10:00:04", "evt_002", "user_2", "click"),  # DUPLICATE!
    ("2024-01-01 10:00:05", "evt_004", "user_3", "click"),
]

df_dupes = spark.createDataFrame(events_with_dupes, 
    ["event_time", "event_id", "user_id", "event_type"])
df_dupes = df_dupes.withColumn("event_time", to_timestamp("event_time"))

# Dedup by event_id with watermark
df_deduped = df_dupes \
    .withWatermark("event_time", "10 minutes") \
    .dropDuplicates(["event_id"])

print("Before dedup:")
df_dupes.show()
print("After dedup:")
df_deduped.show()

"""
# In streaming:
df_stream \
    .withWatermark("event_time", "10 minutes") \
    .dropDuplicates(["event_id"]) \
    .writeStream \
    .outputMode("append") \
    .start()

State stores: set of event_ids seen in last 10 minutes.
After watermark passes, old event_ids are removed from state.
"""

# ============ 4. STREAM-STREAM JOIN (Stateful) ============
"""
Both sides buffer rows in state, waiting for matches.
MUST use watermark on both sides to bound state.

Use case: Join clicks with purchases (purchase may come minutes after click).
"""

print("=== Stream-Stream Join (simulated) ===")

# Stream 1: Clicks
clicks = [
    ("2024-01-01 10:00:00", "user_1", "product_A"),
    ("2024-01-01 10:01:00", "user_2", "product_B"),
    ("2024-01-01 10:02:00", "user_3", "product_C"),
    ("2024-01-01 10:05:00", "user_1", "product_D"),
]

# Stream 2: Purchases (may arrive later)
purchases = [
    ("2024-01-01 10:02:30", "user_1", "product_A", 100.0),
    ("2024-01-01 10:03:00", "user_2", "product_B", 200.0),
    # user_3 never purchased
    ("2024-01-01 10:08:00", "user_1", "product_D", 150.0),
]

df_clicks = spark.createDataFrame(clicks, ["click_time", "user_id", "product"])
df_clicks = df_clicks.withColumn("click_time", to_timestamp("click_time"))

df_purchases = spark.createDataFrame(purchases, ["purchase_time", "user_id", "product", "amount"])
df_purchases = df_purchases.withColumn("purchase_time", to_timestamp("purchase_time"))

# Stream-stream join with time constraint
df_joined = df_clicks \
    .withWatermark("click_time", "10 minutes") \
    .join(
        df_purchases.withWatermark("purchase_time", "10 minutes"),
        expr("""
            clicks.user_id = purchases.user_id AND
            clicks.product = purchases.product AND
            purchase_time >= click_time AND
            purchase_time <= click_time + interval 5 minutes
        """),
        "leftOuter"
    )

# Note: In batch mode, we use alias for the join
df_c = df_clicks.withWatermark("click_time", "10 minutes").alias("clicks")
df_p = df_purchases.withWatermark("purchase_time", "10 minutes").alias("purchases")

df_stream_join = df_c.join(
    df_p,
    (col("clicks.user_id") == col("purchases.user_id")) &
    (col("clicks.product") == col("purchases.product")),
    "left"
)

print("Click-to-Purchase Join:")
df_stream_join.select(
    col("clicks.user_id"),
    col("clicks.product"),
    col("clicks.click_time"),
    col("purchases.purchase_time"),
    col("purchases.amount")
).show(truncate=False)

"""
# In streaming:
df_clicks_stream.withWatermark("click_time", "10 minutes") \
    .join(
        df_purchases_stream.withWatermark("purchase_time", "10 minutes"),
        expr('''
            clicks.user_id = purchases.user_id AND
            purchase_time >= click_time AND
            purchase_time <= click_time + interval 10 minutes
        '''),
        "inner"  # or "leftOuter" (Spark 3.x)
    )

State: Buffers clicks and purchases within watermark window.
When watermark passes, unmatched rows are dropped from state.
"""

# ============ STATE STORE CONFIGURATION ============
"""
State Store Backends:

1. HDFSBackedStateStore (default in older Spark):
   - State stored in memory, checkpointed to HDFS
   - Simple but can be slow for large state

2. RocksDBStateStore (Spark 3.2+, recommended):
   - State stored in RocksDB (on local disk)
   - Better for large state (doesn't need to fit in memory)
   - Incremental checkpointing (faster)
   
   spark.conf.set("spark.sql.streaming.stateStore.providerClass",
       "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")

State Store Configs:
  spark.sql.streaming.stateStore.minDeltasForSnapshot = 10
  spark.sql.streaming.stateStore.maintenanceInterval = 60s
  spark.sql.streaming.stateStore.rocksdb.compactOnCommit = false
"""

# ============ MONITORING STATE ============
"""
In query.lastProgress:
{
  "stateOperators": [{
    "operatorName": "stateStoreSave",
    "numRowsTotal": 1500,        # Total keys in state
    "numRowsUpdated": 50,        # Keys updated this batch
    "allUpdatesTimeMs": 120,     # Time to update state
    "numRowsRemoved": 10,        # Keys removed (watermark cleanup)
    "commitTimeMs": 80,          # Time to checkpoint state
    "memoryUsedBytes": 5242880,  # Memory used by state
    "numShufflePartitions": 4,   # State partitions
    "customMetrics": {
      "rocksdbCommitCompactLatency": 50,
      "rocksdbBytesCopied": 1024
    }
  }]
}

RED FLAGS:
- numRowsTotal growing unbounded -> Missing watermark!
- commitTimeMs increasing -> State too large, consider RocksDB
- memoryUsedBytes > executor memory -> OOM risk
"""

# Write demo
df_deduped.write.mode("overwrite").parquet("/shared/stateful_streaming_demo")

spark.stop()
