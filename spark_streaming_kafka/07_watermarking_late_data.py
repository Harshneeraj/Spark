"""
Topic: Watermarking and Late Data Handling
============================================

Watermarks define how long to wait for late-arriving data.

Spark UI Behavior:
- Watermark doesn't add extra stages or jobs.
- In Streaming tab -> "State" section: shows watermark value.
- State size decreases as watermark advances (old state cleaned up).
- Without watermark: State grows FOREVER (eventual OOM).
- With watermark: State is bounded (old windows/keys are dropped).

Key Interview Points:
- Watermark = max_event_time_seen - threshold
- Events with event_time < watermark are DROPPED (too late).
- Watermark enables: append mode with aggregations, state cleanup.
- Without watermark: State grows unbounded -> OOM eventually.
- Watermark is a TRADE-OFF: longer = more late data accepted, more memory.
- Watermark advances monotonically (never goes backward).
- Watermark is per-partition (global watermark = min across partitions).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, count, sum, avg, max as spark_max,
    to_timestamp, current_timestamp, lit
)

spark = SparkSession.builder \
    .appName("07_Watermarking_Late_Data") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# ============ WHY WATERMARKS ARE NEEDED ============
"""
PROBLEM: In streaming, events can arrive OUT OF ORDER (late data).

Example timeline:
  Real time:  10:00  10:01  10:02  10:03  10:04  10:05
  Events:     E1     E2     E3     E4     E5     E6
                            ↑
                     E_late arrives at 10:02 but has event_time 09:55!

Without watermark:
  - Spark keeps ALL window state forever (waiting for possible late data)
  - State grows unbounded -> OOM
  - Can never finalize a window (what if more late data comes?)

With watermark (e.g., 10 minutes):
  - Spark says: "I'll wait up to 10 minutes for late data"
  - After watermark passes a window's end: window is FINALIZED
  - Finalized windows are output (append mode) and state is cleaned up
  - Events arriving after watermark are DROPPED

WATERMARK FORMULA:
  watermark = max(event_time seen so far) - threshold
  
  If max event_time = 10:30 and threshold = 10 minutes:
  watermark = 10:20
  Any event with event_time < 10:20 is DROPPED
"""

# ============ WATERMARK EXAMPLE ============

# Simulate events with some arriving late
events = [
    # Batch 1 (arrives at processing time ~10:00)
    ("2024-01-01 10:00:00", "user_1", "click", 1.0),
    ("2024-01-01 10:01:00", "user_2", "click", 1.0),
    ("2024-01-01 10:02:00", "user_1", "purchase", 50.0),
    
    # Batch 2 (arrives at processing time ~10:05)
    ("2024-01-01 10:03:00", "user_3", "click", 1.0),
    ("2024-01-01 10:04:00", "user_2", "purchase", 30.0),
    ("2024-01-01 09:58:00", "user_4", "click", 1.0),  # LATE! (2 min late)
    
    # Batch 3 (arrives at processing time ~10:10)
    ("2024-01-01 10:08:00", "user_1", "click", 1.0),
    ("2024-01-01 10:09:00", "user_5", "purchase", 75.0),
    ("2024-01-01 09:50:00", "user_6", "click", 1.0),  # VERY LATE! (19 min late)
    
    # Batch 4
    ("2024-01-01 10:12:00", "user_2", "click", 1.0),
    ("2024-01-01 10:14:00", "user_3", "purchase", 100.0),
    ("2024-01-01 10:01:00", "user_7", "click", 1.0),  # LATE! (13 min late)
]

df = spark.createDataFrame(events, ["event_time", "user_id", "event_type", "value"])
df = df.withColumn("event_time", to_timestamp("event_time"))

print("=== All Events (including late ones) ===")
df.orderBy("event_time").show(truncate=False)

# ============ WINDOWED AGGREGATION WITH WATERMARK ============
"""
# In streaming:
df_stream \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window("event_time", "5 minutes"),  # 5-minute tumbling windows
        "event_type"
    ) \
    .agg(count("*").alias("event_count"), sum("value").alias("total_value"))

Watermark = 10 minutes means:
- Accept events up to 10 minutes late
- After max_event_time - 10min passes a window's end, finalize that window
- Drop events older than watermark
"""

# Batch simulation of watermarked aggregation
print("=== Windowed Aggregation (simulating watermark behavior) ===")

df_windowed = df \
    .withWatermark("event_time", "10 minutes") \
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
    "event_type",
    "event_count",
    "total_value"
).orderBy("window_start", "event_type").show(truncate=False)

# ============ WATERMARK PROGRESSION ============
"""
How watermark advances (10-minute watermark):

Batch 1: max_event_time = 10:02
  watermark = 10:02 - 10min = 09:52
  All events accepted (none older than 09:52)

Batch 2: max_event_time = 10:04
  watermark = 10:04 - 10min = 09:54
  Event at 09:58 -> ACCEPTED (09:58 > 09:54)
  
Batch 3: max_event_time = 10:09
  watermark = 10:09 - 10min = 09:59
  Event at 09:50 -> DROPPED! (09:50 < 09:59)
  
Batch 4: max_event_time = 10:14
  watermark = 10:14 - 10min = 10:04
  Event at 10:01 -> DROPPED! (10:01 < 10:04)
  Windows ending before 10:04 are FINALIZED and output (append mode)

VISUALIZATION:
Time:     09:50  09:55  10:00  10:05  10:10  10:15
           │      │      │      │      │      │
Watermark: ──────────────────────►──────►──────►
           09:52  09:54  09:59  10:04  10:09
                                 │
                    Events before this are DROPPED
"""

# ============ WINDOW TYPES ============
"""
1. TUMBLING WINDOW (non-overlapping, fixed size):
   window("event_time", "5 minutes")
   
   |--W1--|--W2--|--W3--|--W4--|
   00:00  05:00  10:00  15:00  20:00

2. SLIDING WINDOW (overlapping):
   window("event_time", "10 minutes", "5 minutes")
   (10-min window, slides every 5 min)
   
   |----W1----|
        |----W2----|
             |----W3----|
   00:00  05:00  10:00  15:00

3. SESSION WINDOW (gap-based, Spark 3.2+):
   session_window("event_time", "10 minutes")
   (new session if gap > 10 min)
"""

# Tumbling window
print("=== Tumbling Window (5 minutes) ===")
df.groupBy(window("event_time", "5 minutes")) \
    .agg(count("*").alias("events")) \
    .select("window.start", "window.end", "events") \
    .orderBy("start").show()

# Sliding window
print("=== Sliding Window (10 min window, 5 min slide) ===")
df.groupBy(window("event_time", "10 minutes", "5 minutes")) \
    .agg(count("*").alias("events")) \
    .select("window.start", "window.end", "events") \
    .orderBy("start").show()

# ============ CHOOSING WATERMARK DURATION ============
"""
TRADE-OFF:
┌─────────────────────┬──────────────────────┬──────────────────────┐
│ Watermark Duration  │ Pros                 │ Cons                 │
├─────────────────────┼──────────────────────┼──────────────────────┤
│ Short (1-5 min)     │ Less memory/state    │ More late data dropped│
│                     │ Faster finalization   │ Less accurate results│
├─────────────────────┼──────────────────────┼──────────────────────┤
│ Long (30-60 min)    │ Accepts more late data│ More memory/state   │
│                     │ More accurate results │ Slower finalization  │
└─────────────────────┴──────────────────────┴──────────────────────┘

Guidelines:
- Know your data's typical lateness (monitor in production)
- Set watermark slightly above max expected lateness
- Example: If 99% of events arrive within 5 minutes -> watermark = 10 minutes
- For financial data: Longer watermark (accuracy matters)
- For clickstream: Shorter watermark (freshness matters)
"""

# ============ WATERMARK WITH APPEND MODE ============
"""
Without watermark + aggregation:
  - Cannot use append mode (Spark doesn't know when window is final)
  - Must use complete or update mode

With watermark + aggregation:
  - CAN use append mode!
  - Windows are output ONCE when finalized (watermark passes window end)
  - Output is delayed by watermark duration
  - But guarantees: once output, result won't change

# This works:
df.withWatermark("event_time", "10 minutes") \
    .groupBy(window("event_time", "5 minutes")) \
    .count() \
    .writeStream \
    .outputMode("append")  # ✓ Works with watermark!
    .start()

# This FAILS:
df.groupBy(window("event_time", "5 minutes")) \
    .count() \
    .writeStream \
    .outputMode("append")  # ✗ ERROR! No watermark, can't finalize windows
    .start()
"""

# ============ STATE CLEANUP ============
"""
Watermark enables automatic state cleanup:

Without watermark:
  - State for ALL keys/windows is kept FOREVER
  - Memory grows unbounded
  - Eventually OOM

With watermark:
  - State for expired windows/keys is automatically removed
  - Memory is bounded
  - Old state cleaned up after watermark passes

Monitor state size in:
  query.lastProgress["stateOperators"]
  -> numRowsTotal: total keys in state
  -> numRowsUpdated: keys updated this batch
  -> memoryUsedBytes: memory used by state
"""

# Write demo
df_windowed.write.mode("overwrite").parquet("/shared/watermark_demo")

spark.stop()
