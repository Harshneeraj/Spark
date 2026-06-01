"""
Topic: Streaming Joins - Stream-Stream and Stream-Static
=========================================================

Joining streaming DataFrames with other streams or static tables.

Spark UI Behavior:
- Stream-Static join: Same as batch join per micro-batch (no extra state).
- Stream-Stream join: State maintained for both sides (visible in state metrics).
  Each batch shows state size growing/shrinking based on watermark.
- Inner stream-stream join: 2+ stages per batch (shuffle for join).
- Broadcast works for stream-static joins (small static table).

Key Interview Points:
- Stream-Static: Static side re-read each batch (or cached). No watermark needed.
- Stream-Stream: Both sides buffered in state. Watermark REQUIRED for state cleanup.
- Inner join: Output when match found on either side.
- Outer join: Output when match found OR watermark guarantees no future match.
- Time constraint in join condition bounds the state.
- Without time constraint: State grows unbounded!
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, expr, broadcast,
    window, count, sum
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder \
    .appName("11_Streaming_Joins") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# ============ 1. STREAM-STATIC JOIN ============
"""
One side is streaming, other is a regular (static) DataFrame.
Static side is like a lookup/dimension table.

Characteristics:
- No watermark needed (static side doesn't change)
- Static side is re-read each micro-batch (or cached)
- Broadcast join works if static side is small
- All join types supported (inner, left, right, outer)
- No state maintained for the join itself

Use cases:
- Enrich events with dimension data (user info, product catalog)
- Lookup reference data (country codes, config)
"""

# Static dimension table (loaded once)
dim_products = spark.createDataFrame([
    ("P001", "Laptop", "Electronics", 999.99),
    ("P002", "Phone", "Electronics", 699.99),
    ("P003", "Shirt", "Clothing", 49.99),
    ("P004", "Book", "Education", 29.99),
    ("P005", "Headphones", "Electronics", 199.99),
], ["product_id", "product_name", "category", "price"])

# Streaming events (simulated)
order_events = [
    ("2024-01-01 10:00:00", "user_1", "P001", 1),
    ("2024-01-01 10:00:05", "user_2", "P002", 2),
    ("2024-01-01 10:00:10", "user_1", "P003", 1),
    ("2024-01-01 10:00:15", "user_3", "P004", 3),
    ("2024-01-01 10:00:20", "user_2", "P005", 1),
    ("2024-01-01 10:00:25", "user_4", "P999", 1),  # Unknown product
]

df_orders = spark.createDataFrame(order_events, 
    ["event_time", "user_id", "product_id", "quantity"])
df_orders = df_orders.withColumn("event_time", to_timestamp("event_time"))

# Stream-Static Join (enrich orders with product info)
print("=== Stream-Static Join (Enrich with Product Info) ===")
df_enriched = df_orders.join(
    broadcast(dim_products),  # Broadcast small static table
    "product_id",
    "left"  # Keep orders even if product not found
).withColumn("total_price", col("price") * col("quantity"))

df_enriched.show(truncate=False)

"""
# In streaming:
df_stream = spark.readStream.format("kafka")...
df_enriched = df_stream.join(broadcast(dim_products), "product_id", "left")
df_enriched.writeStream.format("kafka")...start()

# To refresh static table periodically:
# Option 1: Restart query (picks up new static data)
# Option 2: Use foreachBatch and reload static table inside function
"""

# ============ 2. STREAM-STREAM JOIN (Inner) ============
"""
Both sides are streaming. State maintained for both.
MUST have watermark + time constraint for bounded state.

Use cases:
- Match ad impressions with clicks
- Match orders with payments
- Match login events with activity events
- Correlate events from different systems
"""

# Stream 1: Ad Impressions
impressions = [
    ("2024-01-01 10:00:00", "imp_1", "user_1", "ad_A"),
    ("2024-01-01 10:00:10", "imp_2", "user_2", "ad_B"),
    ("2024-01-01 10:00:20", "imp_3", "user_3", "ad_A"),
    ("2024-01-01 10:00:30", "imp_4", "user_1", "ad_C"),
    ("2024-01-01 10:01:00", "imp_5", "user_4", "ad_B"),
]

# Stream 2: Ad Clicks (may arrive later)
clicks = [
    ("2024-01-01 10:00:05", "click_1", "user_1", "ad_A"),   # 5s after impression
    ("2024-01-01 10:00:25", "click_2", "user_2", "ad_B"),   # 15s after impression
    ("2024-01-01 10:01:30", "click_3", "user_3", "ad_A"),   # 70s after impression
    ("2024-01-01 10:00:35", "click_4", "user_1", "ad_C"),   # 5s after impression
    # user_4 never clicked (no match)
]

df_impressions = spark.createDataFrame(impressions, 
    ["imp_time", "impression_id", "user_id", "ad_id"])
df_impressions = df_impressions.withColumn("imp_time", to_timestamp("imp_time"))

df_clicks = spark.createDataFrame(clicks, 
    ["click_time", "click_id", "user_id", "ad_id"])
df_clicks = df_clicks.withColumn("click_time", to_timestamp("click_time"))

# Stream-Stream Inner Join with time constraint
print("\n=== Stream-Stream Inner Join (Impressions + Clicks) ===")

df_imp = df_impressions.withWatermark("imp_time", "10 minutes").alias("imp")
df_clk = df_clicks.withWatermark("click_time", "10 minutes").alias("clk")

# Join condition: same user + same ad + click within 1 minute of impression
df_matched = df_imp.join(
    df_clk,
    (col("imp.user_id") == col("clk.user_id")) &
    (col("imp.ad_id") == col("clk.ad_id")) &
    (col("clk.click_time") >= col("imp.imp_time")) &
    (col("clk.click_time") <= col("imp.imp_time") + expr("interval 1 minute")),
    "inner"
)

df_matched.select(
    col("imp.user_id"),
    col("imp.ad_id"),
    col("imp.imp_time"),
    col("clk.click_time"),
    (col("clk.click_time").cast("long") - col("imp.imp_time").cast("long")).alias("latency_seconds")
).show(truncate=False)

# ============ 3. STREAM-STREAM LEFT OUTER JOIN ============
"""
Left outer join: Output all impressions, with click info if available.
Unmatched impressions output when watermark guarantees no future click.

IMPORTANT: Outer join results are delayed until watermark passes!
(Spark waits to see if a match will arrive before outputting null)
"""

print("\n=== Stream-Stream Left Outer Join ===")
df_left_outer = df_imp.join(
    df_clk,
    (col("imp.user_id") == col("clk.user_id")) &
    (col("imp.ad_id") == col("clk.ad_id")) &
    (col("clk.click_time") >= col("imp.imp_time")) &
    (col("clk.click_time") <= col("imp.imp_time") + expr("interval 1 minute")),
    "left"  # Keep all impressions
)

df_left_outer.select(
    col("imp.user_id"),
    col("imp.ad_id"),
    col("imp.imp_time"),
    col("clk.click_time"),
    col("clk.click_id")
).show(truncate=False)
# imp_5 (user_4) has null click (never clicked)

# ============ JOIN STATE MANAGEMENT ============
"""
Stream-Stream Join State:

Left State (Impressions):
┌──────────────┬─────────────────────────────────────────┐
│ Key          │ Buffered Rows                           │
├──────────────┼─────────────────────────────────────────┤
│ (user_1,ad_A)│ [imp_1 @ 10:00:00]                     │
│ (user_2,ad_B)│ [imp_2 @ 10:00:10]                     │
│ (user_3,ad_A)│ [imp_3 @ 10:00:20]                     │
│ ...          │ ...                                     │
└──────────────┴─────────────────────────────────────────┘

Right State (Clicks):
┌──────────────┬─────────────────────────────────────────┐
│ Key          │ Buffered Rows                           │
├──────────────┼─────────────────────────────────────────┤
│ (user_1,ad_A)│ [click_1 @ 10:00:05]                   │
│ (user_2,ad_B)│ [click_2 @ 10:00:25]                   │
│ ...          │ ...                                     │
└──────────────┴─────────────────────────────────────────┘

State cleanup:
- When watermark passes imp_time + 1 minute: Remove impression from state
- When watermark passes click_time: Remove click from state
- Time constraint (1 minute) BOUNDS the state!

WITHOUT time constraint:
- State grows FOREVER (every impression waits for potential click)
- Eventually OOM!
"""

# ============ JOIN TYPE SUPPORT IN STREAMING ============
"""
┌─────────────────────────┬───────────────┬───────────────────────────────────┐
│ Join Type               │ Supported?    │ Requirements                      │
├─────────────────────────┼───────────────┼───────────────────────────────────┤
│ Stream-Static Inner     │ ✓ Always      │ None                              │
│ Stream-Static Left      │ ✓ Always      │ Stream on left side               │
│ Stream-Static Right     │ ✓ Spark 3.x   │ Stream on right side              │
│ Stream-Static Full      │ ✗ No          │ Not supported                     │
├─────────────────────────┼───────────────┼───────────────────────────────────┤
│ Stream-Stream Inner     │ ✓ Always      │ Watermark + time constraint       │
│ Stream-Stream Left      │ ✓ Spark 2.3+  │ Watermark on right + time const.  │
│ Stream-Stream Right     │ ✓ Spark 2.3+  │ Watermark on left + time const.   │
│ Stream-Stream Full      │ ✗ No          │ Not supported                     │
│ Stream-Stream Semi/Anti │ ✓ Spark 3.x   │ Watermark + time constraint       │
└─────────────────────────┴───────────────┴───────────────────────────────────┘
"""

# Write demo
df_enriched.write.mode("overwrite").parquet("/shared/streaming_joins_demo")

spark.stop()
