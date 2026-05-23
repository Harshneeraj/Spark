"""
Topic: Data Skew Handling - CRITICAL INTERVIEW TOPIC
=====================================================

Data skew occurs when some partitions have significantly more data than others,
causing some tasks to take much longer (straggler tasks).

Spark UI Behavior:
- With skew: In Spark UI -> Stages -> look at task duration distribution
  You'll see most tasks finish in seconds, but 1-2 tasks take minutes.
  The "Summary Metrics" will show huge difference between median and max duration.
- After fixing skew: Task durations become more uniform.
- AQE skew join: Spark UI shows "CustomShuffleReader" with skew handling.

Key Interview Points:
- Skew is the #1 performance killer in Spark.
- Symptoms: One task takes 10-100x longer than others.
- Common causes: null keys, hot keys (popular values), uneven data distribution.
- Solutions: Salting, broadcast join, AQE, filtering hot keys, repartitioning.
- AQE (Adaptive Query Execution) in Spark 3.x can auto-handle some skew.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, concat, floor, rand, broadcast,
    count, sum, avg, explode, array, monotonically_increasing_id
)
import random

spark = SparkSession.builder \
    .appName("14_Data_Skew_Handling") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .getOrCreate()

# ============ CREATE SKEWED DATA ============
# Simulate real-world skew: 80% of orders from store_id=1 (hot key)

orders_data = []
for i in range(1, 101):
    if i <= 80:
        store_id = 1  # HOT KEY - 80% of data
    elif i <= 90:
        store_id = 2
    elif i <= 95:
        store_id = 3
    else:
        store_id = 4
    orders_data.append((i, store_id, random.randint(10, 1000)))

stores_data = [
    (1, "MegaStore", "New York"),
    (2, "MidStore", "Chicago"),
    (3, "SmallStore", "Denver"),
    (4, "TinyStore", "Austin")
]

df_orders = spark.createDataFrame(orders_data, ["order_id", "store_id", "amount"])
df_stores = spark.createDataFrame(stores_data, ["store_id", "store_name", "city"])

print("=== Data Distribution (SKEWED) ===")
df_orders.groupBy("store_id").count().orderBy("store_id").show()
# store_id=1 has 80 rows, others have 10, 5, 5

# ============ PROBLEM: Skewed Join ============
# When joining, partition for store_id=1 will have 80 rows
# while others have 5-10 rows. One task does 80% of work!

print("=== Skewed Join (problematic) ===")
df_skewed_join = df_orders.join(df_stores, "store_id", "inner")
df_skewed_join.show(5)

# ============ SOLUTION 1: SALTING TECHNIQUE ============
"""
Salting: Add a random suffix to the skewed key to distribute it across partitions.
Steps:
1. Add salt (random number 0 to N-1) to the large table's key
2. Explode the small table to have all salt values
3. Join on (key + salt)
"""

NUM_SALTS = 4  # Split hot key into 4 partitions

# Step 1: Add salt to orders (large/skewed table)
df_orders_salted = df_orders.withColumn(
    "salt", (rand() * NUM_SALTS).cast("int")
).withColumn(
    "salted_store_id", concat(col("store_id").cast("string"), lit("_"), col("salt").cast("string"))
)

# Step 2: Explode stores (small table) to match all salt values
from pyspark.sql.functions import array, explode

salt_values = spark.createDataFrame(
    [(i,) for i in range(NUM_SALTS)], ["salt"]
)

df_stores_exploded = df_stores.crossJoin(salt_values).withColumn(
    "salted_store_id", concat(col("store_id").cast("string"), lit("_"), col("salt").cast("string"))
)

# Step 3: Join on salted key
print("=== Salted Join (skew fixed!) ===")
df_salted_join = df_orders_salted.join(
    df_stores_exploded,
    "salted_store_id",
    "inner"
).select(
    df_orders_salted["order_id"],
    df_orders_salted["store_id"],
    df_orders_salted["amount"],
    df_stores_exploded["store_name"],
    df_stores_exploded["city"]
)
df_salted_join.show(5)

# Verify distribution is now more even
print("=== Distribution after salting ===")
df_orders_salted.groupBy("salted_store_id").count().orderBy("salted_store_id").show()

# ============ SOLUTION 2: BROADCAST JOIN (Best for small dimension tables) ============

print("=== Broadcast Join (eliminates shuffle entirely) ===")
df_broadcast_join = df_orders.join(broadcast(df_stores), "store_id", "inner")
df_broadcast_join.show(5)
# No skew issue because no shuffle happens!

# ============ SOLUTION 3: ISOLATE HOT KEY ============
"""
Strategy: Process hot key separately from normal keys.
1. Filter out the hot key
2. Process hot key with broadcast join
3. Process remaining keys with regular join
4. Union the results
"""

# Identify hot key (store_id = 1)
HOT_KEY = 1

# Split orders
df_hot = df_orders.filter(col("store_id") == HOT_KEY)
df_normal = df_orders.filter(col("store_id") != HOT_KEY)

# Process hot key with broadcast
df_hot_joined = df_hot.join(broadcast(df_stores.filter(col("store_id") == HOT_KEY)), "store_id")

# Process normal keys with regular join
df_normal_joined = df_normal.join(df_stores.filter(col("store_id") != HOT_KEY), "store_id")

# Union results
df_final = df_hot_joined.unionByName(df_normal_joined)
print("=== Isolated Hot Key Join ===")
df_final.show(5)

# ============ SOLUTION 4: ADAPTIVE QUERY EXECUTION (AQE) ============
"""
Spark 3.x AQE automatically handles skew:
- spark.sql.adaptive.enabled = true
- spark.sql.adaptive.skewJoin.enabled = true
- spark.sql.adaptive.skewJoin.skewedPartitionFactor = 5 (default)
- spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 256MB

AQE detects skewed partitions at runtime and splits them into smaller partitions.
No code changes needed! But manual techniques give more control.
"""

print("=== AQE Configuration ===")
print(f"AQE enabled: {spark.conf.get('spark.sql.adaptive.enabled')}")
print(f"Skew join enabled: {spark.conf.get('spark.sql.adaptive.skewJoin.enabled')}")

# ============ SOLUTION 5: HANDLE NULL KEYS ============
# Null keys all go to the same partition (hash of null = same value)

orders_with_nulls = orders_data + [(101, None, 500), (102, None, 300), (103, None, 700)]
df_with_nulls = spark.createDataFrame(orders_with_nulls, ["order_id", "store_id", "amount"])

# Replace nulls with random values to distribute them
print("=== Handling Null Key Skew ===")
df_null_fixed = df_with_nulls.withColumn(
    "store_id_fixed",
    col("store_id")  # Keep original for non-null
).fillna({"store_id": -1})  # Or use random negative values

df_with_nulls.groupBy("store_id").count().show()

# ============ SKEW IN GROUPBY ============
# Salting for groupBy aggregation

print("=== Skewed GroupBy ===")
# Problem: groupBy("store_id").sum("amount") - store_id=1 partition is huge

# Solution: Two-phase aggregation with salt
# Phase 1: Partial aggregation with salt
df_partial = df_orders.withColumn("salt", (rand() * NUM_SALTS).cast("int")) \
    .groupBy("store_id", "salt") \
    .agg(sum("amount").alias("partial_sum"), count("*").alias("partial_count"))

# Phase 2: Final aggregation without salt
df_final_agg = df_partial.groupBy("store_id") \
    .agg(sum("partial_sum").alias("total_amount"), sum("partial_count").alias("total_count"))

print("=== Two-Phase Aggregation Result ===")
df_final_agg.show()

# Write
df_salted_join.write.mode("overwrite").parquet("/shared/skew_handled_orders")

spark.stop()
