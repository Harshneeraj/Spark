"""
Topic: Salting Technique Deep Dive - Skew Resolution
======================================================

Salting is the most important technique for handling data skew in joins
and aggregations. This is a MUST-KNOW for interviews.

Spark UI Behavior:
- Without salting: One task takes 10-100x longer (visible in task duration metrics).
  Spark UI -> Stages -> Summary Metrics: Max >> Median duration.
- With salting: Tasks have uniform duration.
  Spark UI -> Stages -> Summary Metrics: Max ≈ Median duration.
- Salting adds 1 extra shuffle (for the salt column), but overall faster.

Key Interview Points:
- Salting splits a hot key into N sub-keys to distribute load.
- For joins: Salt the large table, explode the small table.
- For aggregations: Two-phase approach (partial agg with salt, final agg without).
- Choose salt factor based on skew ratio (if 1 key has 100x data, salt by 100).
- Trade-off: More shuffled data (small table exploded) vs. better parallelism.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, concat, floor, rand, count, sum, avg,
    broadcast, explode, array, monotonically_increasing_id
)
import random

spark = SparkSession.builder \
    .appName("33_Salting_Deep_Dive") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

# ============ SETUP: Heavily Skewed Data ============

# Large fact table: 90% of records have country_code = "US" (hot key)
fact_data = []
for i in range(1, 201):
    if i <= 180:
        country = "US"  # 90% -> HOT KEY
    elif i <= 190:
        country = "UK"  # 5%
    elif i <= 195:
        country = "IN"  # 2.5%
    else:
        country = "DE"  # 2.5%
    fact_data.append((i, country, random.randint(100, 10000)))

# Small dimension table
dim_data = [
    ("US", "United States", "North America"),
    ("UK", "United Kingdom", "Europe"),
    ("IN", "India", "Asia"),
    ("DE", "Germany", "Europe")
]

df_fact = spark.createDataFrame(fact_data, ["id", "country_code", "amount"])
df_dim = spark.createDataFrame(dim_data, ["country_code", "country_name", "continent"])

print("=== Data Distribution (SKEWED) ===")
df_fact.groupBy("country_code").count().orderBy(col("count").desc()).show()

# ============ SALTED JOIN: Step by Step ============

SALT_FACTOR = 8  # Split hot key into 8 sub-partitions

print(f"\n=== Salting with factor {SALT_FACTOR} ===")

# STEP 1: Add random salt to the LARGE (fact) table
# Each row gets a random number 0 to SALT_FACTOR-1
df_fact_salted = df_fact.withColumn(
    "salt", (rand() * SALT_FACTOR).cast("int")
).withColumn(
    "salted_key", concat(col("country_code"), lit("_"), col("salt").cast("string"))
)

print("Step 1: Fact table with salt")
df_fact_salted.show(10)

# STEP 2: Explode the SMALL (dimension) table to match all salt values
# Each row in dim table becomes SALT_FACTOR rows
salt_df = spark.createDataFrame(
    [(i,) for i in range(SALT_FACTOR)], ["salt"]
)

df_dim_exploded = df_dim.crossJoin(salt_df).withColumn(
    "salted_key", concat(col("country_code"), lit("_"), col("salt").cast("string"))
)

print("Step 2: Dimension table exploded (each row x SALT_FACTOR)")
df_dim_exploded.show(20)
print(f"Original dim rows: {df_dim.count()}, Exploded: {df_dim_exploded.count()}")

# STEP 3: Join on salted key
df_salted_result = df_fact_salted.join(
    df_dim_exploded,
    "salted_key",
    "inner"
).select(
    df_fact_salted["id"],
    df_fact_salted["country_code"],
    df_fact_salted["amount"],
    df_dim_exploded["country_name"],
    df_dim_exploded["continent"]
)

print("Step 3: Salted join result")
df_salted_result.show(10)
print(f"Result count: {df_salted_result.count()}")

# Verify distribution is now even
print("\n=== Distribution of salted keys (should be even!) ===")
df_fact_salted.groupBy("salted_key").count().orderBy("salted_key").show(20)

# ============ SALTED AGGREGATION: Two-Phase Approach ============

print("\n" + "=" * 60)
print("=== SALTED AGGREGATION ===")
print("=" * 60)

# Problem: groupBy("country_code").sum("amount")
# The "US" partition has 180 rows while others have 5-20

# Phase 1: Partial aggregation WITH salt
# This distributes "US" across SALT_FACTOR partitions
df_phase1 = df_fact.withColumn(
    "salt", (rand() * SALT_FACTOR).cast("int")
).groupBy("country_code", "salt").agg(
    sum("amount").alias("partial_sum"),
    count("*").alias("partial_count"),
    avg("amount").alias("partial_avg")
)

print("\nPhase 1: Partial aggregation with salt")
df_phase1.orderBy("country_code", "salt").show(20)

# Phase 2: Final aggregation WITHOUT salt
# Combine the partial results
df_phase2 = df_phase1.groupBy("country_code").agg(
    sum("partial_sum").alias("total_amount"),
    sum("partial_count").alias("total_count")
).withColumn("avg_amount", col("total_amount") / col("total_count"))

print("Phase 2: Final aggregation (combine partials)")
df_phase2.show()

# ============ CHOOSING SALT FACTOR ============
"""
How to choose SALT_FACTOR:

1. Calculate skew ratio:
   skew_ratio = max_partition_size / median_partition_size
   
2. Set SALT_FACTOR ≈ skew_ratio (or next power of 2)

Example:
   US: 180 rows, UK: 10, IN: 5, DE: 5
   Median ≈ 7.5, Max = 180
   Skew ratio = 180 / 7.5 = 24
   SALT_FACTOR = 16 or 32 (power of 2 for even hashing)

Trade-offs:
- Higher salt = better distribution but more data in exploded dim table
- Lower salt = less overhead but may not fully resolve skew
- Sweet spot: salt until max_partition ≈ 2-3x median_partition

For our example:
  Without salt: US partition = 180 rows, others = 5-10
  With salt=8: US partitions = ~22 rows each, others = 5-10
  Much more balanced!
"""

# ============ SELECTIVE SALTING (Only salt hot keys) ============

print("\n=== Selective Salting (only hot keys) ===")

# Only salt the problematic key, leave others alone
HOT_KEYS = ["US"]  # Identified hot keys

# Fact table: salt only hot keys
df_fact_selective = df_fact.withColumn(
    "salt",
    when(col("country_code").isin(HOT_KEYS), (rand() * SALT_FACTOR).cast("int"))
    .otherwise(lit(0))
).withColumn(
    "salted_key",
    concat(col("country_code"), lit("_"), col("salt").cast("string"))
)

# Dim table: explode only hot keys
from pyspark.sql.functions import when

df_dim_hot = df_dim.filter(col("country_code").isin(HOT_KEYS)) \
    .crossJoin(salt_df) \
    .withColumn("salted_key", concat(col("country_code"), lit("_"), col("salt").cast("string")))

df_dim_cold = df_dim.filter(~col("country_code").isin(HOT_KEYS)) \
    .withColumn("salt", lit(0)) \
    .withColumn("salted_key", concat(col("country_code"), lit("_0")))

df_dim_selective = df_dim_hot.unionByName(df_dim_cold)

print("Selective dim table (only US exploded):")
df_dim_selective.select("country_code", "salted_key").show()

# Join
df_selective_result = df_fact_selective.join(
    df_dim_selective, "salted_key", "inner"
).select(
    df_fact_selective["id"],
    df_fact_selective["country_code"],
    df_fact_selective["amount"],
    df_dim_selective["country_name"]
)

print(f"Selective salting result count: {df_selective_result.count()}")

# Write
df_salted_result.write.mode("overwrite").parquet("/shared/salted_join_result")

spark.stop()
