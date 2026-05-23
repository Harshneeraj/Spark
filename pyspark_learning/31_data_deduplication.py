"""
Topic: Data Deduplication Strategies
======================================

Removing duplicate records - very common in ETL pipelines.

Spark UI Behavior:
- distinct(): WIDE transformation -> shuffle -> 2 stages
  Stage 0: Read + hash partitioning
  Stage 1: Shuffle + deduplicate within each partition
  
- dropDuplicates(): Same as distinct() when no columns specified.
  With columns specified: Still shuffle, but only considers those columns.
  
- Window function dedup: shuffle for window partition -> 2 stages

Key Interview Points:
- distinct() removes exact duplicate rows (all columns must match).
- dropDuplicates(cols) removes duplicates based on subset of columns.
- For "keep latest" dedup, use window functions with row_number().
- dropDuplicates keeps the FIRST occurrence (non-deterministic without ordering!).
- For large datasets, approximate dedup with approx_count_distinct().
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, desc, count, max as spark_max
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("31_Data_Deduplication") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Data with duplicates (simulating CDC/event stream)
data = [
    (1, "Alice", "alice@email.com", "2024-01-01 10:00:00"),
    (1, "Alice", "alice@email.com", "2024-01-01 10:00:00"),  # Exact duplicate
    (2, "Bob", "bob@email.com", "2024-01-02 11:00:00"),
    (2, "Bob", "bob_new@email.com", "2024-01-03 12:00:00"),  # Updated email
    (3, "Charlie", "charlie@email.com", "2024-01-01 09:00:00"),
    (3, "Charlie", "charlie@email.com", "2024-01-04 14:00:00"),  # Same data, new timestamp
    (4, "Diana", "diana@email.com", "2024-01-02 08:00:00"),
    (4, "Diana", "diana_v2@email.com", "2024-01-05 16:00:00"),  # Updated
]

df = spark.createDataFrame(data, ["id", "name", "email", "updated_at"])

print("=== Original Data (with duplicates) ===")
df.show(truncate=False)
print(f"Total rows: {df.count()}")

# ============ METHOD 1: distinct() - Exact duplicates only ============

print("\n=== distinct() - removes exact duplicate rows ===")
df_distinct = df.distinct()
df_distinct.show(truncate=False)
print(f"After distinct: {df_distinct.count()} rows")
# Only removes row where ALL columns match (Alice's exact duplicate)

# ============ METHOD 2: dropDuplicates() on subset ============

print("\n=== dropDuplicates(['id']) - keep first occurrence ===")
df_dedup_id = df.dropDuplicates(["id"])
df_dedup_id.show(truncate=False)
# Keeps first occurrence for each id (non-deterministic which one!)

print("\n=== dropDuplicates(['id', 'name']) ===")
df_dedup_multi = df.dropDuplicates(["id", "name"])
df_dedup_multi.show(truncate=False)

# ============ METHOD 3: Window Function - Keep Latest (BEST) ============
# Most common interview approach: "Keep the latest record per key"

print("\n=== Window dedup - keep LATEST record per id ===")
window_spec = Window.partitionBy("id").orderBy(desc("updated_at"))

df_latest = df.withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .drop("rn")

df_latest.show(truncate=False)
# Keeps the most recent record for each id (deterministic!)

# ============ METHOD 4: GroupBy + Max (for simple cases) ============

print("\n=== GroupBy + max(updated_at) ===")
# Get the latest timestamp per id, then join back
df_max_ts = df.groupBy("id").agg(spark_max("updated_at").alias("max_updated_at"))

df_latest_v2 = df.join(
    df_max_ts,
    (df["id"] == df_max_ts["id"]) & (df["updated_at"] == df_max_ts["max_updated_at"]),
    "inner"
).select(df["id"], df["name"], df["email"], df["updated_at"])

df_latest_v2.show(truncate=False)

# ============ METHOD 5: Dedup with Priority ============
# Keep record based on business priority

data_priority = [
    (1, "Alice", "source_A", 90000),
    (1, "Alice", "source_B", 85000),  # Same person, different source
    (2, "Bob", "source_A", 45000),
    (2, "Bob", "source_C", 47000),
]

df_priority = spark.createDataFrame(data_priority, ["id", "name", "source", "salary"])

# Define priority: source_A > source_B > source_C
print("\n=== Dedup with source priority ===")
from pyspark.sql.functions import when

df_with_priority = df_priority.withColumn(
    "priority",
    when(col("source") == "source_A", 1)
    .when(col("source") == "source_B", 2)
    .otherwise(3)
)

window_priority = Window.partitionBy("id").orderBy("priority")
df_best_source = df_with_priority \
    .withColumn("rn", row_number().over(window_priority)) \
    .filter(col("rn") == 1) \
    .drop("rn", "priority")

df_best_source.show()

# ============ PERFORMANCE COMPARISON ============
"""
Method              | Shuffle | Deterministic | Use Case
--------------------|---------|---------------|---------------------------
distinct()          | Yes     | Yes           | Exact duplicates only
dropDuplicates(cols)| Yes     | No*           | Simple dedup on key
Window + row_number | Yes     | Yes           | Keep latest/first by order
GroupBy + join      | Yes x2  | Yes           | Simple cases (less efficient)

* dropDuplicates is non-deterministic about WHICH duplicate to keep
  unless data is already ordered (which it usually isn't in distributed systems).

RECOMMENDATION: Use Window + row_number for production dedup.
It's deterministic, flexible, and handles complex priority logic.
"""

# Write
df_latest.write.mode("overwrite").parquet("/shared/deduplicated_data")

spark.stop()
