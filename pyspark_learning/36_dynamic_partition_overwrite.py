"""
Topic: Dynamic Partition Overwrite Mode
=========================================

Controls how partitioned data is overwritten.

Spark UI Behavior:
- Same as regular write: 1 job with stages depending on prior transforms.
- No difference in Spark UI between static and dynamic mode.
- The difference is in WHAT gets deleted on disk.

Key Interview Points:
- Static mode (default): Deletes ALL partitions, rewrites everything.
- Dynamic mode: Only overwrites partitions that have new data.
- Critical for incremental/daily loads where you only update today's partition.
- Without dynamic mode, you'd lose historical partitions on overwrite!
- Set: spark.sql.sources.partitionOverwriteMode = dynamic
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder \
    .appName("36_Dynamic_Partition_Overwrite") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# ============ SETUP: Initial Data ============

# Day 1: Write data for Jan 1, Jan 2, Jan 3
initial_data = [
    (1, "Alice", "2024-01-01", 100),
    (2, "Bob", "2024-01-01", 200),
    (3, "Charlie", "2024-01-02", 150),
    (4, "Diana", "2024-01-02", 250),
    (5, "Eve", "2024-01-03", 300),
]

df_initial = spark.createDataFrame(initial_data, ["id", "name", "date", "amount"])

# Write partitioned by date
df_initial.write.mode("overwrite") \
    .partitionBy("date") \
    .parquet("/shared/partitioned_sales")

print("=== Initial Data (3 date partitions) ===")
spark.read.parquet("/shared/partitioned_sales").show()

# ============ STATIC MODE (Default) - DANGEROUS! ============
"""
Static mode: When you write with mode("overwrite") + partitionBy,
it DELETES ALL existing partitions and writes only the new data.

If you only have data for Jan 3, it will DELETE Jan 1 and Jan 2!
"""

# New data: Only for Jan 3 (updated values)
new_data_jan3 = [
    (5, "Eve", "2024-01-03", 350),  # Updated amount
    (6, "Frank", "2024-01-03", 400),  # New record
]

df_new = spark.createDataFrame(new_data_jan3, ["id", "name", "date", "amount"])

# STATIC overwrite (default) - THIS DELETES JAN 1 AND JAN 2!
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

df_new.write.mode("overwrite") \
    .partitionBy("date") \
    .parquet("/shared/partitioned_sales_static")

# First write all data, then overwrite with static
df_initial.write.mode("overwrite").partitionBy("date").parquet("/shared/partitioned_sales_static")
df_new.write.mode("overwrite").partitionBy("date").parquet("/shared/partitioned_sales_static")

print("=== After STATIC overwrite (Jan 1 & Jan 2 are GONE!) ===")
spark.read.parquet("/shared/partitioned_sales_static").show()
# Only Jan 3 data remains! Jan 1 and Jan 2 are deleted!

# ============ DYNAMIC MODE - SAFE! ============
"""
Dynamic mode: Only overwrites partitions that appear in the new data.
Other partitions are left untouched.

If you only have data for Jan 3, it will:
- DELETE only the Jan 3 partition
- REWRITE Jan 3 with new data
- KEEP Jan 1 and Jan 2 untouched!
"""

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Rewrite initial data first
df_initial.write.mode("overwrite") \
    .partitionBy("date") \
    .parquet("/shared/partitioned_sales_dynamic")

# Now overwrite with only Jan 3 data
df_new.write.mode("overwrite") \
    .partitionBy("date") \
    .parquet("/shared/partitioned_sales_dynamic")

print("=== After DYNAMIC overwrite (Jan 1 & Jan 2 preserved!) ===")
spark.read.parquet("/shared/partitioned_sales_dynamic").show()
# Jan 1 and Jan 2 are preserved! Only Jan 3 is updated!

# ============ USE CASE: Daily Incremental Load ============
"""
Typical ETL pattern:
1. Read today's data from source
2. Transform
3. Write with dynamic partition overwrite

This ensures:
- Today's partition is refreshed with latest data
- Historical partitions remain untouched
- Idempotent: Running twice for same day just overwrites same partition

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

df_today = read_from_source(date=today)
df_transformed = transform(df_today)
df_transformed.write \
    .mode("overwrite") \
    .partitionBy("date") \
    .parquet("/data/warehouse/sales")
"""

# ============ COMPARISON ============
"""
┌─────────────────┬──────────────────────────────────────────────────────┐
│ Mode            │ Behavior                                             │
├─────────────────┼──────────────────────────────────────────────────────┤
│ STATIC (default)│ Deletes ENTIRE output directory, writes new data     │
│                 │ Safe only when rewriting ALL partitions               │
│                 │ Use for: Full refresh jobs                            │
├─────────────────┼──────────────────────────────────────────────────────┤
│ DYNAMIC         │ Only deletes partitions present in new data          │
│                 │ Preserves partitions not in new data                  │
│                 │ Use for: Incremental/daily loads                      │
└─────────────────┴──────────────────────────────────────────────────────┘

IMPORTANT: Dynamic mode only works with:
- mode("overwrite")
- partitionBy() in the write
- The partition columns must be in the DataFrame
"""

spark.stop()
