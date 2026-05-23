"""
Topic: Bucketing - Pre-Shuffle Optimization
=============================================

Bucketing pre-partitions data by a column during write, so subsequent
joins/groupBy on that column avoid shuffle.

Spark UI Behavior:
- Writing bucketed table: 1 job (data is hash-partitioned during write).
- Reading bucketed table + join on bucket column:
  WITHOUT bucketing: 3 stages (shuffle both sides + join)
  WITH bucketing: 1 stage (no shuffle needed - data already co-located!)
- Spark UI shows NO Exchange node for bucketed joins.

Key Interview Points:
- Bucketing stores data pre-shuffled by a column into N buckets (files).
- Joins on bucketed columns skip shuffle entirely (huge performance gain).
- Both tables must be bucketed by the SAME column with SAME number of buckets.
- Bucketing is useful for tables that are repeatedly joined on the same key.
- Must use saveAsTable() (not write.parquet()) for bucketing.
- Bucketing info is stored in the metastore (Hive catalog).
- Trade-off: Slower write (extra hash partitioning) for faster reads/joins.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("21_Bucketing") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.sources.bucketing.enabled", "true") \
    .config("spark.sql.warehouse.dir", "/shared/spark-warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# Create sample data
orders = [
    (1, 101, "2024-01-01", 500.0),
    (2, 102, "2024-01-02", 300.0),
    (3, 101, "2024-01-03", 700.0),
    (4, 103, "2024-01-04", 200.0),
    (5, 102, "2024-01-05", 900.0),
    (6, 101, "2024-01-06", 400.0),
    (7, 104, "2024-01-07", 600.0),
    (8, 103, "2024-01-08", 350.0)
]

customers = [
    (101, "Alice", "New York"),
    (102, "Bob", "Chicago"),
    (103, "Charlie", "Denver"),
    (104, "Diana", "Austin")
]

df_orders = spark.createDataFrame(orders, ["order_id", "customer_id", "order_date", "amount"])
df_customers = spark.createDataFrame(customers, ["customer_id", "name", "city"])

# ============ WRITE BUCKETED TABLES ============

# Bucket orders by customer_id into 4 buckets
df_orders.write.mode("overwrite") \
    .bucketBy(4, "customer_id") \
    .sortBy("order_date") \
    .saveAsTable("orders_bucketed")

# Bucket customers by customer_id into 4 buckets (SAME column, SAME count!)
df_customers.write.mode("overwrite") \
    .bucketBy(4, "customer_id") \
    .saveAsTable("customers_bucketed")

# ============ JOIN WITHOUT BUCKETING (has shuffle) ============

print("=== Join WITHOUT bucketing (shuffle required) ===")
df_orders.join(df_customers, "customer_id", "inner").explain()
# Shows Exchange (shuffle) on both sides

# ============ JOIN WITH BUCKETING (no shuffle!) ============

print("\n=== Join WITH bucketing (no shuffle!) ===")
df_orders_b = spark.table("orders_bucketed")
df_customers_b = spark.table("customers_bucketed")

df_bucketed_join = df_orders_b.join(df_customers_b, "customer_id", "inner")
df_bucketed_join.explain()
# NO Exchange node! Data is already co-located by customer_id

df_bucketed_join.show()

# ============ GROUPBY WITH BUCKETING ============

print("\n=== GroupBy on bucketed column (no shuffle!) ===")
df_orders_b.groupBy("customer_id").sum("amount").explain()
# No shuffle needed for groupBy on bucket column

df_orders_b.groupBy("customer_id").sum("amount").show()

# ============ BUCKETING BEST PRACTICES ============
"""
When to use bucketing:
1. Tables that are repeatedly joined on the same column
2. Tables that are frequently grouped by the same column
3. Large tables where shuffle is the bottleneck

When NOT to use:
1. Small tables (broadcast join is better)
2. Tables joined on different columns each time
3. Frequently changing data (re-bucketing is expensive)

Rules:
1. Both tables must have SAME number of buckets for shuffle-free join
2. Join column must be the bucket column
3. Must use saveAsTable (needs metastore)
4. sortBy within buckets helps with sort-merge operations
"""

# ============ CHECKING BUCKET INFO ============

print("\n=== Table Metadata ===")
spark.sql("DESCRIBE EXTENDED orders_bucketed").show(50, truncate=False)

# Write final result
df_bucketed_join.write.mode("overwrite").parquet("/shared/bucketed_join_result")

spark.stop()
