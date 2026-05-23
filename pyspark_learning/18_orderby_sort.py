"""
Topic: orderBy() / sort() and sortWithinPartitions()
======================================================

Sorting operations in Spark.

Spark UI Behavior:
- orderBy() / sort(): WIDE transformation -> causes SHUFFLE (range partitioning)
  Job -> 2 stages: Stage 0 (read + sample for range) | Stage 1 (shuffle + sort)
  This is a GLOBAL sort - expensive for large datasets!
  
- sortWithinPartitions(): NARROW transformation -> NO shuffle
  Sorts data within each partition independently.
  Job -> 1 stage only.

Key Interview Points:
- orderBy() and sort() are identical (aliases).
- Global sort requires shuffle (range partitioning) - expensive!
- sortWithinPartitions() is much cheaper (no shuffle, local sort).
- Use sortWithinPartitions() when global order isn't needed (e.g., before write).
- For "Top N" queries, use limit() which is optimized (doesn't sort everything).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, asc, spark_partition_id

spark = SparkSession.builder \
    .appName("18_OrderBy_Sort") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

data = [
    (1, "Alice", "Engineering", 90000),
    (2, "Bob", "Marketing", 45000),
    (3, "Charlie", "Engineering", 65000),
    (4, "Diana", "HR", 55000),
    (5, "Eve", "Marketing", 70000),
    (6, "Frank", "Engineering", 80000),
    (7, "Grace", "HR", 60000),
    (8, "Henry", "Marketing", 52000)
]

df = spark.createDataFrame(data, ["id", "name", "department", "salary"])

# ============ orderBy / sort (Global Sort - SHUFFLE) ============

# Ascending (default)
# Spark UI: 1 job -> 2 stages (range partition + sort)
print("=== Order by salary ASC ===")
df.orderBy("salary").show()

# Descending
print("=== Order by salary DESC ===")
df.orderBy(col("salary").desc()).show()

# Multiple columns
print("=== Order by department ASC, salary DESC ===")
df.orderBy(col("department").asc(), col("salary").desc()).show()

# Using sort() - same as orderBy()
print("=== sort() is same as orderBy() ===")
df.sort(desc("salary")).show()

# ============ sortWithinPartitions (Local Sort - NO SHUFFLE) ============

# Repartition first to see the effect
df_repart = df.repartition(2, "department")

print("=== Before sortWithinPartitions ===")
df_repart.withColumn("partition", spark_partition_id()).show()

# Sort within each partition (no shuffle!)
# Spark UI: 1 stage only (no Exchange node in plan)
print("=== After sortWithinPartitions ===")
df_sorted_local = df_repart.sortWithinPartitions(col("salary").desc())
df_sorted_local.withColumn("partition", spark_partition_id()).show()

# Compare plans
print("=== Plan: orderBy (has Exchange/Shuffle) ===")
df.orderBy("salary").explain()

print("\n=== Plan: sortWithinPartitions (NO Exchange) ===")
df_repart.sortWithinPartitions("salary").explain()

# ============ PRACTICAL USE: Sort before write ============
# When writing partitioned data, sort within partitions for better compression

df.repartition("department") \
    .sortWithinPartitions("department", "salary") \
    .write.mode("overwrite").parquet("/shared/sorted_employees")

spark.stop()
