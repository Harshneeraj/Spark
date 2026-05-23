"""
Topic: repartition() vs coalesce() - Partition Management
==========================================================

Controls how data is distributed across partitions.

Spark UI Behavior:
- repartition(N): WIDE transformation -> causes FULL SHUFFLE
  Job -> 2 stages: Stage 0 (read) | Stage 1 (shuffle to N partitions)
  Tasks in Stage 1 = N (target partitions)
  
- coalesce(N): NARROW transformation -> NO shuffle (when reducing)
  Job -> 1 stage only
  Combines partitions locally without moving data across network
  
- repartition(N, col): Shuffle by column -> data with same key goes to same partition
  Used before joins/groupBy to co-locate data

Key Interview Points:
- coalesce() can only REDUCE partitions (no shuffle, narrow transformation).
- repartition() can increase OR decrease partitions (always shuffles).
- Use coalesce() before writing to reduce small files (common interview Q).
- Use repartition() when you need even distribution or partition by column.
- repartition() by column is useful before repeated joins on same key.
- Too few partitions = underutilization, too many = overhead.
- Rule of thumb: 2-4 partitions per CPU core.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, spark_partition_id

spark = SparkSession.builder \
    .appName("10_Repartition_Coalesce") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

data = [(i, f"name_{i}", i % 5, i * 1000) for i in range(1, 21)]
df = spark.createDataFrame(data, ["id", "name", "dept_id", "salary"])

# ============ CHECK CURRENT PARTITIONS ============

print(f"Default partitions: {df.rdd.getNumPartitions()}")

# Show which partition each row is in
print("=== Data Distribution (default) ===")
df.withColumn("partition_id", spark_partition_id()).show()

# ============ REPARTITION (Full Shuffle) ============

# Repartition to 4 partitions (SHUFFLE happens)
# Spark UI: 2 stages - read | shuffle to 4 partitions
df_repart = df.repartition(4)
print(f"\nAfter repartition(4): {df_repart.rdd.getNumPartitions()} partitions")
df_repart.withColumn("partition_id", spark_partition_id()).show()

# Repartition by column - same dept_id goes to same partition
# Spark UI: 2 stages - read | hash partition by dept_id
df_repart_col = df.repartition(4, "dept_id")
print(f"\nAfter repartition(4, 'dept_id'): {df_repart_col.rdd.getNumPartitions()} partitions")
print("=== Notice: same dept_id in same partition ===")
df_repart_col.withColumn("partition_id", spark_partition_id()) \
    .orderBy("dept_id", "id").show(20)

# ============ COALESCE (No Shuffle - Narrow) ============

# First repartition to 8, then coalesce down to 2
df_8parts = df.repartition(8)
print(f"\nAfter repartition(8): {df_8parts.rdd.getNumPartitions()} partitions")

# Coalesce to 2 - NO shuffle, just combines adjacent partitions
# Spark UI: same stage, no new shuffle
df_coalesced = df_8parts.coalesce(2)
print(f"After coalesce(2): {df_coalesced.rdd.getNumPartitions()} partitions")
df_coalesced.withColumn("partition_id", spark_partition_id()).show()

# ============ PRACTICAL USE CASE: Write Optimization ============

# Problem: After groupBy, you get 200 partitions (default shuffle partitions)
# Writing 200 small files is inefficient!
df_grouped = df.groupBy("dept_id").count()
print(f"\nAfter groupBy: {df_grouped.rdd.getNumPartitions()} partitions")

# Solution: coalesce before write to reduce file count
df_grouped.coalesce(1).write.mode("overwrite").parquet("/shared/dept_counts")
# This writes 1 file instead of 200!

# ============ REPARTITION BY COLUMN FOR WRITE ============

# Write partitioned by dept_id (creates directory structure)
# /shared/partitioned_data/dept_id=0/, dept_id=1/, etc.
df.repartition("dept_id") \
    .write.mode("overwrite") \
    .partitionBy("dept_id") \
    .parquet("/shared/partitioned_data")

# ============ WHEN TO USE WHICH ============
"""
USE coalesce(N) when:
- Reducing partitions before write (avoid small files)
- After filter that removes most data (many empty partitions)
- You want to avoid shuffle overhead

USE repartition(N) when:
- Need to INCREASE partitions (coalesce can't do this)
- Need EVEN distribution (coalesce may create skewed partitions)
- Before a heavy computation that benefits from parallelism

USE repartition(N, col) when:
- Before repeated joins on same column
- Before groupBy on same column (data locality)
- Writing partitioned output (partitionBy in write)
"""

print("\n=== Summary ===")
print(f"repartition(4): Full shuffle, even distribution, 2 stages")
print(f"coalesce(2): No shuffle, combines locally, 1 stage")
print(f"repartition(4, col): Hash shuffle by column, co-locates keys")

spark.stop()
