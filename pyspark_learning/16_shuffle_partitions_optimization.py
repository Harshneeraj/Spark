"""
Topic: Shuffle Partitions and Parallelism Optimization
=======================================================

Controlling shuffle partitions is critical for Spark performance.

Spark UI Behavior:
- spark.sql.shuffle.partitions controls tasks in shuffle stages.
- Default 200: After any wide transformation, Stage N+1 has 200 tasks.
- Too many partitions (200 for small data): Many tiny tasks, scheduling overhead.
- Too few partitions (2 for big data): Few large tasks, memory pressure, underutilization.
- In Spark UI -> Stages -> Tasks: Look at "Input Size" per task for balance.

Key Interview Points:
- spark.sql.shuffle.partitions (default 200): partitions after shuffle operations.
- spark.default.parallelism: partitions for RDD operations (not DataFrame).
- Rule of thumb: 2-4x number of cores, or target 128MB-200MB per partition.
- For 10GB data with 128MB target: ~80 partitions.
- AQE can auto-coalesce, but setting a good initial value helps.
- Too many small partitions = "small file problem" when writing.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, spark_partition_id
import random

spark = SparkSession.builder \
    .appName("16_Shuffle_Partitions_Optimization") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

# Create sample data
data = [(i, f"name_{i}", i % 10, random.randint(30000, 90000)) for i in range(1, 101)]
df = spark.createDataFrame(data, ["id", "name", "dept_id", "salary"])

# ============ PROBLEM: Default 200 Shuffle Partitions for Small Data ============

print("=== With default 200 shuffle partitions ===")
df_grouped = df.groupBy("dept_id").count()
print(f"Partitions after groupBy: {df_grouped.rdd.getNumPartitions()}")
# 200 partitions for just 10 groups! Most partitions are EMPTY.

# Show partition distribution
print("Partition distribution (most are empty):")
df_grouped.withColumn("partition", spark_partition_id()) \
    .groupBy("partition").count() \
    .orderBy("partition").show(20)

# ============ SOLUTION: Set Appropriate Shuffle Partitions ============

spark.conf.set("spark.sql.shuffle.partitions", "8")

print("\n=== With optimized 8 shuffle partitions ===")
df_grouped_opt = df.groupBy("dept_id").count()
print(f"Partitions after groupBy: {df_grouped_opt.rdd.getNumPartitions()}")

df_grouped_opt.withColumn("partition", spark_partition_id()) \
    .groupBy("partition").count() \
    .orderBy("partition").show()

# ============ CALCULATING OPTIMAL PARTITIONS ============
"""
Formula for optimal partition count:
  optimal_partitions = total_data_size / target_partition_size

Where:
  target_partition_size = 128MB to 256MB (recommended)

Example:
  Data size = 50GB
  Target partition size = 200MB
  Optimal partitions = 50000MB / 200MB = 250 partitions

Also consider:
  - Number of executor cores available
  - At least 2-4 partitions per core for good parallelism
  - Example: 10 executors * 4 cores = 40 cores -> at least 80-160 partitions
"""

# ============ DYNAMIC PARTITION SETTING ============

# You can change shuffle partitions mid-session
spark.conf.set("spark.sql.shuffle.partitions", "4")
df_4parts = df.groupBy("dept_id").count()
print(f"\nWith 4 partitions: {df_4parts.rdd.getNumPartitions()}")

spark.conf.set("spark.sql.shuffle.partitions", "16")
df_16parts = df.groupBy("dept_id").count()
print(f"With 16 partitions: {df_16parts.rdd.getNumPartitions()}")

# ============ INPUT PARTITIONS vs SHUFFLE PARTITIONS ============
"""
Two different concepts:
1. INPUT partitions: How data is read (depends on source)
   - HDFS: 1 partition per block (128MB default)
   - Parquet: 1 partition per file/row group
   - Controlled by: spark.sql.files.maxPartitionBytes (128MB default)

2. SHUFFLE partitions: Partitions after wide transformations
   - Controlled by: spark.sql.shuffle.partitions (200 default)
   - Applies to: groupBy, join, repartition, distinct, etc.
"""

print("\n=== Input vs Shuffle Partitions ===")
print(f"Input partitions (from createDataFrame): {df.rdd.getNumPartitions()}")

spark.conf.set("spark.sql.shuffle.partitions", "8")
df_after_shuffle = df.groupBy("dept_id").agg(sum("salary"))
print(f"Shuffle partitions (after groupBy): {df_after_shuffle.rdd.getNumPartitions()}")

# ============ PARTITION SIZE MONITORING ============

# Check actual partition sizes (useful for tuning)
print("\n=== Partition Sizes ===")
partition_sizes = df.withColumn("partition", spark_partition_id()) \
    .groupBy("partition") \
    .agg(count("*").alias("row_count"))
partition_sizes.show()

# ============ BEST PRACTICES ============
"""
1. For small data (< 1GB): Set shuffle partitions to 2-10
2. For medium data (1-10GB): Set to 50-200
3. For large data (10-100GB): Set to 200-2000
4. For very large data (> 100GB): Set to 2000+

5. Enable AQE (Spark 3.x) to auto-coalesce:
   spark.sql.adaptive.enabled = true
   spark.sql.adaptive.coalescePartitions.enabled = true

6. Monitor in Spark UI:
   - If tasks finish in < 100ms: too many partitions
   - If tasks take > 5 minutes: too few partitions
   - If some tasks are 10x slower: data skew (not partition count issue)
"""

# Write with controlled output files
spark.conf.set("spark.sql.shuffle.partitions", "4")
df.groupBy("dept_id").agg(sum("salary").alias("total_salary")) \
    .coalesce(1) \
    .write.mode("overwrite").parquet("/shared/partition_optimized")

spark.stop()
