"""
Topic: Small File Problem and Solutions
=========================================

Too many small files is a common production issue in Spark/HDFS.

Spark UI Behavior:
- Reading many small files: Spark UI shows many tasks in the read stage
  (1 task per file by default), each processing very little data.
- Task scheduling overhead dominates actual computation.
- In Spark UI -> Stages: Look for stages with many tasks but tiny input size.
- After fixing: Fewer tasks, each processing more data.

Key Interview Points:
- Small file problem: Thousands of tiny files (< 128MB each).
- Causes: Over-partitioning, frequent appends, high shuffle partitions.
- Impact: Excessive NameNode memory (HDFS), slow reads, scheduling overhead.
- Solutions: coalesce before write, repartition, compaction jobs, AQE.
- Rule of thumb: Target file size = 128MB - 1GB.
- spark.sql.files.maxPartitionBytes controls how files are combined on read.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, spark_partition_id, count

spark = SparkSession.builder \
    .appName("30_Small_File_Problem") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

data = [(i, f"name_{i}", i % 10, i * 1000) for i in range(1, 101)]
df = spark.createDataFrame(data, ["id", "name", "dept_id", "salary"])

# ============ CREATING THE PROBLEM ============

# Problem 1: High shuffle partitions -> many small output files
# After groupBy with 200 shuffle partitions, writing creates up to 200 files!
print("=== Problem: Too many files from high shuffle partitions ===")
df_grouped = df.groupBy("dept_id").count()
print(f"Partitions after groupBy: {df_grouped.rdd.getNumPartitions()}")
# Writing this creates 200 files (most empty for 10 groups)!

# Problem 2: Frequent appends create many small files
# Each append creates new files without merging with existing ones

# ============ SOLUTION 1: coalesce() before write ============

print("\n=== Solution 1: coalesce() before write ===")
df_grouped.coalesce(1).write.mode("overwrite").parquet("/shared/small_files_fix1")
print("Written with coalesce(1) -> 1 output file")

# For larger data, target reasonable file count
# Target: ~128MB per file
# If total data = 1GB, target = 1024/128 = 8 files
df_grouped.coalesce(4).write.mode("overwrite").parquet("/shared/small_files_fix1b")
print("Written with coalesce(4) -> 4 output files")

# ============ SOLUTION 2: repartition() before write ============

print("\n=== Solution 2: repartition() before write ===")
# Use when you need even distribution (coalesce can be uneven)
df.repartition(4).write.mode("overwrite").parquet("/shared/small_files_fix2")
print("Written with repartition(4) -> 4 evenly distributed files")

# ============ SOLUTION 3: Reduce shuffle partitions ============

print("\n=== Solution 3: Reduce spark.sql.shuffle.partitions ===")
spark.conf.set("spark.sql.shuffle.partitions", "4")
df_grouped2 = df.groupBy("dept_id").count()
print(f"Partitions with reduced setting: {df_grouped2.rdd.getNumPartitions()}")
df_grouped2.write.mode("overwrite").parquet("/shared/small_files_fix3")

# ============ SOLUTION 4: maxRecordsPerFile ============

print("\n=== Solution 4: maxRecordsPerFile (control file size) ===")
df.write.mode("overwrite") \
    .option("maxRecordsPerFile", 25) \
    .parquet("/shared/small_files_fix4")
# Creates files with at most 25 records each (100 rows / 25 = 4 files)

# ============ SOLUTION 5: AQE Coalescing ============

print("\n=== Solution 5: AQE auto-coalescing ===")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")  # Reset to high value

# AQE will automatically reduce 200 partitions to fewer based on data size
df_aqe = df.groupBy("dept_id").count()
df_aqe.write.mode("overwrite").parquet("/shared/small_files_fix5")
print(f"AQE coalesced partitions: {df_aqe.rdd.getNumPartitions()}")

# ============ SOLUTION 6: Compaction Job (for existing small files) ============

print("\n=== Solution 6: Compaction (read + coalesce + rewrite) ===")
# Read existing small files
df_small_files = spark.read.parquet("/shared/small_files_fix3")

# Rewrite with fewer, larger files
df_small_files.coalesce(2) \
    .write.mode("overwrite") \
    .parquet("/shared/small_files_compacted")
print("Compacted existing small files into 2 larger files")

# ============ READING SMALL FILES EFFICIENTLY ============

print("\n=== Reading small files: maxPartitionBytes ===")
# spark.sql.files.maxPartitionBytes (default 128MB)
# Spark combines multiple small files into one partition for reading
spark.conf.set("spark.sql.files.maxPartitionBytes", str(128 * 1024 * 1024))

# spark.sql.files.openCostInBytes (default 4MB)
# Estimated cost to open a file. Higher value = more files combined per partition
spark.conf.set("spark.sql.files.openCostInBytes", str(4 * 1024 * 1024))

# ============ BEST PRACTICES SUMMARY ============
"""
PREVENTING small files:
1. Set spark.sql.shuffle.partitions appropriately for your data size
2. Use coalesce(N) before write where N = total_size / target_file_size
3. Enable AQE for automatic coalescing
4. Use maxRecordsPerFile for predictable file sizes

FIXING existing small files:
1. Run compaction jobs: read -> coalesce -> rewrite
2. Use Delta Lake / Iceberg OPTIMIZE command (auto-compaction)
3. Schedule periodic compaction in your pipeline

READING small files efficiently:
1. spark.sql.files.maxPartitionBytes combines files on read
2. spark.sql.files.openCostInBytes adjusts combining threshold

TARGET FILE SIZES:
- HDFS: 128MB - 1GB (match HDFS block size)
- Cloud storage (S3/GCS): 256MB - 1GB (fewer API calls)
- Local: Depends on use case, but avoid < 1MB files
"""

spark.stop()
