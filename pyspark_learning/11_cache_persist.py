"""
Topic: cache() and persist() - Data Caching
=============================================

Caching stores DataFrame in memory/disk to avoid recomputation.

Spark UI Behavior:
- cache()/persist() is LAZY - doesn't trigger a job by itself.
- First action after cache triggers computation + caching.
- Subsequent actions on cached df are MUCH faster (skip recomputation).
- In Spark UI -> Storage tab: shows cached DataFrames, size, partitions.
- Cached stages show green dot in DAG visualization.
- unpersist() removes from cache immediately.

Key Interview Points:
- cache() = persist(StorageLevel.MEMORY_ONLY) - stores deserialized in JVM heap.
- persist() allows choosing storage level (MEMORY_ONLY, MEMORY_AND_DISK, etc.).
- If data doesn't fit in memory with MEMORY_ONLY, partitions are recomputed on access.
- MEMORY_AND_DISK spills to disk instead of recomputing.
- MEMORY_ONLY_SER stores serialized (less memory, more CPU).
- Cache when: DataFrame is reused multiple times in the pipeline.
- Don't cache when: DataFrame is used only once, or data is too large.
- unpersist() to free memory when done.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count
from pyspark import StorageLevel

spark = SparkSession.builder \
    .appName("11_Cache_Persist") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

data = [(i, f"name_{i}", i % 5, i * 1000 + 30000) for i in range(1, 51)]
df = spark.createDataFrame(data, ["id", "name", "dept_id", "salary"])

# ============ WITHOUT CACHE ============
# Each action recomputes from scratch

# Expensive transformation
df_transformed = df.filter(col("salary") > 35000) \
    .withColumn("bonus", col("salary") * 0.15) \
    .withColumn("total_comp", col("salary") + col("salary") * 0.15)

# Action 1: Computes df_transformed from scratch
print("=== Action 1 (no cache): count ===")
print(f"Count: {df_transformed.count()}")

# Action 2: Computes df_transformed from scratch AGAIN
print("=== Action 2 (no cache): show ===")
df_transformed.show(5)

# Action 3: Computes df_transformed from scratch AGAIN
print("=== Action 3 (no cache): groupBy ===")
df_transformed.groupBy("dept_id").avg("salary").show()

# Each of the above triggered a FULL recomputation!

# ============ WITH CACHE ============
# Compute once, reuse from memory

# cache() is lazy - just marks for caching
df_cached = df_transformed.cache()

# First action: computes AND stores in memory
# Spark UI: Job runs, then data appears in Storage tab
print("\n=== First action (triggers caching) ===")
print(f"Count: {df_cached.count()}")

# Subsequent actions: read from cache (FAST, no recomputation)
# Spark UI: Job runs but skips computation stages (reads from cache)
print("=== Second action (from cache - fast!) ===")
df_cached.show(5)

print("=== Third action (from cache - fast!) ===")
df_cached.groupBy("dept_id").avg("salary").show()

# ============ PERSIST WITH STORAGE LEVELS ============

# Different storage levels for different scenarios
df_memory = df_transformed.persist(StorageLevel.MEMORY_ONLY)
# Best performance, but drops partitions if memory is full

df_memory_disk = df_transformed.persist(StorageLevel.MEMORY_AND_DISK)
# Spills to disk if memory is full (safer)

df_memory_ser = df_transformed.persist(StorageLevel.MEMORY_ONLY_SER)
# Serialized - uses less memory but more CPU to deserialize

df_disk = df_transformed.persist(StorageLevel.DISK_ONLY)
# Only disk - slowest but doesn't use memory

# ============ UNPERSIST ============
# Always unpersist when done to free resources

df_cached.unpersist()  # Removes from memory immediately
print("\n=== After unpersist: data removed from cache ===")

# ============ STORAGE LEVEL COMPARISON ============
"""
Storage Level          | Space | CPU  | In Memory | On Disk | Serialized
-----------------------|-------|------|-----------|---------|----------
MEMORY_ONLY            | High  | Low  | Yes       | No      | No
MEMORY_ONLY_SER        | Low   | High | Yes       | No      | Yes
MEMORY_AND_DISK        | High  | Med  | Yes       | Yes     | No
MEMORY_AND_DISK_SER    | Low   | High | Yes       | Yes     | Yes
DISK_ONLY              | Low   | High | No        | Yes     | Yes
"""

# ============ WHEN TO CACHE ============
"""
CACHE when:
1. DataFrame is used multiple times (reused in multiple actions)
2. DataFrame is expensive to compute (complex joins, aggregations)
3. Iterative algorithms (ML training loops)
4. Interactive exploration (notebook usage)

DON'T CACHE when:
1. DataFrame is used only once
2. Data is too large to fit in memory
3. Source data changes frequently
4. Simple transformations that are cheap to recompute
"""

# ============ PRACTICAL EXAMPLE ============
# Common pattern: cache after expensive join, reuse for multiple analyses

df_dept = spark.createDataFrame(
    [(0, "HR"), (1, "Eng"), (2, "Mkt"), (3, "Fin"), (4, "Ops")],
    ["dept_id", "dept_name"]
)

# Expensive operation: join
df_joined = df.join(df_dept, "dept_id", "inner").cache()

# Reuse cached result for multiple analyses
df_joined.groupBy("dept_name").agg(avg("salary")).show()
df_joined.groupBy("dept_name").agg(count("*")).show()
df_joined.groupBy("dept_name").agg(sum("salary")).show()

# Clean up
df_joined.unpersist()

# Write
df_transformed.write.mode("overwrite").parquet("/shared/cached_example")

spark.stop()
