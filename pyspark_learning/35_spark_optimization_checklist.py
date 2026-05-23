"""
Topic: Spark Optimization Checklist - Complete Interview Guide
===============================================================

A comprehensive checklist of all optimization techniques.

This file serves as a quick reference for interviews.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, count, sum

spark = SparkSession.builder \
    .appName("35_Optimization_Checklist") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# ============ OPTIMIZATION CHECKLIST ============
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    SPARK OPTIMIZATION CHECKLIST                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  1. DATA FORMAT                                                              ║
║  ─────────────────                                                           ║
║  ✓ Use Parquet/ORC (columnar, compressed, predicate pushdown)               ║
║  ✓ Avoid CSV/JSON for large datasets                                        ║
║  ✓ Use Snappy compression (fast) or ZSTD (better ratio)                    ║
║  ✓ Partition data by frequently filtered columns                            ║
║                                                                              ║
║  2. SHUFFLE OPTIMIZATION                                                     ║
║  ─────────────────────                                                       ║
║  ✓ Minimize number of shuffles (combine operations)                         ║
║  ✓ Set spark.sql.shuffle.partitions appropriately                           ║
║  ✓ Use broadcast join for small tables (< 10MB)                             ║
║  ✓ Enable AQE for auto-coalescing and skew handling                         ║
║  ✓ Use bucketing for repeated joins on same key                             ║
║                                                                              ║
║  3. DATA SKEW                                                                ║
║  ────────────────                                                            ║
║  ✓ Identify skew: Check task duration distribution in Spark UI              ║
║  ✓ Salting: Split hot keys into sub-keys                                    ║
║  ✓ Broadcast join: Eliminate shuffle entirely for small tables              ║
║  ✓ Isolate hot keys: Process separately with broadcast                      ║
║  ✓ Enable AQE skew join handling                                            ║
║  ✓ Two-phase aggregation for skewed groupBy                                 ║
║                                                                              ║
║  4. MEMORY MANAGEMENT                                                        ║
║  ─────────────────────                                                       ║
║  ✓ Cache DataFrames that are reused multiple times                          ║
║  ✓ Unpersist when done to free memory                                       ║
║  ✓ Use MEMORY_AND_DISK for large cached datasets                            ║
║  ✓ Increase partitions to reduce per-task memory                            ║
║  ✓ Avoid collect() on large DataFrames                                      ║
║  ✓ Use Kryo serializer for RDD operations                                   ║
║                                                                              ║
║  5. PARALLELISM                                                              ║
║  ──────────────                                                              ║
║  ✓ 2-4 partitions per CPU core                                              ║
║  ✓ Target 128MB-256MB per partition                                         ║
║  ✓ Use repartition() to increase parallelism                                ║
║  ✓ Use coalesce() to reduce partitions (no shuffle)                         ║
║                                                                              ║
║  6. CODE OPTIMIZATION                                                        ║
║  ─────────────────────                                                       ║
║  ✓ Use built-in functions over UDFs                                         ║
║  ✓ Use Pandas UDFs over regular UDFs when UDF is needed                     ║
║  ✓ Filter early (reduce data before expensive operations)                   ║
║  ✓ Select only needed columns (column pruning)                              ║
║  ✓ Avoid unnecessary actions (each action = new job)                        ║
║  ✓ Use select() with multiple expressions over multiple withColumn()        ║
║                                                                              ║
║  7. JOIN OPTIMIZATION                                                        ║
║  ─────────────────────                                                       ║
║  ✓ Broadcast small tables (< 10MB, or force with broadcast())              ║
║  ✓ Filter before join (reduce shuffle data)                                 ║
║  ✓ Use bucketing for repeated joins                                         ║
║  ✓ Handle null keys (they never match in joins)                             ║
║  ✓ Choose correct join type (don't use outer when inner suffices)           ║
║                                                                              ║
║  8. I/O OPTIMIZATION                                                         ║
║  ─────────────────────                                                       ║
║  ✓ Avoid small file problem (coalesce before write)                         ║
║  ✓ Use partitionBy for write (enables partition pruning on read)            ║
║  ✓ Use predicate pushdown (filter on Parquet columns)                       ║
║  ✓ Provide explicit schema (avoid inferSchema extra job)                    ║
║  ✓ Use dynamic partition overwrite mode                                     ║
║                                                                              ║
║  9. CLUSTER CONFIGURATION                                                    ║
║  ──────────────────────────                                                  ║
║  ✓ spark.executor.cores = 5 (optimal for HDFS)                              ║
║  ✓ spark.executor.memory = sized for workload                               ║
║  ✓ Enable dynamic allocation for varying workloads                          ║
║  ✓ Use speculation for heterogeneous clusters                               ║
║  ✓ Set appropriate driver memory for collect/broadcast                      ║
║                                                                              ║
║  10. MONITORING                                                              ║
║  ──────────────                                                              ║
║  ✓ Check Spark UI for skew (task duration variance)                         ║
║  ✓ Check for spill (memory/disk) in stage metrics                           ║
║  ✓ Check GC time in executor metrics                                        ║
║  ✓ Use explain() to verify optimization (pushdown, broadcast)               ║
║  ✓ Monitor shuffle read/write sizes                                         ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

# ============ COMMON INTERVIEW SCENARIOS ============
"""
SCENARIO 1: "Your Spark job is slow. How do you debug?"
─────────────────────────────────────────────────────────
1. Check Spark UI -> Jobs -> Find the slow job
2. Click into the slow stage
3. Look at task metrics:
   - If one task is much slower → DATA SKEW
   - If all tasks are slow → Not enough resources or too much data per task
   - If high GC time → Memory pressure
   - If spill to disk → Increase memory or partitions
4. Check the DAG for unnecessary shuffles
5. Check explain() plan for missing optimizations

SCENARIO 2: "OOM error in your Spark job"
──────────────────────────────────────────
1. Where is OOM? Driver or Executor?
   - Driver: collect(), broadcast too large, too many partitions tracked
   - Executor: Skew, too much data per task, large broadcast
2. Solutions:
   - Increase memory (spark.executor.memory / spark.driver.memory)
   - Increase partitions (reduce data per task)
   - Fix skew (salting, broadcast)
   - Avoid collect() (use take/show instead)
   - Use MEMORY_AND_DISK for cache

SCENARIO 3: "Join between large table and small table is slow"
──────────────────────────────────────────────────────────────
1. Is the small table < 10MB? → Use broadcast join
2. Is there skew in join key? → Salt the hot keys
3. Are both tables large? → 
   - Bucket both tables by join key
   - Filter before join to reduce data
   - Increase shuffle partitions

SCENARIO 4: "Too many small files in output"
─────────────────────────────────────────────
1. coalesce(N) before write (N = total_size / target_file_size)
2. Reduce spark.sql.shuffle.partitions
3. Use maxRecordsPerFile option
4. Enable AQE coalescing
5. Run compaction job periodically

SCENARIO 5: "Spark job works in dev but fails in production"
─────────────────────────────────────────────────────────────
1. Data volume difference (skew appears at scale)
2. Memory settings too low for production data
3. Shuffle partitions too low (200 default may not be enough)
4. Network issues (shuffle fetch failures)
5. Data quality issues (nulls, unexpected values)
"""

# ============ QUICK DEMO: Before and After Optimization ============

# Sample data
orders = [(i, i % 100, i * 10) for i in range(1, 201)]
stores = [(i, f"Store_{i}") for i in range(1, 101)]

df_orders = spark.createDataFrame(orders, ["order_id", "store_id", "amount"])
df_stores = spark.createDataFrame(stores, ["store_id", "store_name"])

# BEFORE: Unoptimized
print("=== BEFORE Optimization ===")
result_before = df_orders \
    .join(df_stores, "store_id", "inner") \
    .groupBy("store_name") \
    .agg(sum("amount").alias("total"))
result_before.explain()

# AFTER: Optimized with broadcast
print("\n=== AFTER Optimization (broadcast join) ===")
result_after = df_orders \
    .join(broadcast(df_stores), "store_id", "inner") \
    .groupBy("store_name") \
    .agg(sum("amount").alias("total"))
result_after.explain()
# Notice: BroadcastHashJoin instead of SortMergeJoin (no shuffle on orders!)

result_after.show(5)

# Write
result_after.write.mode("overwrite").parquet("/shared/optimization_demo")

spark.stop()
