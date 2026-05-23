"""
Topic: Important Spark Configurations for Interviews
======================================================

All the critical Spark configurations you should know.

Spark UI Behavior:
- All configurations visible in Spark UI -> Environment tab.
- Changing configs at runtime (spark.conf.set) reflects immediately.
- Some configs are static (must be set before SparkSession creation).
- Some configs are dynamic (can be changed mid-session).

Key Interview Points:
- Know the default values and when to change them.
- Understand the impact of each configuration on performance.
- Be able to explain trade-offs for each setting.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("24_Spark_Configurations") \
    .master("local[*]") \
    .getOrCreate()

# ============ CLUSTER / RESOURCE CONFIGURATIONS ============
"""
┌─────────────────────────────────────────────────────────────────────────────┐
│ RESOURCE CONFIGURATIONS (Static - set at submit time)                       │
├─────────────────────────────────────────────────────────────────────────────┤
│ Config                          │ Default │ Description                     │
├─────────────────────────────────┼─────────┼─────────────────────────────────┤
│ spark.driver.memory             │ 1g      │ Driver JVM heap                 │
│ spark.driver.cores              │ 1       │ Driver CPU cores                │
│ spark.executor.memory           │ 1g      │ Executor JVM heap               │
│ spark.executor.cores            │ 1       │ Cores per executor              │
│ spark.executor.instances        │ 2       │ Number of executors             │
│ spark.executor.memoryOverhead   │ 10%     │ Off-heap per executor (YARN)    │
│ spark.dynamicAllocation.enabled │ false   │ Auto-scale executors            │
│ spark.dynamicAllocation.min     │ 0       │ Min executors with dynamic      │
│ spark.dynamicAllocation.max     │ inf     │ Max executors with dynamic      │
└─────────────────────────────────┴─────────┴─────────────────────────────────┘

INTERVIEW TIP: How to size a Spark cluster?
Example: 10 nodes, 16 cores each, 64GB RAM each

Option A (Fat executors - NOT recommended):
  1 executor per node, 16 cores, 64GB
  Problem: GC pressure, HDFS throughput issues (max 5 threads recommended)

Option B (Recommended):
  spark.executor.cores = 5 (good for HDFS parallelism)
  Executors per node = 16/5 = 3 (leave 1 core for OS)
  spark.executor.memory = (64GB - overhead) / 3 ≈ 18-19GB
  spark.executor.instances = 10 * 3 - 1 = 29 (1 for ApplicationMaster)
"""

# ============ SHUFFLE CONFIGURATIONS ============
"""
┌─────────────────────────────────────────────────────────────────────────────┐
│ SHUFFLE CONFIGURATIONS                                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│ Config                              │ Default │ Description                 │
├─────────────────────────────────────┼─────────┼─────────────────────────────┤
│ spark.sql.shuffle.partitions        │ 200     │ Partitions after shuffle    │
│ spark.shuffle.compress              │ true    │ Compress shuffle output     │
│ spark.shuffle.spill.compress        │ true    │ Compress spill files        │
│ spark.sql.shuffle.sort.bypassThreshold│ 200   │ Bypass sort for few parts   │
│ spark.reducer.maxSizeInFlight       │ 48MB    │ Buffer for shuffle fetch    │
│ spark.shuffle.file.buffer           │ 32KB    │ Buffer for shuffle write    │
│ spark.shuffle.io.maxRetries         │ 3       │ Retry failed shuffle fetch  │
│ spark.shuffle.io.retryWait          │ 5s      │ Wait between retries        │
└─────────────────────────────────────┴─────────┴─────────────────────────────┘
"""

# ============ JOIN CONFIGURATIONS ============
"""
┌─────────────────────────────────────────────────────────────────────────────┐
│ JOIN CONFIGURATIONS                                                         │
├─────────────────────────────────────────────────────────────────────────────┤
│ Config                                    │ Default │ Description            │
├───────────────────────────────────────────┼─────────┼────────────────────────┤
│ spark.sql.autoBroadcastJoinThreshold      │ 10MB    │ Auto-broadcast if <    │
│ spark.sql.join.preferSortMergeJoin        │ true    │ Prefer SMJ over HJ     │
│ spark.sql.broadcastTimeout                │ 300s    │ Timeout for broadcast  │
└───────────────────────────────────────────┴─────────┴────────────────────────┘

Join Strategy Selection:
1. BroadcastHashJoin: One side < autoBroadcastJoinThreshold
2. SortMergeJoin: Default for large-large joins (both sides shuffled + sorted)
3. ShuffleHashJoin: When one side is much smaller (but > broadcast threshold)
"""

# ============ AQE CONFIGURATIONS ============
"""
┌─────────────────────────────────────────────────────────────────────────────┐
│ ADAPTIVE QUERY EXECUTION (Spark 3.x)                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│ Config                                              │ Default │ Description  │
├─────────────────────────────────────────────────────┼─────────┼──────────────┤
│ spark.sql.adaptive.enabled                          │ true*   │ Enable AQE   │
│ spark.sql.adaptive.coalescePartitions.enabled       │ true    │ Merge small  │
│ spark.sql.adaptive.coalescePartitions.minPartitionSize│ 1MB   │ Min size     │
│ spark.sql.adaptive.advisoryPartitionSizeInBytes     │ 64MB   │ Target size  │
│ spark.sql.adaptive.skewJoin.enabled                 │ true    │ Handle skew  │
│ spark.sql.adaptive.skewJoin.skewedPartitionFactor   │ 5      │ Skew factor  │
│ spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes│256MB│Min skew sz│
│ spark.sql.adaptive.localShuffleReader.enabled       │ true    │ Local read   │
└─────────────────────────────────────────────────────┴─────────┴──────────────┘
* Default true in Spark 3.2+, false in 3.0-3.1
"""

# ============ SERIALIZATION CONFIGURATIONS ============
"""
┌─────────────────────────────────────────────────────────────────────────────┐
│ SERIALIZATION                                                               │
├─────────────────────────────────────────────────────────────────────────────┤
│ Config                          │ Default        │ Description              │
├─────────────────────────────────┼────────────────┼──────────────────────────┤
│ spark.serializer                │ JavaSerializer │ Use KryoSerializer!      │
│ spark.kryoserializer.buffer.max │ 64MB           │ Max Kryo buffer          │
│ spark.sql.execution.arrow.enabled│ false         │ Arrow for toPandas()     │
└─────────────────────────────────┴────────────────┴──────────────────────────┘

INTERVIEW TIP: Always use Kryo serializer for better performance:
  spark.serializer = org.apache.spark.serializer.KryoSerializer
  Kryo is 10x faster than Java serialization and more compact.
"""

# ============ FILE/IO CONFIGURATIONS ============
"""
┌─────────────────────────────────────────────────────────────────────────────┐
│ FILE AND I/O                                                                │
├─────────────────────────────────────────────────────────────────────────────┤
│ Config                                │ Default │ Description               │
├───────────────────────────────────────┼─────────┼───────────────────────────┤
│ spark.sql.files.maxPartitionBytes     │ 128MB   │ Max bytes per partition   │
│ spark.sql.files.openCostInBytes       │ 4MB     │ Cost to open a file       │
│ spark.sql.parquet.compression.codec   │ snappy  │ Parquet compression       │
│ spark.sql.sources.partitionOverwriteMode│static │ static vs dynamic         │
│ spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version│ 2 │ Commit algo│
└───────────────────────────────────────┴─────────┴───────────────────────────┘

INTERVIEW TIP: Dynamic partition overwrite mode:
  spark.sql.sources.partitionOverwriteMode = dynamic
  Only overwrites partitions that have new data (not all partitions!)
"""

# ============ SPECULATION AND FAULT TOLERANCE ============
"""
┌─────────────────────────────────────────────────────────────────────────────┐
│ SPECULATION (handling slow tasks)                                            │
├─────────────────────────────────────────────────────────────────────────────┤
│ Config                          │ Default │ Description                     │
├─────────────────────────────────┼─────────┼─────────────────────────────────┤
│ spark.speculation                │ false   │ Enable speculative execution   │
│ spark.speculation.multiplier     │ 1.5     │ Task is slow if 1.5x median   │
│ spark.speculation.quantile       │ 0.75    │ Start after 75% tasks done    │
│ spark.task.maxFailures           │ 4       │ Max retries per task           │
└─────────────────────────────────┴─────────┴─────────────────────────────────┘

Speculation: Launches duplicate of slow tasks on other executors.
First to finish wins, other is killed.
Useful for: Heterogeneous clusters, noisy neighbors.
Risky for: Non-idempotent operations (writes without proper commit protocol).
"""

# ============ GARBAGE COLLECTION ============
"""
┌─────────────────────────────────────────────────────────────────────────────┐
│ GC TUNING                                                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│ Config                              │ Recommendation                        │
├─────────────────────────────────────┼───────────────────────────────────────┤
│ spark.executor.extraJavaOptions     │ -XX:+UseG1GC -XX:G1HeapRegionSize=16m│
│                                     │ Use G1GC for large heaps (>4GB)       │
│ spark.executor.extraJavaOptions     │ -XX:+PrintGCDetails -XX:+PrintGCTime │
│                                     │ Enable GC logging for debugging       │
└─────────────────────────────────────┴───────────────────────────────────────┘
"""

# ============ PRINT ALL CURRENT CONFIGS ============

print("=== Current Spark Configuration ===\n")
important_configs = [
    "spark.sql.shuffle.partitions",
    "spark.sql.autoBroadcastJoinThreshold",
    "spark.sql.adaptive.enabled",
    "spark.default.parallelism",
    "spark.driver.memory",
    "spark.executor.memory",
    "spark.memory.fraction",
    "spark.memory.storageFraction",
    "spark.serializer",
    "spark.sql.parquet.compression.codec",
]

for config in important_configs:
    try:
        value = spark.conf.get(config)
        print(f"  {config} = {value}")
    except Exception:
        print(f"  {config} = (default)")

# ============ DYNAMIC vs STATIC CONFIGS ============
"""
DYNAMIC (can change at runtime with spark.conf.set()):
- spark.sql.shuffle.partitions
- spark.sql.autoBroadcastJoinThreshold
- spark.sql.adaptive.enabled
- spark.sql.adaptive.skewJoin.enabled

STATIC (must set before SparkSession, or at spark-submit):
- spark.driver.memory
- spark.executor.memory
- spark.executor.cores
- spark.executor.instances
- spark.serializer
"""

# Example of dynamic config change
spark.conf.set("spark.sql.shuffle.partitions", "50")
print(f"\nDynamic change: shuffle.partitions = {spark.conf.get('spark.sql.shuffle.partitions')}")

spark.stop()
