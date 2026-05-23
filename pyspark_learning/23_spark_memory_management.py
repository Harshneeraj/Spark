"""
Topic: Spark Memory Management and Configuration
==================================================

Understanding how Spark uses memory is critical for tuning.

Spark UI Behavior:
- Spark UI -> Executors tab: Shows memory usage per executor.
- Storage Memory: Used for cached DataFrames (visible in Storage tab).
- Execution Memory: Used for shuffles, joins, sorts, aggregations.
- If you see "Spill (Memory)" or "Spill (Disk)" in stages -> memory pressure.
- GC time in Executors tab: High GC = memory pressure.

Key Interview Points:
- Unified Memory Management (Spark 1.6+): Execution and Storage share memory.
- Execution can evict Storage (but not vice versa beyond a threshold).
- spark.memory.fraction = 0.6 (60% of JVM heap for Spark)
- spark.memory.storageFraction = 0.5 (50% of Spark memory reserved for storage)
- Remaining 40% = User memory (UDFs, data structures) + Reserved (300MB)
- OOM errors: Usually caused by too much data per task or skew.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("23_Memory_Management") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.memory.fraction", "0.6") \
    .config("spark.memory.storageFraction", "0.5") \
    .config("spark.memory.offHeap.enabled", "false") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# ============ MEMORY LAYOUT ============
"""
Total JVM Heap (e.g., 4GB with spark.executor.memory=4g)
├── Reserved Memory: 300MB (fixed, for Spark internals)
├── User Memory: (1 - spark.memory.fraction) * (Heap - 300MB)
│   └── Used for: UDF data structures, RDD metadata, internal objects
└── Spark Memory: spark.memory.fraction * (Heap - 300MB)
    ├── Storage Memory: spark.memory.storageFraction * Spark Memory
    │   └── Used for: cache(), persist(), broadcast variables
    └── Execution Memory: (1 - spark.memory.storageFraction) * Spark Memory
        └── Used for: Shuffles, joins, sorts, aggregations, hash maps

Example with 4GB heap:
- Reserved: 300MB
- Usable: 4GB - 300MB = 3.7GB
- Spark Memory: 0.6 * 3.7GB = 2.22GB
  - Storage: 0.5 * 2.22GB = 1.11GB
  - Execution: 0.5 * 2.22GB = 1.11GB
- User Memory: 0.4 * 3.7GB = 1.48GB

UNIFIED MEMORY:
- Execution can borrow from Storage (evicts cached data)
- Storage can borrow from Execution (only if Execution is not using it)
- Storage cannot evict Execution memory (Execution has priority)
"""

# ============ KEY MEMORY CONFIGURATIONS ============

print("=== Memory Configuration ===")
configs = [
    ("spark.driver.memory", "Driver JVM heap size"),
    ("spark.executor.memory", "Executor JVM heap size"),
    ("spark.memory.fraction", "Fraction of heap for Spark (execution + storage)"),
    ("spark.memory.storageFraction", "Fraction of Spark memory for storage"),
    ("spark.memory.offHeap.enabled", "Off-heap memory enabled"),
    ("spark.memory.offHeap.size", "Off-heap memory size"),
    ("spark.executor.memoryOverhead", "Off-heap memory for JVM overhead (containers)"),
]

for config, description in configs:
    try:
        value = spark.conf.get(config)
    except Exception:
        value = "(not set - using default)"
    print(f"  {config} = {value}")
    print(f"    -> {description}")
    print()

# ============ COMMON OOM SCENARIOS AND FIXES ============
"""
Scenario 1: OOM during shuffle (sort/join/groupBy)
- Cause: Too much data per partition
- Fix: Increase spark.sql.shuffle.partitions (more partitions = less data per task)
- Fix: Increase executor memory
- Fix: Fix data skew (one partition has too much data)

Scenario 2: OOM during collect()
- Cause: Bringing too much data to driver
- Fix: Don't use collect() on large DataFrames!
- Fix: Use take(N) or show(N) instead
- Fix: Increase spark.driver.memory if collect is necessary

Scenario 3: OOM during broadcast join
- Cause: Broadcast table too large for executor memory
- Fix: Reduce autoBroadcastJoinThreshold
- Fix: Use sort-merge join instead
- Fix: Increase executor memory

Scenario 4: OOM during cache/persist
- Cause: Cached data exceeds storage memory
- Fix: Use MEMORY_AND_DISK (spill to disk)
- Fix: Cache fewer DataFrames
- Fix: Use MEMORY_ONLY_SER (serialized, less memory)

Scenario 5: High GC (Garbage Collection) time
- Cause: Too many objects in JVM heap
- Fix: Use serialized storage (MEMORY_ONLY_SER)
- Fix: Increase executor memory
- Fix: Reduce data per task (more partitions)
- Fix: Use off-heap memory
"""

# ============ MONITORING MEMORY IN CODE ============

# Check storage memory usage
print("\n=== Storage Memory Info ===")
data = [(i, f"name_{i}", i * 1000) for i in range(1, 101)]
df = spark.createDataFrame(data, ["id", "name", "salary"])

# Cache and trigger materialization
df.cache()
df.count()  # Triggers caching

# Check catalog for cached tables
print("Cached DataFrames:")
print(f"  Is cached: {df.is_cached}")

# Unpersist to free memory
df.unpersist()

# ============ MEMORY TUNING GUIDELINES ============
"""
For a cluster with 16GB per executor:

Conservative (safe):
  spark.executor.memory = 12g  (leave room for OS/overhead)
  spark.memory.fraction = 0.6
  spark.memory.storageFraction = 0.5
  spark.executor.memoryOverhead = 4g  (for containers)

Aggressive (more for Spark):
  spark.executor.memory = 14g
  spark.memory.fraction = 0.75
  spark.memory.storageFraction = 0.3  (less storage, more execution)
  spark.executor.memoryOverhead = 2g

For heavy caching workloads:
  spark.memory.storageFraction = 0.6  (more storage)

For heavy shuffle workloads:
  spark.memory.storageFraction = 0.3  (more execution)
  Increase shuffle partitions to reduce per-task memory
"""

# ============ OFF-HEAP MEMORY ============
"""
Off-heap memory (outside JVM heap):
- Not subject to GC pauses
- Managed by Spark directly (Tungsten)
- Useful for very large datasets

Enable with:
  spark.memory.offHeap.enabled = true
  spark.memory.offHeap.size = 2g

Benefits:
- No GC overhead
- More predictable performance
- Can use more total memory

Drawbacks:
- Harder to debug
- Must be explicitly sized
"""

# Write demo
df_demo = spark.createDataFrame(
    [(i, f"name_{i}", i * 1000) for i in range(1, 21)],
    ["id", "name", "salary"]
)
df_demo.write.mode("overwrite").parquet("/shared/memory_demo")

spark.stop()
