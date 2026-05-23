"""
Topic: Adaptive Query Execution (AQE) - Spark 3.x Optimization
================================================================

AQE optimizes queries at RUNTIME based on actual data statistics,
unlike the Catalyst optimizer which plans at compile time.

Spark UI Behavior:
- With AQE enabled, you'll see "AdaptiveSparkPlan" in the physical plan.
- Spark UI shows "CustomShuffleReader" when AQE coalesces partitions.
- The number of post-shuffle partitions may differ from spark.sql.shuffle.partitions.
- Stages may be re-optimized mid-execution.

Key Interview Points:
- AQE was introduced in Spark 3.0, enabled by default in Spark 3.2+.
- Three main optimizations:
  1. Coalescing post-shuffle partitions (reduces small partitions)
  2. Converting sort-merge join to broadcast join at runtime
  3. Optimizing skew joins (splits skewed partitions)
- AQE uses runtime statistics from completed stages to optimize remaining stages.
- It can change the physical plan DURING execution (not just at planning time).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, broadcast
import random

spark = SparkSession.builder \
    .appName("15_AQE_Adaptive_Query_Execution") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
    .getOrCreate()

# ============ AQE FEATURE 1: Coalescing Post-Shuffle Partitions ============
"""
Problem: spark.sql.shuffle.partitions = 200 (default)
For small data, 200 partitions means many empty/tiny partitions = overhead.

Without AQE: You get 200 partitions regardless of data size.
With AQE: Spark detects small partitions and merges them at runtime.

Relevant configs:
- spark.sql.adaptive.coalescePartitions.enabled = true
- spark.sql.adaptive.coalescePartitions.minPartitionSize = 1MB
- spark.sql.adaptive.advisoryPartitionSizeInBytes = 64MB
"""

data = [(i, f"name_{i}", i % 5, random.randint(30000, 90000)) for i in range(1, 51)]
df = spark.createDataFrame(data, ["id", "name", "dept_id", "salary"])

print("=== Coalescing Partitions Demo ===")
print(f"Configured shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

# GroupBy creates 200 shuffle partitions, but AQE will coalesce them
df_grouped = df.groupBy("dept_id").agg(count("*").alias("cnt"), sum("salary").alias("total"))

# Check the plan - should show AdaptiveSparkPlan
df_grouped.explain(mode="formatted")
df_grouped.show()

# After execution, actual partitions used will be << 200
print(f"Actual partitions after AQE coalescing: {df_grouped.rdd.getNumPartitions()}")

# ============ AQE FEATURE 2: Dynamic Join Strategy Switch ============
"""
Problem: At planning time, Spark may not know the actual size of a table
(e.g., after filtering, the table might be small enough to broadcast).

Without AQE: Uses sort-merge join (expensive shuffle on both sides).
With AQE: After computing one side, if it's small enough, switches to broadcast.

Relevant configs:
- spark.sql.adaptive.autoBroadcastJoinThreshold (same as non-adaptive threshold)
"""

# Large table
orders = [(i, i % 10, random.randint(10, 1000)) for i in range(1, 101)]
df_orders = spark.createDataFrame(orders, ["order_id", "store_id", "amount"])

# Table that becomes small after filtering
stores = [(i, f"Store_{i}", f"City_{i}") for i in range(1, 11)]
df_stores = spark.createDataFrame(stores, ["store_id", "store_name", "city"])

# After filter, df_stores_filtered is very small -> AQE may broadcast it
df_stores_filtered = df_stores.filter(col("store_id") <= 3)

print("\n=== Dynamic Join Strategy ===")
print("Join with filtered small table - AQE may convert to broadcast:")
df_joined = df_orders.join(df_stores_filtered, "store_id", "inner")
df_joined.explain(mode="formatted")
df_joined.show(5)

# ============ AQE FEATURE 3: Skew Join Optimization ============
"""
Problem: One partition has much more data than others (skew).

Without AQE: One task processes the huge partition (straggler).
With AQE: Detects skewed partition and splits it into smaller sub-partitions.

Relevant configs:
- spark.sql.adaptive.skewJoin.enabled = true
- spark.sql.adaptive.skewJoin.skewedPartitionFactor = 5
  (partition is skewed if size > factor * median partition size)
- spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 256MB
  (minimum size to be considered skewed)
"""

# Create skewed data
skewed_orders = []
for i in range(1, 201):
    if i <= 160:
        store_id = 1  # 80% of data -> SKEWED
    else:
        store_id = random.randint(2, 10)
    skewed_orders.append((i, store_id, random.randint(10, 1000)))

df_skewed = spark.createDataFrame(skewed_orders, ["order_id", "store_id", "amount"])

print("\n=== Skew Join with AQE ===")
print("Data distribution:")
df_skewed.groupBy("store_id").count().orderBy(col("count").desc()).show()

# AQE will detect skew in store_id=1 partition and split it
df_skew_joined = df_skewed.join(df_stores, "store_id", "inner")
df_skew_joined.explain(mode="formatted")
df_skew_joined.show(5)

# ============ AQE CONFIGURATION SUMMARY ============

print("\n=== All AQE Configurations ===")
aqe_configs = [
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.coalescePartitions.minPartitionSize",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes",
    "spark.sql.adaptive.skewJoin.enabled",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes",
    "spark.sql.adaptive.localShuffleReader.enabled",
]

for config in aqe_configs:
    try:
        value = spark.conf.get(config)
        print(f"  {config} = {value}")
    except Exception:
        print(f"  {config} = (not set)")

# ============ COMPARING WITH AND WITHOUT AQE ============
"""
To compare performance:
1. Run with spark.sql.adaptive.enabled = false
2. Run with spark.sql.adaptive.enabled = true
3. Compare:
   - Number of stages
   - Task duration distribution
   - Shuffle data size
   - Overall job duration

In production:
- AQE is almost always beneficial -> keep it enabled.
- It adds minimal overhead (collecting statistics between stages).
- Most impactful for: skewed joins, unknown data sizes, varying workloads.
"""

# Write
df_skew_joined.write.mode("overwrite").parquet("/shared/aqe_demo_output")

spark.stop()
