"""
Topic: Broadcast Join (Map-Side Join) - IMPORTANT INTERVIEW TOPIC
==================================================================

Broadcast join sends the smaller DataFrame to ALL executors, avoiding shuffle
of the larger DataFrame.

Spark UI Behavior:
- Regular Sort-Merge Join: 3 stages (shuffle left + shuffle right + join)
- Broadcast Join: 2 stages only (broadcast small df + map-side join)
  Stage 0: Read large DataFrame
  Stage 1: Join (small df is broadcast, no shuffle of large df)
- In DAG, you'll see "BroadcastExchange" instead of "ShuffleExchange"
- Broadcast is visible in the "Broadcast" section of Spark UI

Key Interview Points:
- spark.sql.autoBroadcastJoinThreshold = 10MB (default). Tables smaller than this
  are automatically broadcast.
- Use broadcast() hint to force broadcast regardless of size.
- Broadcast join eliminates shuffle of the large table -> HUGE performance gain.
- Broadcast join CANNOT be used for: full outer join, right outer join (on broadcast side).
- The broadcast table must fit in driver + executor memory.
- If broadcast table is too large, you get OOM errors.
- Also called "Map-Side Join" or "Replicated Join".
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

spark = SparkSession.builder \
    .appName("08_Broadcast_Join") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.autoBroadcastJoinThreshold", "10485760") \
    .getOrCreate()

# Large table (imagine millions of rows in production)
transactions = [
    (1, "TXN001", 101, 500.0),
    (2, "TXN002", 102, 1200.0),
    (3, "TXN003", 101, 300.0),
    (4, "TXN004", 103, 800.0),
    (5, "TXN005", 102, 150.0),
    (6, "TXN006", 101, 2000.0),
    (7, "TXN007", 103, 450.0),
    (8, "TXN008", 104, 900.0),
    (9, "TXN009", 101, 1100.0),
    (10, "TXN010", 102, 600.0)
]

# Small lookup/dimension table (perfect for broadcast)
stores = [
    (101, "Store NYC", "New York"),
    (102, "Store LA", "Los Angeles"),
    (103, "Store CHI", "Chicago"),
    (104, "Store SF", "San Francisco")
]

df_transactions = spark.createDataFrame(transactions, ["id", "txn_id", "store_id", "amount"])
df_stores = spark.createDataFrame(stores, ["store_id", "store_name", "city"])

# ============ REGULAR JOIN (Sort-Merge) ============
# Spark UI: 3 stages - both sides shuffled
print("=== Regular Join (Sort-Merge) ===")
df_regular = df_transactions.join(df_stores, "store_id", "inner")
df_regular.explain()  # Shows SortMergeJoin or ShuffleHashJoin
print()
df_regular.show()

# ============ BROADCAST JOIN (Explicit Hint) ============
# Spark UI: 2 stages - NO shuffle on transactions side
print("=== Broadcast Join ===")
df_broadcast = df_transactions.join(broadcast(df_stores), "store_id", "inner")
df_broadcast.explain()  # Shows BroadcastHashJoin
print()
df_broadcast.show()

# ============ COMPARE EXECUTION PLANS ============
print("=== Execution Plan: Regular Join ===")
df_regular.explain(mode="formatted")

print("\n=== Execution Plan: Broadcast Join ===")
df_broadcast.explain(mode="formatted")

# ============ DISABLE AUTO BROADCAST ============
# Sometimes you want to force sort-merge join for testing
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

print("=== Join with auto-broadcast DISABLED ===")
df_no_auto = df_transactions.join(df_stores, "store_id", "inner")
df_no_auto.explain()  # Will show SortMergeJoin even for small table
print()

# Re-enable and force broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")

# ============ BROADCAST WITH LEFT JOIN ============
# Broadcast the RIGHT (small) side in a left join
print("=== Broadcast Left Join ===")
df_transactions.join(broadcast(df_stores), "store_id", "left").show()

# ============ WHEN NOT TO USE BROADCAST ============
"""
DON'T use broadcast when:
1. The "small" table is actually large (> 1-2 GB) -> OOM
2. The small table will grow over time (design issue)
3. Full outer join is needed
4. Both tables are large (broadcast won't help)

DO use broadcast when:
1. Dimension/lookup tables (countries, states, categories)
2. Configuration tables
3. Any table that fits comfortably in memory
"""

# Write result
df_broadcast.write.mode("overwrite").parquet("/shared/transactions_with_store")

spark.stop()
