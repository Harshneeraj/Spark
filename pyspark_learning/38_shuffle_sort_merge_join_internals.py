"""
Topic: Sort-Merge Join Internals - How Spark Joins Work
=========================================================

Understanding the default join mechanism in Spark.

Spark UI Behavior:
- Sort-Merge Join: 1 job -> 3 stages
  Stage 0: Read left table + shuffle (hash partition by join key)
  Stage 1: Read right table + shuffle (hash partition by join key)
  Stage 2: Sort both sides within partition + merge (zip through sorted data)
- In DAG: Exchange (shuffle) -> Sort -> SortMergeJoin
- Shuffle write/read visible in stage metrics.

Key Interview Points:
- Sort-Merge Join is the DEFAULT for large-large joins.
- Steps: Shuffle both sides by join key -> Sort within partition -> Merge
- Requires both sides to be sorted by join key (sort happens after shuffle).
- Works well for large tables (no memory constraint like hash join).
- Alternative: Shuffle Hash Join (builds hash table on smaller side).
- Broadcast Hash Join avoids shuffle entirely (for small tables).
- Join strategy selection is automatic but can be hinted.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

spark = SparkSession.builder \
    .appName("38_Sort_Merge_Join_Internals") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .config("spark.sql.join.preferSortMergeJoin", "true") \
    .getOrCreate()

# Two tables for joining
orders = [(i, i % 10, i * 100) for i in range(1, 51)]
customers = [(i, f"Customer_{i}", f"City_{i}") for i in range(1, 11)]

df_orders = spark.createDataFrame(orders, ["order_id", "customer_id", "amount"])
df_customers = spark.createDataFrame(customers, ["customer_id", "name", "city"])

# ============ SORT-MERGE JOIN (Default) ============
"""
How Sort-Merge Join works:

1. SHUFFLE PHASE:
   - Both tables are hash-partitioned by the join key (customer_id)
   - Records with same customer_id go to same partition on both sides
   - This is the "Exchange" in the plan

2. SORT PHASE:
   - Within each partition, both sides are sorted by join key
   - This enables efficient merge in next step

3. MERGE PHASE:
   - Two sorted lists are merged (like merge step in merge sort)
   - Walk through both sorted partitions simultaneously
   - When keys match -> output joined row
   - O(N + M) per partition (very efficient for sorted data)

Visualization for one partition:
   Left (sorted):  [1, 1, 3, 5, 7]
   Right (sorted): [1, 2, 3, 3, 5]
   
   Pointer L=0, R=0: L[0]=1, R[0]=1 -> MATCH! Output (1,1)
   Pointer L=1, R=0: L[1]=1, R[0]=1 -> MATCH! Output (1,1)
   Pointer L=2, R=1: L[2]=3, R[1]=2 -> L>R, advance R
   Pointer L=2, R=2: L[2]=3, R[2]=3 -> MATCH! Output (3,3)
   ... and so on
"""

print("=== Sort-Merge Join Plan ===")
df_smj = df_orders.join(df_customers, "customer_id", "inner")
df_smj.explain(mode="formatted")
# Look for: SortMergeJoin, Exchange (hashpartitioning), Sort
df_smj.show(10)

# ============ SHUFFLE HASH JOIN ============
"""
How Shuffle Hash Join works:

1. SHUFFLE PHASE: Same as SMJ - both sides shuffled by join key
2. BUILD PHASE: Build hash table from SMALLER side (in memory)
3. PROBE PHASE: Probe hash table with each row from larger side

Faster than SMJ when:
- One side is much smaller (fits in memory as hash table)
- No sorting needed

Slower than SMJ when:
- Both sides are large (hash table doesn't fit in memory -> spill)
- Data is already sorted (SMJ can skip sort)

Force with hint:
"""

print("\n=== Shuffle Hash Join Plan ===")
df_shj = df_orders.join(df_customers.hint("shuffle_hash"), "customer_id", "inner")
df_shj.explain(mode="formatted")
# Look for: ShuffledHashJoin instead of SortMergeJoin

# ============ BROADCAST HASH JOIN ============
"""
How Broadcast Hash Join works:

1. BROADCAST PHASE: Small table collected to driver, broadcast to all executors
2. BUILD PHASE: Each executor builds hash table from broadcast data
3. PROBE PHASE: Each partition of large table probes local hash table

No shuffle of large table! Only 2 stages instead of 3.
"""

print("\n=== Broadcast Hash Join Plan ===")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
df_bhj = df_orders.join(broadcast(df_customers), "customer_id", "inner")
df_bhj.explain(mode="formatted")
# Look for: BroadcastHashJoin, BroadcastExchange

# ============ JOIN STRATEGY COMPARISON ============
"""
┌─────────────────────┬──────────────┬──────────────┬───────────────────────┐
│ Strategy            │ Shuffle      │ Memory       │ When to use           │
├─────────────────────┼──────────────┼──────────────┼───────────────────────┤
│ Broadcast Hash Join │ None (large) │ Small table  │ One side < 10MB       │
│                     │              │ in memory    │ (or forced broadcast) │
├─────────────────────┼──────────────┼──────────────┼───────────────────────┤
│ Sort-Merge Join     │ Both sides   │ Low (stream) │ Both sides large      │
│                     │              │              │ (default strategy)    │
├─────────────────────┼──────────────┼──────────────┼───────────────────────┤
│ Shuffle Hash Join   │ Both sides   │ Small side   │ One side much smaller │
│                     │              │ as hash table│ but > broadcast limit │
├─────────────────────┼──────────────┼──────────────┼───────────────────────┤
│ Broadcast Nested    │ None         │ Small table  │ Non-equi joins        │
│ Loop Join           │              │ in memory    │ (theta joins)         │
└─────────────────────┴──────────────┴──────────────┴───────────────────────┘

Selection priority (Spark's automatic choice):
1. If one side < autoBroadcastJoinThreshold → Broadcast Hash Join
2. If hint provided → Use hinted strategy
3. If preferSortMergeJoin=true → Sort-Merge Join
4. If one side much smaller → Shuffle Hash Join
5. Default → Sort-Merge Join
"""

# ============ JOIN HINTS ============

print("\n=== Join Hints ===")

# Force broadcast
df_orders.join(df_customers.hint("broadcast"), "customer_id").explain()

# Force sort-merge
df_orders.join(df_customers.hint("merge"), "customer_id").explain()

# Force shuffle hash
df_orders.join(df_customers.hint("shuffle_hash"), "customer_id").explain()

# Force nested loop (for non-equi joins)
# df_orders.join(df_customers.hint("shuffle_replicate_nl"), condition).explain()

# ============ WHY SORT-MERGE IS DEFAULT ============
"""
Sort-Merge Join advantages:
1. Memory efficient: Streams through data, doesn't need to hold entire side in memory
2. Handles large-large joins: Both sides can be arbitrarily large
3. Spill-friendly: If memory is tight, sorted data can be spilled and merged from disk
4. Reusable sort: If data is already sorted (bucketed), sort step is skipped

Sort-Merge Join disadvantages:
1. Requires sorting (O(N log N) per partition)
2. Requires shuffle of BOTH sides
3. Slower than hash join for small-large joins
"""

# Write
df_smj.write.mode("overwrite").parquet("/shared/join_internals_demo")

spark.stop()
