"""
Topic: Explain Plan and Catalyst Optimizer
============================================

Understanding execution plans is crucial for debugging and optimization.

Spark UI Behavior:
- explain() does NOT trigger a job (just shows the plan).
- The DAG in Spark UI corresponds to the physical plan.
- Each stage boundary in the DAG = a shuffle (Exchange node in plan).
- Spark UI -> SQL tab shows the query plan graphically.

Key Interview Points:
- Catalyst Optimizer phases: Analysis -> Logical Optimization -> Physical Planning -> Code Generation
- explain() modes: simple, extended, codegen, cost, formatted
- Key things to look for in plans:
  - Exchange (shuffle) - expensive, minimize these
  - BroadcastExchange - good (broadcast join)
  - Filter pushdown - filters pushed to scan level
  - Column pruning - only needed columns read
  - Predicate pushdown - filters pushed to data source
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, broadcast

spark = SparkSession.builder \
    .appName("17_Explain_Plan_Catalyst") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

employees = [
    (1, "Alice", "Engineering", 90000),
    (2, "Bob", "Marketing", 45000),
    (3, "Charlie", "Engineering", 65000),
    (4, "Diana", "HR", 55000),
    (5, "Eve", "Marketing", 70000)
]

departments = [
    ("Engineering", "Building A", 100),
    ("Marketing", "Building B", 50),
    ("HR", "Building C", 30)
]

df_emp = spark.createDataFrame(employees, ["id", "name", "department", "salary"])
df_dept = spark.createDataFrame(departments, ["dept_name", "location", "budget"])

# ============ EXPLAIN MODES ============

# Simple explain (physical plan only)
print("=" * 60)
print("=== SIMPLE EXPLAIN ===")
print("=" * 60)
df_emp.filter(col("salary") > 50000).explain()

# Extended explain (logical + physical plans)
print("\n" + "=" * 60)
print("=== EXTENDED EXPLAIN (all plans) ===")
print("=" * 60)
df_emp.filter(col("salary") > 50000) \
    .groupBy("department").avg("salary") \
    .explain(mode="extended")

# Formatted explain (most readable)
print("\n" + "=" * 60)
print("=== FORMATTED EXPLAIN ===")
print("=" * 60)
df_emp.join(df_dept, df_emp["department"] == df_dept["dept_name"], "inner") \
    .filter(col("salary") > 50000) \
    .groupBy("department").agg(avg("salary").alias("avg_sal")) \
    .explain(mode="formatted")

# ============ UNDERSTANDING PLAN NODES ============
"""
Common Physical Plan Nodes:

1. Scan (FileScan, InMemoryTableScan)
   - Reading data from source
   - Look for: pushed filters, partition pruning

2. Filter
   - Row filtering
   - Best when pushed down to scan level

3. Project
   - Column selection/computation
   - Column pruning happens here

4. Exchange (SHUFFLE!)
   - hashpartitioning: shuffle by hash of columns
   - rangepartitioning: shuffle for sort
   - SinglePartition: collect to one partition
   - THIS IS EXPENSIVE - minimize these!

5. HashAggregate / SortAggregate
   - Aggregation operations
   - Usually appears in pairs (partial + final)

6. SortMergeJoin
   - Default join strategy
   - Requires both sides sorted by join key
   - Preceded by Exchange (shuffle) on both sides

7. BroadcastHashJoin
   - Small table broadcast to all executors
   - No shuffle on large side
   - Look for BroadcastExchange

8. Sort
   - Sorting operation
   - May require Exchange for global sort
"""

# ============ CATALYST OPTIMIZATIONS IN ACTION ============

# Optimization 1: Predicate Pushdown
# Filter is pushed BEFORE the join (less data to shuffle)
print("\n=== Predicate Pushdown ===")
df_emp.join(df_dept, df_emp["department"] == df_dept["dept_name"]) \
    .filter(col("salary") > 50000) \
    .explain()
# Notice: Filter appears BEFORE the join in the plan!

# Optimization 2: Column Pruning
# Only needed columns are read from source
print("\n=== Column Pruning ===")
df_emp.select("name", "salary").explain()
# Only 'name' and 'salary' are scanned

# Optimization 3: Constant Folding
# Spark evaluates constant expressions at compile time
print("\n=== Constant Folding ===")
df_emp.filter(col("salary") > 1000 * 50).explain()
# 1000 * 50 is computed once, not per row

# Optimization 4: Broadcast Join Detection
print("\n=== Auto Broadcast Detection ===")
df_emp.join(df_dept, df_emp["department"] == df_dept["dept_name"]).explain()
# Small table may be auto-broadcast

# ============ COMPARING PLANS ============

# Inefficient: filter after join
print("\n=== INEFFICIENT: Filter after join ===")
plan1 = df_emp.join(df_dept, df_emp["department"] == df_dept["dept_name"]) \
    .filter(col("salary") > 80000)
plan1.explain()

# Efficient: filter before join (Catalyst does this automatically!)
print("\n=== EFFICIENT: Filter before join (same plan due to Catalyst!) ===")
plan2 = df_emp.filter(col("salary") > 80000) \
    .join(df_dept, df_emp["department"] == df_dept["dept_name"])
plan2.explain()
# Both produce the SAME physical plan! Catalyst optimizes automatically.

# ============ READING THE DAG IN SPARK UI ============
"""
How to read Spark UI DAG:
1. Go to Spark UI -> Jobs -> Click on a job
2. Click on a stage to see its DAG
3. Read BOTTOM to TOP (data flows upward)
4. Each box = an operation
5. Dotted lines between stages = shuffle boundary

What to look for:
- Many Exchange nodes = too many shuffles (optimize!)
- Skewed task durations = data skew
- Large "Shuffle Write" = lots of data being shuffled
- "Spill (Memory)" or "Spill (Disk)" = not enough memory per task
"""

# Trigger an action to see the plan in Spark UI
df_emp.join(df_dept, df_emp["department"] == df_dept["dept_name"]) \
    .groupBy("location").agg(avg("salary").alias("avg_salary")) \
    .write.mode("overwrite").parquet("/shared/explain_plan_demo")

spark.stop()
