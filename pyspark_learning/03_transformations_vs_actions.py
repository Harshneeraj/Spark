"""
Topic: Transformations vs Actions (Lazy Evaluation)
=====================================================

Transformations are lazy - they build a logical plan but don't execute.
Actions trigger the execution of the plan.

Spark UI Behavior:
- Transformations (select, filter, withColumn, etc.) -> NO jobs in Spark UI
- Actions (show, count, collect, write, etc.) -> Trigger jobs
- Each action = at least 1 job
- This script will show ~3 jobs total in Spark UI

Key Interview Points:
- Narrow transformations: each input partition contributes to at most one output partition
  (map, filter, select, withColumn) -> No shuffle -> Same stage
- Wide transformations: input partitions contribute to multiple output partitions
  (groupBy, join, repartition) -> Shuffle -> New stage boundary
- Catalyst optimizer optimizes the logical plan before execution.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, when, lit

spark = SparkSession.builder \
    .appName("03_Transformations_vs_Actions") \
    .master("local[*]") \
    .getOrCreate()

data = [
    (1, "Alice", "Engineering", 50000),
    (2, "Bob", "Marketing", 45000),
    (3, "Charlie", "Engineering", 60000),
    (4, "Diana", "HR", 55000),
    (5, "Eve", "Marketing", 70000)
]

df = spark.createDataFrame(data, ["id", "name", "department", "salary"])

# ============ NARROW TRANSFORMATIONS (No shuffle, no job) ============

# select - picks columns (NARROW)
df_selected = df.select("name", "salary")

# filter/where - filters rows (NARROW)
df_filtered = df.filter(col("salary") > 50000)

# withColumn - adds/modifies column (NARROW)
df_with_bonus = df.withColumn("bonus", col("salary") * 0.1)

# when/otherwise - conditional logic (NARROW)
df_category = df.withColumn(
    "salary_band",
    when(col("salary") > 55000, "HIGH")
    .when(col("salary") > 45000, "MEDIUM")
    .otherwise("LOW")
)

# None of the above triggered any job! Check Spark UI - still 0 jobs.

# ============ WIDE TRANSFORMATIONS (Cause shuffle, new stage) ============

# groupBy + agg (WIDE - causes shuffle)
df_grouped = df.groupBy("department").sum("salary")

# Still no job! Wide transformations are also lazy.

# ============ ACTIONS (Trigger execution) ============

# Action 1: show() -> Job 0
# Spark UI: Job 0 -> 1 stage (narrow transforms only, no shuffle needed for show with limit)
print("=== Filtered DataFrame (salary > 50000) ===")
df_filtered.show()

# Action 2: show() on grouped -> Job 1
# Spark UI: Job 1 -> 2 stages
#   Stage 0: Read data + partial aggregation (map side)
#   Stage 1: Shuffle + final aggregation (reduce side)
print("=== Grouped by Department ===")
df_grouped.show()

# Action 3: collect() -> Job 2
# Spark UI: Job 2 -> 1 stage
# collect() brings ALL data to driver - DANGEROUS for large datasets!
result = df_category.collect()
print(f"\nCollected {len(result)} rows to driver")

# Action 4: write -> Job 3
# Spark UI: Job 3 -> 1 stage (no shuffle, just write)
df_with_bonus.write.mode("overwrite").parquet("/shared/employees_with_bonus")

print("\n=== Summary of Spark UI ===")
print("Total Jobs: ~4")
print("Job 0: show() on filtered df -> 1 stage")
print("Job 1: show() on grouped df -> 2 stages (shuffle boundary)")
print("Job 2: collect() on category df -> 1 stage")
print("Job 3: write parquet -> 1 stage")

spark.stop()
