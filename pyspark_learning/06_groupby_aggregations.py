"""
Topic: groupBy() and Aggregation Functions
============================================

GroupBy is a WIDE transformation that causes a shuffle.

Spark UI Behavior:
- groupBy().agg() triggers a SHUFFLE (wide transformation).
- show() on grouped df -> 1 job -> 2 stages:
  Stage 0: Read data + partial aggregation (map-side combine)
  Stage 1: Shuffle + final aggregation
- The shuffle exchange is visible in the DAG visualization.
- Number of tasks in Stage 1 = spark.sql.shuffle.partitions (default 200)

Key Interview Points:
- groupBy causes a full shuffle of data across the cluster.
- Spark does map-side partial aggregation (like a combiner in MapReduce).
- spark.sql.shuffle.partitions controls output partitions after shuffle (default=200).
- For small datasets, reduce shuffle partitions to avoid overhead.
- agg() allows multiple aggregations in one pass.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum, avg, min, max, 
    countDistinct, collect_list, collect_set,
    first, last, stddev, variance, approx_count_distinct
)

spark = SparkSession.builder \
    .appName("06_GroupBy_Aggregations") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

data = [
    (1, "Alice", "Engineering", "Backend", 90000),
    (2, "Bob", "Marketing", "Digital", 45000),
    (3, "Charlie", "Engineering", "Frontend", 65000),
    (4, "Diana", "HR", "Recruitment", 55000),
    (5, "Eve", "Marketing", "Digital", 55000),
    (6, "Frank", "Engineering", "Backend", 80000),
    (7, "Grace", "HR", "Payroll", 60000),
    (8, "Henry", "Engineering", "Frontend", 70000),
    (9, "Ivy", "Marketing", "Content", 50000),
    (10, "Jack", "Engineering", "Backend", 95000)
]

df = spark.createDataFrame(data, ["id", "name", "department", "team", "salary"])

# ============ Basic groupBy ============

# Single aggregation
# Spark UI: 1 job -> 2 stages (read + partial agg | shuffle + final agg)
print("=== Count by Department ===")
df.groupBy("department").count().show()

# ============ Multiple Aggregations with agg() ============

# Multiple agg functions in one pass (efficient - single shuffle)
print("=== Department Statistics ===")
df.groupBy("department").agg(
    count("*").alias("employee_count"),
    sum("salary").alias("total_salary"),
    avg("salary").alias("avg_salary"),
    min("salary").alias("min_salary"),
    max("salary").alias("max_salary"),
    stddev("salary").alias("salary_stddev")
).show()

# ============ Multiple GroupBy Columns ============

print("=== Group by Department and Team ===")
df.groupBy("department", "team").agg(
    count("*").alias("count"),
    avg("salary").alias("avg_salary")
).show()

# ============ Special Aggregation Functions ============

# countDistinct - count unique values
print("=== Distinct Teams per Department ===")
df.groupBy("department").agg(
    countDistinct("team").alias("distinct_teams")
).show()

# collect_list - gather all values into a list (preserves duplicates)
print("=== Names List per Department ===")
df.groupBy("department").agg(
    collect_list("name").alias("employees")
).show(truncate=False)

# collect_set - gather unique values into a set
print("=== Unique Teams per Department ===")
df.groupBy("department").agg(
    collect_set("team").alias("teams")
).show(truncate=False)

# first and last
print("=== First and Last employee per Department ===")
df.groupBy("department").agg(
    first("name").alias("first_employee"),
    last("name").alias("last_employee")
).show()

# approx_count_distinct - faster than countDistinct for large data
print("=== Approximate Distinct Count ===")
df.groupBy("department").agg(
    approx_count_distinct("team").alias("approx_distinct_teams")
).show()

# ============ Aggregation without groupBy ============

# Aggregate entire DataFrame
print("=== Overall Statistics ===")
df.agg(
    count("*").alias("total_employees"),
    avg("salary").alias("overall_avg_salary"),
    sum("salary").alias("total_payroll")
).show()

# Write grouped result
df.groupBy("department").agg(
    count("*").alias("count"),
    avg("salary").alias("avg_salary")
).write.mode("overwrite").parquet("/shared/department_stats")

spark.stop()
