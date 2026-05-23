"""
Topic: Window Functions
========================

Window functions perform calculations across a set of rows related to the current row,
without collapsing rows (unlike groupBy).

Spark UI Behavior:
- Window functions cause a SHUFFLE (wide transformation) to partition data by the
  partition column.
- show() on windowed df -> 1 job -> 2 stages:
  Stage 0: Read data
  Stage 1: Shuffle by window partition column + compute window function
- If multiple window functions use the SAME partitionBy, Spark optimizes to 1 shuffle.
- Different partitionBy columns = multiple shuffles = more stages.

Key Interview Points:
- Window functions don't reduce rows (unlike groupBy which collapses).
- partitionBy defines the group, orderBy defines the order within group.
- Types: Ranking (row_number, rank, dense_rank), Analytic (lead, lag),
  Aggregate (sum, avg over window).
- row_number() vs rank() vs dense_rank() is a VERY common interview question.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, row_number, rank, dense_rank, 
    lead, lag, sum, avg, count, max, min,
    ntile, percent_rank, cume_dist
)
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("09_Window_Functions") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

data = [
    (1, "Alice", "Engineering", 90000),
    (2, "Bob", "Engineering", 85000),
    (3, "Charlie", "Engineering", 85000),
    (4, "Diana", "Engineering", 70000),
    (5, "Eve", "Marketing", 65000),
    (6, "Frank", "Marketing", 60000),
    (7, "Grace", "Marketing", 60000),
    (8, "Henry", "HR", 55000),
    (9, "Ivy", "HR", 50000),
    (10, "Jack", "HR", 50000)
]

df = spark.createDataFrame(data, ["id", "name", "department", "salary"])

# ============ DEFINE WINDOWS ============

# Window partitioned by department, ordered by salary descending
window_dept = Window.partitionBy("department").orderBy(col("salary").desc())

# Window partitioned by department (no order - for running aggregates)
window_dept_unordered = Window.partitionBy("department")

# Window with rows between (for running totals)
window_running = Window.partitionBy("department").orderBy("salary") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# ============ RANKING FUNCTIONS ============
# CRITICAL INTERVIEW QUESTION: Difference between row_number, rank, dense_rank

print("=== row_number vs rank vs dense_rank ===")
df_ranked = df.withColumn("row_number", row_number().over(window_dept)) \
    .withColumn("rank", rank().over(window_dept)) \
    .withColumn("dense_rank", dense_rank().over(window_dept))

df_ranked.show()
"""
For Engineering (90000, 85000, 85000, 70000):
- row_number: 1, 2, 3, 4  (always unique, arbitrary for ties)
- rank:       1, 2, 2, 4  (same rank for ties, SKIPS next)
- dense_rank: 1, 2, 2, 3  (same rank for ties, NO skip)
"""

# ============ LEAD and LAG ============
# lead: next row's value, lag: previous row's value

print("=== Lead and Lag ===")
df_lead_lag = df.withColumn("next_salary", lead("salary", 1).over(window_dept)) \
    .withColumn("prev_salary", lag("salary", 1).over(window_dept)) \
    .withColumn("salary_diff_from_prev", col("salary") - lag("salary", 1).over(window_dept))

df_lead_lag.show()

# ============ AGGREGATE WINDOW FUNCTIONS ============

print("=== Aggregate Windows (without collapsing rows) ===")
df_agg_window = df \
    .withColumn("dept_avg_salary", avg("salary").over(window_dept_unordered)) \
    .withColumn("dept_max_salary", max("salary").over(window_dept_unordered)) \
    .withColumn("dept_total_salary", sum("salary").over(window_dept_unordered)) \
    .withColumn("dept_count", count("*").over(window_dept_unordered))

df_agg_window.show()

# ============ RUNNING TOTAL ============

print("=== Running Total (cumulative sum) ===")
df_running = df.withColumn("running_total", sum("salary").over(window_running))
df_running.show()

# ============ NTILE ============
# Divides rows into N roughly equal groups

print("=== NTILE (divide into quartiles) ===")
window_overall = Window.orderBy(col("salary").desc())
df_ntile = df.withColumn("quartile", ntile(4).over(window_overall))
df_ntile.show()

# ============ PERCENT_RANK and CUME_DIST ============

print("=== Percent Rank and Cumulative Distribution ===")
df_pct = df.withColumn("percent_rank", percent_rank().over(window_dept)) \
    .withColumn("cume_dist", cume_dist().over(window_dept))
df_pct.show()

# ============ PRACTICAL USE CASE: Top N per Group ============
# Very common interview question!

print("=== Top 2 earners per department ===")
df_top2 = df.withColumn("rn", row_number().over(window_dept)) \
    .filter(col("rn") <= 2) \
    .drop("rn")
df_top2.show()

# Write
df_ranked.write.mode("overwrite").parquet("/shared/employees_ranked")

spark.stop()
