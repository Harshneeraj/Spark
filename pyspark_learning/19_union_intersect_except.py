"""
Topic: union(), unionByName(), intersect(), except/subtract()
==============================================================

Set operations on DataFrames.

Spark UI Behavior:
- union() / unionByName(): NARROW transformation -> NO shuffle, NO job
  Simply concatenates partitions from both DataFrames.
  
- intersect(): WIDE transformation -> causes SHUFFLE
  Job -> 2+ stages (shuffle both sides + hash to find common rows)
  
- exceptAll() / subtract(): WIDE transformation -> causes SHUFFLE
  Job -> 2+ stages (shuffle both sides + find differences)

Key Interview Points:
- union() matches by POSITION (column order matters!).
- unionByName() matches by COLUMN NAME (safer, order doesn't matter).
- union() does NOT remove duplicates (use union + distinct for that).
- intersect() removes duplicates by default.
- exceptAll() keeps duplicates, except() removes them.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("19_Union_Intersect_Except") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Two DataFrames with same schema
df1 = spark.createDataFrame([
    (1, "Alice", 90000),
    (2, "Bob", 45000),
    (3, "Charlie", 65000)
], ["id", "name", "salary"])

df2 = spark.createDataFrame([
    (3, "Charlie", 65000),  # Duplicate with df1
    (4, "Diana", 55000),
    (5, "Eve", 70000)
], ["id", "name", "salary"])

# Different column order
df3 = spark.createDataFrame([
    ("Frank", 6, 80000),
    ("Grace", 7, 60000)
], ["name", "id", "salary"])  # Note: name and id are swapped!

# ============ UNION (by position) ============
# Spark UI: NO job triggered (lazy, narrow)
print("=== union() - matches by POSITION ===")
df_union = df1.union(df2)
df_union.show()
# Charlie appears TWICE (union doesn't deduplicate)

# DANGER: union with different column order
print("=== union() with different column order (WRONG!) ===")
df_wrong = df1.union(df3)
df_wrong.show()
# id and name are SWAPPED for Frank and Grace!

# ============ UNION BY NAME (safer) ============
print("=== unionByName() - matches by COLUMN NAME ===")
df_safe = df1.unionByName(df3)
df_safe.show()
# Correctly matches columns regardless of order

# ============ UNION + DISTINCT (deduplicate) ============
# Spark UI: distinct() causes shuffle -> 2 stages
print("=== union() + distinct() ===")
df1.union(df2).distinct().show()
# Charlie appears only ONCE

# ============ INTERSECT ============
# Rows that exist in BOTH DataFrames (removes duplicates)
# Spark UI: 1 job -> 2+ stages (shuffle for comparison)
print("=== intersect() ===")
df1.intersect(df2).show()
# Only Charlie (exists in both)

# intersectAll - keeps duplicates
print("=== intersectAll() ===")
df1.intersectAll(df2).show()

# ============ EXCEPT / SUBTRACT ============
# Rows in df1 that are NOT in df2 (removes duplicates)
# Spark UI: 1 job -> 2+ stages (shuffle for comparison)
print("=== except() - rows in df1 not in df2 ===")
df1.exceptAll(df2).show()
# Alice and Bob (not in df2)

print("=== except() - rows in df2 not in df1 ===")
df2.exceptAll(df1).show()
# Diana and Eve (not in df1)

# subtract() is same as except()
print("=== subtract() - same as except() ===")
df1.subtract(df2).show()

# Write
df1.unionByName(df2).distinct() \
    .write.mode("overwrite").parquet("/shared/union_result")

spark.stop()
