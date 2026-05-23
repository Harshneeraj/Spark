"""
Topic: Handling Null Values
============================

Null handling is critical for data quality and correct results.

Spark UI Behavior:
- Null handling operations (fillna, dropna, isNull) are NARROW transformations.
- No shuffle, no extra stages.
- Execute in the same stage as the read.
- show() -> 1 job, 1 stage.

Key Interview Points:
- Nulls propagate in expressions: null + 5 = null, null > 5 = null.
- Nulls in groupBy: null keys form their own group.
- Nulls in joins: null != null (null keys never match in joins!).
- Nulls in orderBy: nulls are placed first (ASC) or last (DESC) by default.
- coalesce() returns first non-null value (different from coalesce(N) for partitions!).
- isNull/isNotNull for null checks (NOT == None or == null).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, coalesce, lit, isnull, 
    count, sum, avg, isnan, nanvl
)

spark = SparkSession.builder \
    .appName("28_Handling_Nulls") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

data = [
    (1, "Alice", "Engineering", 90000.0),
    (2, "Bob", None, 45000.0),          # null department
    (3, None, "Engineering", 65000.0),   # null name
    (4, "Diana", "HR", None),            # null salary
    (5, "Eve", None, None),              # multiple nulls
    (6, "Frank", "Engineering", 80000.0),
    (7, None, None, None),               # all nulls except id
]

df = spark.createDataFrame(data, ["id", "name", "department", "salary"])

print("=== Original Data with Nulls ===")
df.show()

# ============ DETECTING NULLS ============

# isNull / isNotNull
print("=== Rows with null name ===")
df.filter(col("name").isNull()).show()

print("=== Rows with non-null department ===")
df.filter(col("department").isNotNull()).show()

# Count nulls per column
print("=== Null counts per column ===")
df.select(
    count(when(col("name").isNull(), 1)).alias("null_names"),
    count(when(col("department").isNull(), 1)).alias("null_depts"),
    count(when(col("salary").isNull(), 1)).alias("null_salaries")
).show()

# ============ DROPPING NULLS ============

# Drop rows where ANY column is null
print("=== dropna('any') - drop if ANY null ===")
df.dropna("any").show()  # Only rows with zero nulls

# Drop rows where ALL columns are null
print("=== dropna('all') - drop if ALL null ===")
df.dropna("all").show()  # Only drops row 7 (if all were null)

# Drop based on specific columns
print("=== Drop if name OR department is null ===")
df.dropna(subset=["name", "department"]).show()

# Drop with threshold (keep rows with at least N non-null values)
print("=== Drop if fewer than 3 non-null values ===")
df.dropna(thresh=3).show()

# ============ FILLING NULLS ============

# fillna with single value (applies to matching type columns)
print("=== fillna with defaults ===")
df.fillna({
    "name": "Unknown",
    "department": "Unassigned",
    "salary": 0.0
}).show()

# fillna with column-specific values
print("=== fillna specific columns ===")
df.fillna("N/A", subset=["name", "department"]).show()

# ============ COALESCE (first non-null) ============
# Note: This is the FUNCTION coalesce, not the partition coalesce!

print("=== coalesce() - first non-null value ===")
df.withColumn(
    "dept_or_default",
    coalesce(col("department"), lit("No Department"))
).show()

# Multiple fallbacks
df.withColumn(
    "display_name",
    coalesce(col("name"), col("department"), lit("Anonymous"))
).show()

# ============ NULL BEHAVIOR IN OPERATIONS ============

# Null arithmetic: null + anything = null
print("=== Null in arithmetic (null + 1000 = null) ===")
df.withColumn("salary_plus_bonus", col("salary") + 1000).show()

# Null comparison: null > 50000 = null (not true or false!)
print("=== Null in comparison (null > 50000 = null, excluded from filter) ===")
df.filter(col("salary") > 50000).show()
# Rows with null salary are EXCLUDED (null is not > 50000)

# To include nulls in filter:
print("=== Include nulls in filter ===")
df.filter((col("salary") > 50000) | col("salary").isNull()).show()

# ============ NULLS IN JOINS ============
"""
CRITICAL: null != null in joins!
Rows with null join keys will NEVER match.
"""

df_left = spark.createDataFrame([
    (1, "A"), (2, "B"), (None, "C")
], ["key", "val_left"])

df_right = spark.createDataFrame([
    (1, "X"), (None, "Y"), (3, "Z")
], ["key", "val_right"])

print("=== Null keys in INNER join (nulls don't match!) ===")
df_left.join(df_right, "key", "inner").show()
# Only key=1 matches. null != null!

print("=== Null keys in LEFT join ===")
df_left.join(df_right, "key", "left").show()
# Left null row appears but doesn't match right null row

# To join on nulls, use eqNullSafe (<=>)
print("=== Null-safe join (nulls DO match) ===")
df_left.join(
    df_right,
    df_left["key"].eqNullSafe(df_right["key"]),
    "inner"
).show()

# ============ NULLS IN GROUPBY ============

print("=== Nulls in groupBy (null is its own group) ===")
df.groupBy("department").count().show()
# null department forms its own group

# ============ NULLS IN ORDERBY ============

print("=== Nulls in orderBy (nulls first by default in ASC) ===")
df.orderBy("salary").show()  # nulls first

print("=== Nulls last ===")
df.orderBy(col("salary").asc_nulls_last()).show()

# ============ NaN vs NULL ============
"""
NaN (Not a Number) is different from NULL:
- NULL = missing/unknown value
- NaN = result of invalid math (0/0, sqrt(-1))
- isNull() doesn't catch NaN!
- Use isnan() for NaN detection
- Use nanvl() to replace NaN
"""

from pyspark.sql.types import DoubleType
df_nan = spark.createDataFrame([
    (1, float('nan')),
    (2, 3.14),
    (3, None),
    (4, float('nan'))
], ["id", "value"])

print("=== NaN vs NULL ===")
df_nan.show()
df_nan.select(
    col("id"),
    col("value"),
    isnan("value").alias("is_nan"),
    isnull("value").alias("is_null")
).show()

# Write
df.fillna({"name": "Unknown", "department": "Unassigned", "salary": 0.0}) \
    .write.mode("overwrite").parquet("/shared/null_handled")

spark.stop()
