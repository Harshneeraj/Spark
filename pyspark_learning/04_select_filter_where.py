"""
Topic: select(), filter(), where(), drop(), alias()
=====================================================

Basic column selection and row filtering operations.

Spark UI Behavior:
- select, filter, where, drop are all NARROW transformations.
- They execute in the SAME stage as the data read.
- show() triggers 1 job -> 1 stage -> tasks = number of partitions
- No shuffle involved.

Key Interview Points:
- filter() and where() are identical - just aliases.
- select() can take column names as strings or Column objects.
- drop() removes columns (opposite of select).
- alias() / .alias() renames a column in the output.
- Column pruning: Spark only reads columns that are needed (predicate pushdown).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

spark = SparkSession.builder \
    .appName("04_Select_Filter_Where") \
    .master("local[*]") \
    .getOrCreate()

data = [
    (1, "Alice", "Engineering", "Senior", 90000),
    (2, "Bob", "Marketing", "Junior", 45000),
    (3, "Charlie", "Engineering", "Mid", 65000),
    (4, "Diana", "HR", "Senior", 75000),
    (5, "Eve", "Marketing", "Mid", 55000),
    (6, "Frank", "Engineering", "Junior", 50000)
]

df = spark.createDataFrame(data, ["id", "name", "department", "level", "salary"])

# ============ SELECT ============

# Method 1: String column names
df.select("name", "salary").show()
# Spark UI: Job 0 -> Stage 0 -> 1 task

# Method 2: Column objects
df.select(col("name"), col("salary") * 1.1).show()

# Method 3: Using expr() for SQL-like expressions
df.select(expr("name"), expr("salary * 1.1 as adjusted_salary")).show()

# Method 4: Select with alias
df.select(
    col("name").alias("employee_name"),
    col("salary").alias("annual_salary")
).show()

# ============ FILTER / WHERE ============

# filter and where are IDENTICAL
df.filter(col("salary") > 60000).show()
df.where(col("salary") > 60000).show()  # Same result

# Multiple conditions with & (AND) and | (OR)
df.filter(
    (col("department") == "Engineering") & (col("salary") > 60000)
).show()

# Using isin()
df.filter(col("level").isin("Senior", "Mid")).show()

# Using like (SQL pattern matching)
df.filter(col("name").like("A%")).show()

# Negation with ~
df.filter(~col("department").isin("HR", "Marketing")).show()

# isNull / isNotNull
df.filter(col("name").isNotNull()).show()

# between
df.filter(col("salary").between(50000, 70000)).show()

# ============ DROP ============

# Drop single column
df.drop("level").show()

# Drop multiple columns
df.drop("level", "id").show()

# ============ DISTINCT / dropDuplicates ============

df.select("department").distinct().show()
# Spark UI: distinct() causes a shuffle -> 2 stages

df.dropDuplicates(["department", "level"]).show()

# Write result
df.filter(col("department") == "Engineering") \
    .select("name", "level", "salary") \
    .write.mode("overwrite").csv("/shared/engineering_team")

spark.stop()
