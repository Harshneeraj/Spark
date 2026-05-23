"""
Topic: DataFrame Joins (All Types)
====================================

Joins are WIDE transformations that cause shuffles.

Spark UI Behavior:
- A regular join triggers 1 job with 3 stages:
  Stage 0: Read + shuffle left DataFrame
  Stage 1: Read + shuffle right DataFrame  
  Stage 2: Join the shuffled data (sort-merge join by default)
- If one side is small enough, Spark may auto-broadcast (no shuffle on that side)
  -> 2 stages instead of 3
- Broadcast threshold: spark.sql.autoBroadcastJoinThreshold (default 10MB)

Key Interview Points:
- Default join strategy is Sort-Merge Join (both sides shuffled + sorted).
- Broadcast Hash Join: small table broadcast to all executors (no shuffle).
- Shuffle Hash Join: both sides shuffled, hash table built on smaller side.
- Join types: inner, left, right, full/outer, cross, semi, anti.
- ALWAYS specify join type explicitly for clarity.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("07_Joins") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Employee data
employees = [
    (1, "Alice", 101, 90000),
    (2, "Bob", 102, 45000),
    (3, "Charlie", 101, 65000),
    (4, "Diana", 103, 55000),
    (5, "Eve", 104, 70000),  # dept 104 doesn't exist in departments
    (6, "Frank", None, 50000)  # null department
]

# Department data
departments = [
    (101, "Engineering", "Building A"),
    (102, "Marketing", "Building B"),
    (103, "HR", "Building C"),
    (105, "Finance", "Building D")  # No employees in Finance
]

df_emp = spark.createDataFrame(employees, ["emp_id", "name", "dept_id", "salary"])
df_dept = spark.createDataFrame(departments, ["dept_id", "dept_name", "location"])

print("=== Employees ===")
df_emp.show()
print("=== Departments ===")
df_dept.show()

# ============ INNER JOIN ============
# Only matching rows from both sides
# Spark UI: 1 job -> 3 stages (shuffle both + join)
print("=== INNER JOIN ===")
df_emp.join(df_dept, "dept_id", "inner").show()
# Eve (dept 104) and Frank (null) are excluded
# Finance (dept 105) is excluded

# ============ LEFT JOIN (LEFT OUTER) ============
# All rows from left + matching from right (null if no match)
print("=== LEFT JOIN ===")
df_emp.join(df_dept, "dept_id", "left").show()
# Eve and Frank appear with null dept_name/location

# ============ RIGHT JOIN (RIGHT OUTER) ============
# All rows from right + matching from left (null if no match)
print("=== RIGHT JOIN ===")
df_emp.join(df_dept, "dept_id", "right").show()
# Finance appears with null employee info

# ============ FULL OUTER JOIN ============
# All rows from both sides (null where no match)
print("=== FULL OUTER JOIN ===")
df_emp.join(df_dept, "dept_id", "outer").show()
# Everyone appears, nulls where no match

# ============ CROSS JOIN ============
# Cartesian product - every row paired with every other row
# WARNING: Produces M x N rows!
print("=== CROSS JOIN (first 5 rows) ===")
df_emp.crossJoin(df_dept).show(5)
# 6 employees x 4 departments = 24 rows

# ============ LEFT SEMI JOIN ============
# Rows from left that HAVE a match in right (like EXISTS in SQL)
# Only returns columns from left side
print("=== LEFT SEMI JOIN ===")
df_emp.join(df_dept, "dept_id", "leftsemi").show()
# Only employees whose dept exists in departments table

# ============ LEFT ANTI JOIN ============
# Rows from left that DON'T have a match in right (like NOT EXISTS)
# Only returns columns from left side
print("=== LEFT ANTI JOIN ===")
df_emp.join(df_dept, "dept_id", "leftanti").show()
# Only Eve (dept 104 not in departments) - Frank excluded due to null

# ============ JOIN ON MULTIPLE COLUMNS ============
# When join column names differ or multiple conditions
print("=== Join with different column names ===")
df_emp_renamed = df_emp.withColumnRenamed("dept_id", "department_id")
df_emp_renamed.join(
    df_dept,
    df_emp_renamed["department_id"] == df_dept["dept_id"],
    "inner"
).drop("dept_id").show()

# ============ JOIN WITH COMPLEX CONDITION ============
print("=== Join with complex condition ===")
df_emp.join(
    df_dept,
    (df_emp["dept_id"] == df_dept["dept_id"]) & (df_emp["salary"] > 50000),
    "inner"
).select(df_emp["name"], df_dept["dept_name"], df_emp["salary"]).show()

# Write joined result
df_emp.join(df_dept, "dept_id", "left") \
    .write.mode("overwrite").parquet("/shared/employee_department_joined")

spark.stop()
