"""
Topic: Spark SQL - Running SQL Queries on DataFrames
=====================================================

Spark SQL allows running SQL queries directly on DataFrames using temporary views.

Spark UI Behavior:
- createOrReplaceTempView() -> NO job (just registers metadata)
- spark.sql("SELECT ...") -> Lazy (no job until action)
- spark.sql("SELECT ...").show() -> 1 job, stages depend on query complexity
- SQL queries go through the same Catalyst optimizer as DataFrame API
- Same physical plan whether you use SQL or DataFrame API

Key Interview Points:
- createOrReplaceTempView: session-scoped (dies with SparkSession)
- createOrReplaceGlobalTempView: accessible across sessions (prefix: global_temp.)
- SQL and DataFrame API produce the SAME execution plan (Catalyst optimizer)
- SQL is often preferred for complex analytical queries
- You can mix SQL and DataFrame API freely
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("12_Spark_SQL") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Create sample data
employees = [
    (1, "Alice", "Engineering", 90000, "2020-01-15"),
    (2, "Bob", "Marketing", 45000, "2021-06-20"),
    (3, "Charlie", "Engineering", 65000, "2019-03-10"),
    (4, "Diana", "HR", 55000, "2022-09-01"),
    (5, "Eve", "Marketing", 70000, "2018-11-25"),
    (6, "Frank", "Engineering", 80000, "2020-07-14"),
    (7, "Grace", "HR", 60000, "2021-02-28"),
    (8, "Henry", "Marketing", 52000, "2023-01-05")
]

df = spark.createDataFrame(employees, ["id", "name", "department", "salary", "join_date"])

# ============ REGISTER AS TEMP VIEW ============
# No job triggered - just metadata registration
df.createOrReplaceTempView("employees")

# ============ BASIC SQL QUERIES ============

# Simple SELECT
# Spark UI: 1 job -> 1 stage (narrow operations only)
print("=== All Employees ===")
spark.sql("SELECT * FROM employees").show()

# WHERE clause
print("=== High Earners ===")
spark.sql("SELECT name, salary FROM employees WHERE salary > 60000").show()

# ORDER BY (causes shuffle for global sort)
# Spark UI: 1 job -> 2 stages (read | sort with shuffle)
print("=== Ordered by Salary ===")
spark.sql("SELECT name, salary FROM employees ORDER BY salary DESC").show()

# ============ AGGREGATION IN SQL ============

# GROUP BY (causes shuffle)
# Spark UI: 1 job -> 2 stages (partial agg | shuffle + final agg)
print("=== Department Stats ===")
spark.sql("""
    SELECT 
        department,
        COUNT(*) as emp_count,
        ROUND(AVG(salary), 2) as avg_salary,
        MAX(salary) as max_salary,
        MIN(salary) as min_salary,
        SUM(salary) as total_salary
    FROM employees
    GROUP BY department
    ORDER BY avg_salary DESC
""").show()

# HAVING clause
print("=== Departments with avg salary > 60000 ===")
spark.sql("""
    SELECT department, AVG(salary) as avg_salary
    FROM employees
    GROUP BY department
    HAVING AVG(salary) > 60000
""").show()

# ============ WINDOW FUNCTIONS IN SQL ============

print("=== Ranking within Department (SQL) ===")
spark.sql("""
    SELECT 
        name, 
        department, 
        salary,
        ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank,
        salary - AVG(salary) OVER (PARTITION BY department) as diff_from_avg
    FROM employees
""").show()

# ============ SUBQUERIES ============

print("=== Employees earning above department average ===")
spark.sql("""
    SELECT name, department, salary
    FROM employees e
    WHERE salary > (
        SELECT AVG(salary) 
        FROM employees 
        WHERE department = e.department
    )
""").show()

# ============ CTE (Common Table Expressions) ============

print("=== Using CTE ===")
spark.sql("""
    WITH dept_stats AS (
        SELECT department, AVG(salary) as avg_sal, COUNT(*) as cnt
        FROM employees
        GROUP BY department
    )
    SELECT e.name, e.department, e.salary, d.avg_sal, d.cnt
    FROM employees e
    JOIN dept_stats d ON e.department = d.department
    WHERE e.salary > d.avg_sal
""").show()

# ============ MIXING SQL AND DATAFRAME API ============

# SQL result is a DataFrame - can chain DataFrame operations
df_sql_result = spark.sql("SELECT * FROM employees WHERE department = 'Engineering'")
df_final = df_sql_result.withColumn("tax", col("salary") * 0.3)
df_final.show()

# ============ GLOBAL TEMP VIEW ============
# Accessible across SparkSessions (prefix with global_temp)
df.createOrReplaceGlobalTempView("global_employees")
spark.sql("SELECT * FROM global_temp.global_employees LIMIT 3").show()

# Write SQL result
spark.sql("""
    SELECT department, COUNT(*) as count, AVG(salary) as avg_salary
    FROM employees
    GROUP BY department
""").write.mode("overwrite").parquet("/shared/sql_dept_stats")

spark.stop()
