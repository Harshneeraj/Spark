"""
Topic: withColumn(), withColumnRenamed(), cast(), drop()
=========================================================

Column manipulation operations.

Spark UI Behavior:
- withColumn, withColumnRenamed, cast are all NARROW transformations.
- They add to the logical plan but don't trigger jobs.
- All execute in the same stage as the read.
- show() -> 1 job, 1 stage.

Key Interview Points:
- withColumn() adds a new column or replaces existing one with same name.
- Multiple withColumn() calls create a chain in the plan - Spark optimizes this.
- cast() changes data type (string to int, etc.).
- withColumnRenamed() just renames - no data transformation.
- IMPORTANT: Each withColumn creates a new DataFrame (immutable).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, when, current_date, datediff, to_date
from pyspark.sql.types import IntegerType, DoubleType, StringType

spark = SparkSession.builder \
    .appName("05_WithColumn_Cast_Rename") \
    .master("local[*]") \
    .getOrCreate()

data = [
    (1, "Alice", "30", "50000", "2020-01-15"),
    (2, "Bob", "25", "45000", "2021-06-20"),
    (3, "Charlie", "35", "60000", "2019-03-10"),
    (4, "Diana", "28", "55000", "2022-09-01"),
    (5, "Eve", "32", "70000", "2018-11-25")
]

df = spark.createDataFrame(data, ["id", "name", "age_str", "salary_str", "join_date_str"])

print("=== Original Schema (all strings) ===")
df.printSchema()

# ============ CAST - Type Conversion ============

df_casted = df \
    .withColumn("age", col("age_str").cast(IntegerType())) \
    .withColumn("salary", col("salary_str").cast(DoubleType())) \
    .withColumn("join_date", to_date(col("join_date_str"), "yyyy-MM-dd")) \
    .drop("age_str", "salary_str", "join_date_str")

print("=== After Casting ===")
df_casted.printSchema()
df_casted.show()

# ============ withColumn - Add/Modify Columns ============

# Add new column with literal value
df_enhanced = df_casted.withColumn("company", lit("TechCorp"))

# Add computed column
df_enhanced = df_enhanced.withColumn("monthly_salary", col("salary") / 12)

# Add conditional column
df_enhanced = df_enhanced.withColumn(
    "experience_level",
    when(col("age") >= 35, "Senior")
    .when(col("age") >= 28, "Mid")
    .otherwise("Junior")
)

# Add column using concat
df_enhanced = df_enhanced.withColumn(
    "employee_code",
    concat(lit("EMP_"), col("id").cast(StringType()))
)

# Modify existing column (replace)
df_enhanced = df_enhanced.withColumn("salary", col("salary") * 1.1)  # 10% raise

print("=== Enhanced DataFrame ===")
df_enhanced.show(truncate=False)

# ============ withColumnRenamed ============

df_renamed = df_enhanced \
    .withColumnRenamed("name", "employee_name") \
    .withColumnRenamed("salary", "annual_salary")

print("=== Renamed Columns ===")
df_renamed.printSchema()

# ============ Multiple withColumn (Chaining) ============
# Note: Too many withColumn calls can make the plan complex.
# For many columns, consider using select() with expressions instead.

# Better approach for multiple new columns:
df_better = df_casted.select(
    "*",
    (col("salary") * 0.1).alias("bonus"),
    (col("salary") * 1.1).alias("total_comp"),
    when(col("age") > 30, "Experienced").otherwise("Growing").alias("category")
)

print("=== Using select() for multiple new columns ===")
df_better.show()

# Write
df_enhanced.write.mode("overwrite").parquet("/shared/employees_enhanced")

spark.stop()
