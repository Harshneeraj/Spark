"""
Topic: UDFs (User Defined Functions) and Pandas UDFs
=====================================================

UDFs allow custom Python logic on DataFrame columns.

Spark UI Behavior:
- UDFs don't change the number of jobs/stages.
- They execute within existing stages as part of the map operation.
- Regular UDFs: data serialized from JVM -> Python -> JVM (SLOW)
- Pandas UDFs: data transferred in Arrow format (FAST, vectorized)
- In Spark UI, you'll see longer task execution times with UDFs.

Key Interview Points:
- Regular UDFs are SLOW because of serialization overhead (JVM <-> Python).
- Pandas UDFs (vectorized UDFs) use Apache Arrow for efficient transfer.
- Pandas UDFs are 3-100x faster than regular UDFs.
- Always prefer built-in Spark functions over UDFs when possible.
- UDFs are a black box to Catalyst optimizer (can't optimize through them).
- UDF return type MUST be specified.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, pandas_udf, upper
from pyspark.sql.types import StringType, IntegerType, DoubleType, ArrayType
import pandas as pd

spark = SparkSession.builder \
    .appName("13_UDF_User_Defined_Functions") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

data = [
    (1, "alice smith", 30, 50000),
    (2, "bob jones", 25, 45000),
    (3, "charlie brown", 35, 60000),
    (4, "diana prince", 28, 55000),
    (5, "eve wilson", 32, 70000)
]

df = spark.createDataFrame(data, ["id", "name", "age", "salary"])

# ============ REGULAR UDF (Python UDF) ============

# Method 1: Using @udf decorator
@udf(returnType=StringType())
def title_case(name):
    """Convert name to title case"""
    if name is None:
        return None
    return name.title()

# Method 2: Using udf() function
def calculate_tax(salary):
    """Calculate tax based on salary bracket"""
    if salary is None:
        return 0.0
    if salary > 60000:
        return salary * 0.30
    elif salary > 45000:
        return salary * 0.20
    else:
        return salary * 0.10

tax_udf = udf(calculate_tax, DoubleType())

# Method 3: Lambda UDF
categorize_age = udf(lambda age: "Senior" if age >= 30 else "Junior", StringType())

# Apply UDFs
print("=== Regular UDFs ===")
df_with_udf = df \
    .withColumn("title_name", title_case(col("name"))) \
    .withColumn("tax", tax_udf(col("salary"))) \
    .withColumn("age_category", categorize_age(col("age")))

df_with_udf.show()

# ============ UDF RETURNING COMPLEX TYPES ============

@udf(returnType=ArrayType(StringType()))
def split_name(name):
    """Split full name into parts"""
    if name is None:
        return []
    return name.split(" ")

print("=== UDF with Array return type ===")
df.withColumn("name_parts", split_name(col("name"))).show(truncate=False)

# ============ PANDAS UDF (Vectorized UDF) - PREFERRED ============
# Much faster than regular UDFs due to Arrow-based transfer

# Series to Series (most common)
@pandas_udf(StringType())
def pandas_title_case(names: pd.Series) -> pd.Series:
    """Vectorized title case - processes entire column at once"""
    return names.str.title()

@pandas_udf(DoubleType())
def pandas_tax(salaries: pd.Series) -> pd.Series:
    """Vectorized tax calculation"""
    import numpy as np
    conditions = [
        salaries > 60000,
        salaries > 45000,
        salaries <= 45000
    ]
    rates = [0.30, 0.20, 0.10]
    return pd.Series(np.select(conditions, [salaries * r for r in rates], default=0.0))

print("=== Pandas UDFs (Vectorized - FAST) ===")
df_pandas_udf = df \
    .withColumn("title_name", pandas_title_case(col("name"))) \
    .withColumn("tax", pandas_tax(col("salary")))

df_pandas_udf.show()

# ============ REGISTER UDF FOR SQL ============

spark.udf.register("sql_title_case", lambda x: x.title() if x else None, StringType())

df.createOrReplaceTempView("employees")
print("=== UDF in SQL ===")
spark.sql("SELECT id, sql_title_case(name) as formatted_name FROM employees").show()

# ============ PERFORMANCE COMPARISON ============
"""
Performance Hierarchy (fastest to slowest):
1. Built-in Spark functions (upper, lower, etc.) - BEST
2. Pandas UDFs (vectorized, Arrow-based) - GOOD
3. Regular Python UDFs (row-by-row serialization) - SLOW

Example: Converting to uppercase
- Built-in: df.withColumn("upper_name", upper(col("name")))  # FASTEST
- Pandas UDF: Uses pd.Series.str.upper()  # FAST
- Regular UDF: udf(lambda x: x.upper(), StringType())  # SLOWEST

WHY regular UDFs are slow:
1. Each row: JVM -> serialize -> Python -> deserialize -> process -> serialize -> JVM
2. No vectorization (row by row)
3. Catalyst can't optimize through UDFs

WHY Pandas UDFs are faster:
1. Data transferred in batches using Apache Arrow (columnar format)
2. Vectorized operations on entire pandas Series
3. Minimal serialization overhead
"""

# ============ BEST PRACTICE: Prefer Built-in Functions ============

# BAD: Using UDF for something Spark can do natively
bad_upper = udf(lambda x: x.upper() if x else None, StringType())
df.withColumn("upper_name", bad_upper(col("name"))).show()

# GOOD: Using built-in function
df.withColumn("upper_name", upper(col("name"))).show()

# Write
df_with_udf.write.mode("overwrite").parquet("/shared/udf_results")

spark.stop()
