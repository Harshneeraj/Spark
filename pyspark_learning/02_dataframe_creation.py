"""
Topic: DataFrame Creation Methods
==================================

Multiple ways to create DataFrames in PySpark.

Spark UI Behavior:
- createDataFrame() from a Python list triggers 0 jobs (schema is inferred locally).
- If you use inferSchema=True with CSV/JSON, it triggers 1 extra job to scan data.
- show() triggers 1 job with 1 stage (since data is small and local).
- Each show() call = 1 job in Spark UI.

Key Interview Points:
- DataFrames are immutable, distributed collections of data organized into named columns.
- DataFrames are lazily evaluated - transformations build a plan, actions execute it.
- Schema can be explicitly defined (faster) or inferred (extra scan job).
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder \
    .appName("02_DataFrame_Creation") \
    .master("local[*]") \
    .getOrCreate()

# Method 1: From Python list with inferred schema
# Spark UI: No job triggered until an action is called
data = [
    (1, "Alice", 30, 50000.0),
    (2, "Bob", 25, 45000.0),
    (3, "Charlie", 35, 60000.0),
    (4, "Diana", 28, 55000.0),
    (5, "Eve", 32, 70000.0)
]

df_inferred = spark.createDataFrame(data, ["id", "name", "age", "salary"])

# Method 2: From Python list with explicit schema (PREFERRED - no extra scan)
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True)
])

df_explicit = spark.createDataFrame(data, schema)

# Method 3: From RDD
rdd = spark.sparkContext.parallelize(data)
df_from_rdd = spark.createDataFrame(rdd, schema)

# Method 4: From Pandas DataFrame
import pandas as pd
pandas_df = pd.DataFrame(data, columns=["id", "name", "age", "salary"])
df_from_pandas = spark.createDataFrame(pandas_df)

# Actions - each triggers a job in Spark UI
# Job 1: show()
print("=== DataFrame with Explicit Schema ===")
df_explicit.show()
# Spark UI: Job 0 -> Stage 0 -> 1 task (small data, 1 partition)

# Job 2: printSchema() - NO job triggered (schema is metadata)
df_explicit.printSchema()

# Job 3: count() triggers a job
print(f"Row count: {df_explicit.count()}")
# Spark UI: Job 1 -> Stage 1 -> 1 task

# Write to /shared path
df_explicit.write.mode("overwrite").parquet("/shared/employees")
# Spark UI: Job 2 -> Stage 2 -> tasks depend on partitions

spark.stop()
