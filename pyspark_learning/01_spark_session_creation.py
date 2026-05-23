"""
Topic: SparkSession Creation and Configuration
================================================

SparkSession is the entry point to PySpark. It provides a way to interact with
Spark functionality including DataFrames, SQL, Streaming, etc.

Spark UI Behavior:
- Creating a SparkSession does NOT trigger any job in Spark UI.
- No stages, no tasks are created.
- The Spark UI becomes available at http://localhost:4040 once the session is created.
- You will see the "Environment" tab populated with all configurations.

Key Interview Points:
- SparkSession was introduced in Spark 2.0, unifying SQLContext and HiveContext.
- getOrCreate() reuses an existing session or creates a new one.
- SparkSession is thread-safe for SQL operations.
"""

from pyspark.sql import SparkSession

# Basic SparkSession creation
spark = SparkSession.builder \
    .appName("01_SparkSession_Creation") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

# Access SparkContext from SparkSession
sc = spark.sparkContext

# Print basic info
print(f"Spark Version: {spark.version}")
print(f"App Name: {sc.appName}")
print(f"Master: {sc.master}")
print(f"Default Parallelism: {sc.defaultParallelism}")

# Check all configurations
print("\n--- All Spark Configurations ---")
for conf in spark.sparkContext.getConf().getAll():
    print(f"{conf[0]} = {conf[1]}")

spark.stop()
