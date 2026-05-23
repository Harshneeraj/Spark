"""
Topic: Reading and Writing Different File Formats
===================================================

Spark supports multiple file formats with different trade-offs.

Spark UI Behavior:
- read: Triggers 1 job if schema inference is needed (CSV/JSON with inferSchema).
  With explicit schema or Parquet (self-describing): NO extra job for schema.
- write: Always triggers 1 job (data must be materialized).
  Stages depend on prior transformations.
- Parquet/ORC reads benefit from predicate pushdown (fewer rows scanned).

Key Interview Points:
- Parquet: Columnar, compressed, schema embedded, predicate pushdown. BEST for analytics.
- ORC: Similar to Parquet, optimized for Hive. Good compression.
- CSV: Row-based, human-readable, no schema, slow. Use for interchange only.
- JSON: Semi-structured, schema inference possible, larger than Parquet.
- Delta/Iceberg: Table formats with ACID, time travel, schema evolution.
- Always prefer Parquet for Spark workloads.
- partitionBy() in write creates directory-based partitioning (partition pruning).
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("20_Read_Write_File_Formats") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

data = [
    (1, "Alice", "Engineering", 90000, "2020-01-15"),
    (2, "Bob", "Marketing", 45000, "2021-06-20"),
    (3, "Charlie", "Engineering", 65000, "2019-03-10"),
    (4, "Diana", "HR", 55000, "2022-09-01"),
    (5, "Eve", "Marketing", 70000, "2018-11-25")
]

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("join_date", StringType(), True)
])

df = spark.createDataFrame(data, schema)

# ============ WRITE FORMATS ============

# 1. PARQUET (Columnar - RECOMMENDED)
# Spark UI: 1 job for write
df.write.mode("overwrite").parquet("/shared/data_parquet")

# Parquet with compression
df.write.mode("overwrite") \
    .option("compression", "snappy") \
    .parquet("/shared/data_parquet_snappy")

# 2. CSV
df.write.mode("overwrite") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .csv("/shared/data_csv")

# 3. JSON
df.write.mode("overwrite").json("/shared/data_json")

# 4. ORC
df.write.mode("overwrite").orc("/shared/data_orc")

# 5. Partitioned write (creates directory structure)
# /shared/data_partitioned/department=Engineering/part-00000.parquet
df.write.mode("overwrite") \
    .partitionBy("department") \
    .parquet("/shared/data_partitioned")

# 6. Bucketed write (pre-shuffle for joins)
df.write.mode("overwrite") \
    .bucketBy(4, "department") \
    .sortBy("salary") \
    .saveAsTable("bucketed_employees")

# ============ READ FORMATS ============

# 1. Read Parquet (schema embedded - no inference needed)
# Spark UI: No extra job for schema
print("=== Read Parquet ===")
df_parquet = spark.read.parquet("/shared/data_parquet")
df_parquet.show()
df_parquet.printSchema()

# 2. Read CSV with schema inference (EXTRA JOB for inference!)
# Spark UI: 1 extra job to scan and infer types
print("=== Read CSV (infer schema - extra job!) ===")
df_csv_infer = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/shared/data_csv")
df_csv_infer.show()

# 3. Read CSV with explicit schema (NO extra job - faster!)
print("=== Read CSV (explicit schema - no extra job) ===")
df_csv_explicit = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv("/shared/data_csv")
df_csv_explicit.show()

# 4. Read JSON
print("=== Read JSON ===")
df_json = spark.read.json("/shared/data_json")
df_json.show()

# 5. Read ORC
print("=== Read ORC ===")
df_orc = spark.read.orc("/shared/data_orc")
df_orc.show()

# 6. Read partitioned data (partition pruning!)
# Only reads Engineering partition - skips others entirely
print("=== Read with Partition Pruning ===")
df_eng = spark.read.parquet("/shared/data_partitioned") \
    .filter(col("department") == "Engineering")
df_eng.show()
df_eng.explain()  # Shows PartitionFilters in scan

# ============ WRITE MODES ============
"""
Write modes:
- "overwrite": Delete existing data, write new
- "append": Add to existing data
- "ignore": Skip if data exists (no error)
- "error"/"errorifexists": Throw error if data exists (default)
"""

# ============ FILE FORMAT COMPARISON ============
"""
Format   | Columnar | Compression | Schema | Predicate Pushdown | Speed
---------|----------|-------------|--------|-------------------|------
Parquet  | Yes      | Excellent   | Yes    | Yes               | Fast
ORC      | Yes      | Excellent   | Yes    | Yes               | Fast
CSV      | No       | Poor        | No     | No                | Slow
JSON     | No       | Poor        | Partial| No                | Slow
Avro     | No       | Good        | Yes    | No                | Medium

ALWAYS use Parquet unless you have a specific reason not to.
"""

# ============ IMPORTANT OPTIONS ============

# Control output file count
df.coalesce(1).write.mode("overwrite").parquet("/shared/single_file")

# Control max records per file
df.write.mode("overwrite") \
    .option("maxRecordsPerFile", 2) \
    .parquet("/shared/max_records")

spark.stop()
