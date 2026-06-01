"""
Topic: Schema Evolution and Partitioning in Hudi
==================================================

How Hudi handles schema changes and data partitioning.

Key Interview Points:
- Hudi supports backward-compatible schema evolution (add columns, widen types).
- Schema is stored in the timeline (each commit records its schema).
- Partition evolution: Can change partition strategy without rewriting.
- Hudi supports both Hive-style partitioning and custom partitioning.
- Multi-level partitioning: year/month/day for time-series data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, year, month, dayofmonth, concat

spark = SparkSession.builder \
    .appName("06_Hudi_Schema_Evolution") \
    .master("local[*]") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

# ============ SCHEMA EVOLUTION ============
"""
Hudi supports these schema changes WITHOUT rewriting existing data:

SUPPORTED (backward-compatible):
✓ Add new columns (nullable, with default value)
✓ Widen column types (int → long, float → double)
✓ Promote column to nullable
✓ Add nested fields to structs

NOT SUPPORTED (breaking changes):
✗ Remove columns (use soft deprecation instead)
✗ Rename columns (add new + deprecate old)
✗ Narrow types (long → int)
✗ Change column from nullable to non-nullable

CONFIGURATION:
hoodie.datasource.write.reconcile.schema = true  (enable schema evolution)
hoodie.schema.on.read.enable = true  (schema-on-read for missing columns)

HOW IT WORKS:
1. Commit 1: Schema = {id, name, amount}
2. Commit 2: Schema = {id, name, amount, email}  ← new column added
3. Reading old files: 'email' column returns NULL (schema-on-read)
4. Reading new files: 'email' column has actual values

# Write with new schema (adds 'email' column)
df_with_email.write.format("hudi") \\
    .option("hoodie.datasource.write.reconcile.schema", "true") \\
    .options(**hudi_options) \\
    .mode("append") \\
    .save("/shared/hudi/orders")

# Read: Old records have email=null, new records have email values
df = spark.read.format("hudi").load("/shared/hudi/orders")
"""

print("=== Schema Evolution ===")

# Simulate schema evolution
# Version 1: Original schema
v1_data = [
    ("ORD001", "user_1", 1200.00, "2024-01-01"),
    ("ORD002", "user_2", 800.00, "2024-01-01"),
]
df_v1 = spark.createDataFrame(v1_data, ["order_id", "user_id", "amount", "date"])
print("Schema V1:")
df_v1.printSchema()

# Version 2: Added 'email' and 'country' columns
v2_data = [
    ("ORD003", "user_3", 500.00, "2024-01-02", "user3@email.com", "US"),
    ("ORD004", "user_4", 600.00, "2024-01-02", "user4@email.com", "UK"),
]
df_v2 = spark.createDataFrame(v2_data, 
    ["order_id", "user_id", "amount", "date", "email", "country"])
print("Schema V2 (added email, country):")
df_v2.printSchema()

# After schema evolution, reading returns unified schema
# Old records have null for new columns
df_v1_evolved = df_v1.withColumn("email", lit(None).cast("string")) \
    .withColumn("country", lit(None).cast("string"))

df_unified = df_v1_evolved.unionByName(df_v2)
print("Unified read (old records have null for new columns):")
df_unified.show()

# ============ PARTITIONING STRATEGIES ============
"""
Hudi supports multiple partitioning approaches:

1. SINGLE COLUMN PARTITION:
   hoodie.datasource.write.partitionpath.field = date
   Layout: /table/date=2024-01-01/, /table/date=2024-01-02/

2. MULTI-LEVEL PARTITION:
   hoodie.datasource.write.partitionpath.field = year,month,day
   Layout: /table/year=2024/month=01/day=01/

3. CUSTOM PARTITION (using KeyGenerator):
   hoodie.datasource.write.keygenerator.class = 
       org.apache.hudi.keygen.CustomKeyGenerator
   hoodie.datasource.write.partitionpath.field = 
       date:SIMPLE,country:SIMPLE
   Layout: /table/2024-01-01/US/, /table/2024-01-01/UK/

4. TIMESTAMP-BASED PARTITION:
   hoodie.datasource.write.keygenerator.class = 
       org.apache.hudi.keygen.TimestampBasedKeyGenerator
   hoodie.deltastreamer.keygen.timebased.timestamp.type = DATE_STRING
   hoodie.deltastreamer.keygen.timebased.input.dateformat = yyyy-MM-dd HH:mm:ss
   hoodie.deltastreamer.keygen.timebased.output.dateformat = yyyy/MM/dd
   Layout: /table/2024/01/01/, /table/2024/01/02/

5. NON-PARTITIONED:
   hoodie.datasource.write.partitionpath.field = ""
   hoodie.datasource.write.keygenerator.class = 
       org.apache.hudi.keygen.NonpartitionedKeyGenerator
   Layout: /table/ (all files in root)

CHOOSING PARTITION STRATEGY:
┌─────────────────────────────┬─────────────────────────────────────────────┐
│ Data Pattern                │ Recommended Partition                       │
├─────────────────────────────┼─────────────────────────────────────────────┤
│ Time-series (daily loads)   │ date or year/month/day                     │
│ Multi-tenant                │ tenant_id                                   │
│ Geographic                  │ country or region                           │
│ Event-driven                │ event_date + event_type                    │
│ Small table (< 1GB)         │ Non-partitioned                            │
│ Large table, many queries   │ Most common filter column                  │
└─────────────────────────────┴─────────────────────────────────────────────┘

PARTITION BEST PRACTICES:
1. Don't over-partition (too many small partitions = overhead)
2. Target: 100MB - 1GB per partition
3. Partition by columns used in WHERE clauses
4. Avoid high-cardinality partition keys (user_id = bad!)
5. Date-based partitioning is most common and recommended
"""

print("\n=== Partitioning Strategies ===")

# Demonstrate partition path generation
data = [
    ("ORD001", "user_1", 1200.00, "2024-01-15 10:30:00", "US"),
    ("ORD002", "user_2", 800.00, "2024-02-20 14:00:00", "UK"),
    ("ORD003", "user_3", 500.00, "2024-01-15 16:45:00", "US"),
    ("ORD004", "user_4", 600.00, "2024-03-10 09:00:00", "IN"),
]

df = spark.createDataFrame(data, ["order_id", "user_id", "amount", "event_time", "country"])

# Single partition by date
print("Single partition (date):")
df.withColumn("date", col("event_time").substr(1, 10)) \
    .select("order_id", "date").show()

# Multi-level partition
from pyspark.sql.functions import to_timestamp
df_ts = df.withColumn("ts", to_timestamp("event_time"))
print("Multi-level partition (year/month/day):")
df_ts.select(
    "order_id",
    year("ts").alias("year"),
    month("ts").alias("month"),
    dayofmonth("ts").alias("day")
).show()

# Composite partition
print("Composite partition (date + country):")
df.withColumn("partition_path", 
    concat(col("event_time").substr(1, 10), lit("/"), col("country"))
).select("order_id", "partition_path").show()

# ============ KEY GENERATORS ============
"""
Key Generators determine how record key and partition path are derived:

1. SimpleKeyGenerator (default):
   - Single record key field, single partition field
   - hoodie.datasource.write.recordkey.field = order_id
   - hoodie.datasource.write.partitionpath.field = date

2. ComplexKeyGenerator:
   - Composite record key (multiple fields)
   - hoodie.datasource.write.recordkey.field = user_id,order_id
   - hoodie.datasource.write.partitionpath.field = date

3. CustomKeyGenerator:
   - Custom partition path formatting
   - Supports SIMPLE, TIMESTAMP partition types
   - hoodie.datasource.write.partitionpath.field = date:SIMPLE,country:SIMPLE

4. TimestampBasedKeyGenerator:
   - Converts timestamp to partition path format
   - Input: 2024-01-15 10:30:00 → Output: 2024/01/15

5. NonpartitionedKeyGenerator:
   - No partitioning (all data in one directory)
   - For small tables or when partitioning isn't beneficial
"""

# Write demo
df.write.mode("overwrite").parquet("/shared/hudi_demo/schema_evolution")

spark.stop()
