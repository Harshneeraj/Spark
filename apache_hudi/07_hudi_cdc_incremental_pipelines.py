"""
Topic: CDC and Incremental Pipelines with Hudi
================================================

Hudi's native CDC and incremental query support for building
efficient data pipelines that process only changed data.

Spark UI Behavior:
- Incremental read: Fewer tasks than full scan (reads only changed files).
- CDC read: Same as incremental but includes operation type (I/U/D).
- Pipeline jobs are much faster than full-table scans.

Key Interview Points:
- Incremental queries read ONLY changed data since a timestamp.
- CDC (Change Data Capture) provides before/after images of changes.
- Hudi is designed for streaming CDC from databases (MySQL, Postgres).
- DeltaStreamer: Built-in tool for ingesting CDC from various sources.
- Incremental pipelines avoid expensive full-table scans.
- This is Hudi's biggest differentiator vs plain Parquet.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, when, to_timestamp

spark = SparkSession.builder \
    .appName("07_Hudi_CDC_Incremental") \
    .master("local[*]") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# ============ CDC (Change Data Capture) CONCEPT ============
"""
CDC captures INSERT, UPDATE, DELETE operations from a source database
and applies them to the data lake.

Source Database (MySQL/Postgres)
    │
    │ CDC Tool (Debezium, AWS DMS, etc.)
    ▼
Kafka Topic (CDC events)
    │
    │ Hudi DeltaStreamer / Spark Streaming
    ▼
Hudi Table (data lake with full history)
    │
    │ Incremental Query
    ▼
Downstream (analytics, ML, reporting)


CDC EVENT FORMAT (from Debezium):
{
  "op": "u",           // c=create, u=update, d=delete
  "before": {          // Previous state (for updates/deletes)
    "id": 1,
    "name": "Alice",
    "salary": 50000
  },
  "after": {           // New state (for creates/updates)
    "id": 1,
    "name": "Alice",
    "salary": 55000    // salary changed!
  },
  "ts_ms": 1704067200000,
  "source": {
    "table": "employees",
    "db": "hr"
  }
}
"""

# ============ SIMULATING CDC EVENTS ============

# Batch 1: Initial load (all inserts)
batch1_data = [
    ("I", 1, "Alice", "Engineering", 90000, "2024-01-01 10:00:00"),
    ("I", 2, "Bob", "Marketing", 45000, "2024-01-01 10:00:00"),
    ("I", 3, "Charlie", "Engineering", 65000, "2024-01-01 10:00:00"),
    ("I", 4, "Diana", "HR", 55000, "2024-01-01 10:00:00"),
    ("I", 5, "Eve", "Marketing", 70000, "2024-01-01 10:00:00"),
]

# Batch 2: Changes (updates + inserts + deletes)
batch2_data = [
    ("U", 1, "Alice", "Engineering", 95000, "2024-01-02 09:00:00"),  # Salary update
    ("U", 3, "Charlie", "Management", 80000, "2024-01-02 09:05:00"),  # Dept + salary
    ("I", 6, "Frank", "Engineering", 60000, "2024-01-02 09:10:00"),  # New hire
    ("D", 5, "Eve", "Marketing", 70000, "2024-01-02 09:15:00"),  # Terminated
]

# Batch 3: More changes
batch3_data = [
    ("U", 2, "Bob", "Marketing", 50000, "2024-01-03 08:00:00"),  # Raise
    ("I", 7, "Grace", "HR", 58000, "2024-01-03 08:05:00"),  # New hire
    ("U", 6, "Frank", "Engineering", 65000, "2024-01-03 08:10:00"),  # Raise
]

df_batch1 = spark.createDataFrame(batch1_data,
    ["op", "id", "name", "department", "salary", "event_time"])
df_batch2 = spark.createDataFrame(batch2_data,
    ["op", "id", "name", "department", "salary", "event_time"])
df_batch3 = spark.createDataFrame(batch3_data,
    ["op", "id", "name", "department", "salary", "event_time"])

print("=== Batch 1: Initial Load (all inserts) ===")
df_batch1.show()

print("=== Batch 2: CDC Changes ===")
df_batch2.show()

print("=== Batch 3: More CDC Changes ===")
df_batch3.show()

# ============ PROCESSING CDC WITH HUDI ============
"""
# Processing CDC events and writing to Hudi:

def process_cdc_batch(df_cdc, hudi_table_path):
    '''Process a batch of CDC events into Hudi table.'''
    
    # Separate deletes from inserts/updates
    df_upserts = df_cdc.filter(col("op").isin("I", "U")).drop("op")
    df_deletes = df_cdc.filter(col("op") == "D").drop("op")
    
    # Upsert (insert + update)
    if df_upserts.count() > 0:
        df_upserts.write.format("hudi") \\
            .options(**{
                "hoodie.table.name": "employees",
                "hoodie.datasource.write.recordkey.field": "id",
                "hoodie.datasource.write.precombine.field": "event_time",
                "hoodie.datasource.write.partitionpath.field": "department",
                "hoodie.datasource.write.operation": "upsert",
            }) \\
            .mode("append") \\
            .save(hudi_table_path)
    
    # Delete
    if df_deletes.count() > 0:
        df_deletes.write.format("hudi") \\
            .options(**{
                "hoodie.table.name": "employees",
                "hoodie.datasource.write.recordkey.field": "id",
                "hoodie.datasource.write.precombine.field": "event_time",
                "hoodie.datasource.write.partitionpath.field": "department",
                "hoodie.datasource.write.operation": "delete",
            }) \\
            .mode("append") \\
            .save(hudi_table_path)
"""

# ============ INCREMENTAL QUERY FOR PIPELINES ============
"""
INCREMENTAL PIPELINE PATTERN:

Instead of scanning the ENTIRE table every time, read only CHANGES:

# Traditional (SLOW - full scan every run):
df_all = spark.read.format("hudi").load("/hudi/employees")
df_all.join(dim_table, ...).write.parquet("/output")  # Processes ALL records

# Incremental (FAST - only changes since last run):
df_changes = spark.read.format("hudi") \\
    .option("hoodie.datasource.query.type", "incremental") \\
    .option("hoodie.datasource.read.begin.instanttime", last_commit_time) \\
    .load("/hudi/employees")
df_changes.join(dim_table, ...).write.mode("append").parquet("/output")  # Only new/changed!

TRACKING LAST PROCESSED COMMIT:
1. Store last processed commit time in a control table
2. On next run, read from that commit time
3. After processing, update control table with new commit time

# Get latest commit time from Hudi table
latest_commit = spark.read.format("hudi") \\
    .load("/hudi/employees") \\
    .select("_hoodie_commit_time") \\
    .agg(max("_hoodie_commit_time")) \\
    .collect()[0][0]
"""

# Simulate incremental processing
print("\n=== Incremental Pipeline Simulation ===")

# After batch 1: Full table state
print("After Batch 1 (initial load):")
df_state_1 = df_batch1.drop("op")
df_state_1.show()

# After batch 2: Apply changes
print("After Batch 2 (apply CDC):")
# Upserts from batch 2
upserts_2 = df_batch2.filter(col("op").isin("I", "U")).drop("op")
deletes_2 = df_batch2.filter(col("op") == "D").select("id")

# Apply: Remove deleted, update existing, add new
df_state_2 = df_state_1.join(deletes_2, "id", "leftanti")  # Remove deletes
df_state_2 = df_state_2.join(upserts_2, "id", "leftanti")  # Remove records being updated
df_state_2 = df_state_2.unionByName(upserts_2)  # Add updated/new records
df_state_2.orderBy("id").show()

# Incremental read would return ONLY batch 2 changes (not full table!)
print("Incremental query (only batch 2 changes):")
df_batch2.show()
print("^ This is what incremental query returns - NOT the full table!")

# ============ HUDI CDC MODE (Hudi 0.13+) ============
"""
Hudi's native CDC mode provides before/after images:

# Enable CDC on table
hoodie.table.cdc.enabled = true
hoodie.table.cdc.supplemental.logging.mode = data_before_after

# Read CDC changes
df_cdc = spark.read.format("hudi") \\
    .option("hoodie.datasource.query.type", "incremental") \\
    .option("hoodie.datasource.query.incremental.format", "cdc") \\
    .option("hoodie.datasource.read.begin.instanttime", "20240101100000") \\
    .load("/hudi/employees")

# Returns columns:
# op: I (insert), U (update), D (delete)
# ts_ms: timestamp of change
# before: struct with previous values (null for inserts)
# after: struct with new values (null for deletes)

USE CASES FOR CDC MODE:
1. Propagate changes to downstream systems
2. Build audit logs (who changed what, when)
3. Replicate to another data store
4. Feed real-time dashboards with deltas
"""

# ============ DELTASTREAMER (Built-in CDC Ingestion) ============
"""
HoodieDeltaStreamer: Built-in tool for continuous/scheduled ingestion.

Supports sources:
- Kafka (JSON, Avro, Debezium CDC)
- DFS (files on HDFS/S3)
- JDBC (database tables)
- S3 events

# Run DeltaStreamer for Kafka CDC ingestion:
spark-submit \\
    --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer \\
    --packages org.apache.hudi:hudi-utilities-bundle_2.12:0.14.1 \\
    --props /path/to/deltastreamer.properties \\
    --table-type MERGE_ON_READ \\
    --source-class org.apache.hudi.utilities.sources.debezium.MysqlDebeziumSource \\
    --source-ordering-field ts_ms \\
    --target-base-path /shared/hudi/employees \\
    --target-table employees \\
    --op UPSERT \\
    --continuous

# deltastreamer.properties:
hoodie.datasource.write.recordkey.field=id
hoodie.datasource.write.precombine.field=ts_ms
hoodie.datasource.write.partitionpath.field=department
hoodie.deltastreamer.source.kafka.topic=dbserver1.hr.employees
hoodie.deltastreamer.source.kafka.value.deserializer.class=io.confluent.kafka.serializers.KafkaAvroDeserializer
bootstrap.servers=kafka:9092
schema.registry.url=http://schema-registry:8081

DELTASTREAMER MODES:
1. --continuous: Runs forever, ingests as data arrives (streaming)
2. Without --continuous: Runs once, processes available data (batch)
3. Can be scheduled via cron/Airflow for periodic ingestion
"""

# ============ COMPLETE INCREMENTAL PIPELINE EXAMPLE ============
"""
# Full incremental pipeline: Source → Hudi → Downstream

# Step 1: Read incremental changes from source Hudi table
last_processed = read_checkpoint()  # e.g., "20240102090000"

df_changes = spark.read.format("hudi") \\
    .option("hoodie.datasource.query.type", "incremental") \\
    .option("hoodie.datasource.read.begin.instanttime", last_processed) \\
    .load("/hudi/source/employees")

# Step 2: Transform (enrich, filter, aggregate)
df_enriched = df_changes \\
    .join(broadcast(df_departments), "department", "left") \\
    .withColumn("processed_at", current_timestamp())

# Step 3: Write to downstream Hudi table
df_enriched.write.format("hudi") \\
    .options(**downstream_hudi_options) \\
    .mode("append") \\
    .save("/hudi/downstream/enriched_employees")

# Step 4: Update checkpoint
new_checkpoint = df_changes.agg(max("_hoodie_commit_time")).collect()[0][0]
save_checkpoint(new_checkpoint)

# BENEFITS:
# - Processes only changed records (not millions of unchanged ones)
# - Maintains exactly-once semantics with checkpointing
# - Scales to any table size (processing time depends on change volume)
"""

print("\n=== CDC Pipeline Summary ===")
print("""
1. CDC captures INSERT/UPDATE/DELETE from source databases
2. Hudi stores changes with full history (time travel)
3. Incremental queries read ONLY changes (not full scan)
4. DeltaStreamer automates Kafka/DB → Hudi ingestion
5. Downstream pipelines use incremental reads for efficiency
""")

# Write demo
df_state_2.write.mode("overwrite").parquet("/shared/hudi_demo/cdc_pipeline")

spark.stop()
