"""
Topic: Hudi Write Operations - Insert, Upsert, Delete, Bulk Insert
====================================================================

All write operation types in Apache Hudi.

Spark UI Behavior:
- INSERT: 1-2 jobs (write data + commit metadata).
- UPSERT: 2-3 jobs (index lookup to find existing records + write + commit).
  Index lookup is the extra cost vs plain insert.
- DELETE: 2-3 jobs (index lookup + write delete markers + commit).
- BULK_INSERT: 1 job (no index, fastest for initial load).
- Compaction (MoR): Separate async job.

Key Interview Points:
- UPSERT is Hudi's killer feature (update existing, insert new).
- Record key + precombine field determine how duplicates are resolved.
- BULK_INSERT skips indexing (fastest for initial/full load).
- INSERT may create duplicates if record already exists.
- DELETE can be soft (mark deleted) or hard (remove from storage).
- Write operations are atomic (all-or-nothing via timeline).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, when

spark = SparkSession.builder \
    .appName("02_Hudi_Write_Operations") \
    .master("local[*]") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# ============ COMMON HUDI OPTIONS ============
"""
hudi_options = {
    # Table identity
    "hoodie.table.name": "orders",
    
    # Record identification
    "hoodie.datasource.write.recordkey.field": "order_id",
    "hoodie.datasource.write.precombine.field": "updated_at",
    "hoodie.datasource.write.partitionpath.field": "date",
    
    # Write operation
    "hoodie.datasource.write.operation": "upsert",  # insert/upsert/delete/bulk_insert
    
    # Table type
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",  # or MERGE_ON_READ
    
    # Index type (for upsert performance)
    "hoodie.index.type": "BLOOM",  # BLOOM, SIMPLE, GLOBAL_BLOOM, GLOBAL_SIMPLE, BUCKET
    
    # Parallelism
    "hoodie.insert.shuffle.parallelism": "4",
    "hoodie.upsert.shuffle.parallelism": "4",
    "hoodie.delete.shuffle.parallelism": "4",
    "hoodie.bulkinsert.shuffle.parallelism": "4",
}
"""

# ============ 1. BULK INSERT (Initial Load) ============
"""
BULK_INSERT:
- Fastest write operation (no index lookup)
- Use for: Initial table creation, full refresh, large backfills
- Does NOT check for existing records (may create duplicates)
- Sorts data by partition path for optimal file layout
- No deduplication

Spark UI: 1 job (just write, no index)

hudi_options = {
    "hoodie.table.name": "orders",
    "hoodie.datasource.write.recordkey.field": "order_id",
    "hoodie.datasource.write.precombine.field": "updated_at",
    "hoodie.datasource.write.partitionpath.field": "date",
    "hoodie.datasource.write.operation": "bulk_insert",
    "hoodie.bulkinsert.shuffle.parallelism": "4",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
}

df_initial.write.format("hudi") \\
    .options(**hudi_options) \\
    .mode("overwrite") \\
    .save("/shared/hudi/orders")
"""

# Initial data
initial_data = [
    ("ORD001", "user_1", "laptop", 1200.00, "2024-01-01", "2024-01-01 10:00:00"),
    ("ORD002", "user_2", "phone", 800.00, "2024-01-01", "2024-01-01 10:05:00"),
    ("ORD003", "user_3", "tablet", 500.00, "2024-01-01", "2024-01-01 10:10:00"),
    ("ORD004", "user_1", "headphones", 200.00, "2024-01-02", "2024-01-02 09:00:00"),
    ("ORD005", "user_4", "monitor", 600.00, "2024-01-02", "2024-01-02 09:30:00"),
]

df_initial = spark.createDataFrame(initial_data,
    ["order_id", "user_id", "product", "amount", "date", "updated_at"])

print("=== 1. BULK INSERT (Initial Load) ===")
df_initial.show()

# ============ 2. INSERT ============
"""
INSERT:
- Inserts new records without checking for duplicates
- Faster than upsert (no index lookup)
- May create duplicates if record key already exists!
- Use for: Append-only data (logs, events) where duplicates are OK

Spark UI: 1-2 jobs (write + commit)

hudi_options["hoodie.datasource.write.operation"] = "insert"

df_new.write.format("hudi") \\
    .options(**hudi_options) \\
    .mode("append") \\
    .save("/shared/hudi/orders")
"""

new_orders = [
    ("ORD006", "user_5", "keyboard", 150.00, "2024-01-03", "2024-01-03 08:00:00"),
    ("ORD007", "user_2", "mouse", 50.00, "2024-01-03", "2024-01-03 08:30:00"),
]

df_new = spark.createDataFrame(new_orders,
    ["order_id", "user_id", "product", "amount", "date", "updated_at"])

print("\n=== 2. INSERT (New Records) ===")
df_new.show()

# ============ 3. UPSERT (Most Important!) ============
"""
UPSERT (Update + Insert):
- If record key EXISTS: UPDATE the record
- If record key DOESN'T EXIST: INSERT as new record
- Uses precombine field to resolve conflicts (higher value wins)
- This is Hudi's PRIMARY use case!

Spark UI: 2-3 jobs (index lookup + write + commit)

How it works:
1. Index Lookup: Check which incoming records already exist in table
2. Tag Records: Mark each record as INSERT or UPDATE
3. Write: 
   - CoW: Rewrite affected files with updated records
   - MoR: Append updates to log files
4. Commit: Update timeline metadata

hudi_options["hoodie.datasource.write.operation"] = "upsert"

df_updates.write.format("hudi") \\
    .options(**hudi_options) \\
    .mode("append") \\
    .save("/shared/hudi/orders")
"""

# Mix of updates and new records
upsert_data = [
    # UPDATE: ORD001 amount changed from 1200 to 1100 (price adjustment)
    ("ORD001", "user_1", "laptop", 1100.00, "2024-01-01", "2024-01-03 12:00:00"),
    # UPDATE: ORD003 amount changed (partial refund)
    ("ORD003", "user_3", "tablet", 400.00, "2024-01-01", "2024-01-03 12:05:00"),
    # INSERT: New order
    ("ORD008", "user_6", "camera", 900.00, "2024-01-03", "2024-01-03 12:10:00"),
]

df_upsert = spark.createDataFrame(upsert_data,
    ["order_id", "user_id", "product", "amount", "date", "updated_at"])

print("\n=== 3. UPSERT (Update existing + Insert new) ===")
print("ORD001: amount 1200 -> 1100 (UPDATE)")
print("ORD003: amount 500 -> 400 (UPDATE)")
print("ORD008: new record (INSERT)")
df_upsert.show()

# ============ 4. DELETE ============
"""
DELETE:
- Removes records from the table
- Requires record key to identify which records to delete
- Two types:
  - Soft delete: Set all non-key fields to null (record still exists)
  - Hard delete: Remove record entirely

Spark UI: 2-3 jobs (index lookup + write delete markers + commit)

# Hard delete
hudi_options["hoodie.datasource.write.operation"] = "delete"

df_to_delete.write.format("hudi") \\
    .options(**hudi_options) \\
    .mode("append") \\
    .save("/shared/hudi/orders")

# Soft delete (set payload to empty)
hudi_options["hoodie.datasource.write.operation"] = "upsert"
hudi_options["hoodie.datasource.write.payload.class"] = \\
    "org.apache.hudi.common.model.EmptyHoodieRecordPayload"
"""

# Records to delete
delete_data = [
    ("ORD005", "user_4", "monitor", 600.00, "2024-01-02", "2024-01-03 14:00:00"),
]

df_delete = spark.createDataFrame(delete_data,
    ["order_id", "user_id", "product", "amount", "date", "updated_at"])

print("\n=== 4. DELETE (Remove ORD005) ===")
df_delete.select("order_id", "product").show()

# ============ 5. INSERT OVERWRITE ============
"""
INSERT_OVERWRITE:
- Replaces ALL data in specified partitions
- Like Spark's dynamic partition overwrite but with Hudi features
- Use for: Full partition refresh, reprocessing a day's data

hudi_options["hoodie.datasource.write.operation"] = "insert_overwrite"

# This replaces ALL data in date=2024-01-01 partition
df_replacement.write.format("hudi") \\
    .options(**hudi_options) \\
    .mode("append") \\
    .save("/shared/hudi/orders")
"""

print("\n=== 5. INSERT_OVERWRITE (Replace partition) ===")
print("Replaces entire partition with new data (like dynamic partition overwrite)")

# ============ WRITE OPERATION COMPARISON ============
"""
┌─────────────────┬───────────┬───────────┬──────────────┬──────────────────────┐
│ Operation       │ Index?    │ Speed     │ Duplicates?  │ Use Case             │
├─────────────────┼───────────┼───────────┼──────────────┼──────────────────────┤
│ bulk_insert     │ No        │ Fastest   │ Possible     │ Initial load         │
│ insert          │ No        │ Fast      │ Possible     │ Append-only data     │
│ upsert          │ Yes       │ Medium    │ No (deduped) │ CDC, updates         │
│ delete          │ Yes       │ Medium    │ N/A          │ GDPR, corrections    │
│ insert_overwrite│ No        │ Fast      │ No (replace) │ Partition refresh    │
└─────────────────┴───────────┴───────────┴──────────────┴──────────────────────┘
"""

# ============ PRECOMBINE FIELD BEHAVIOR ============
"""
When two records have the same record key, precombine field decides winner:

Example: order_id = "ORD001"
  Record A: updated_at = "2024-01-01 10:00:00", amount = 1200
  Record B: updated_at = "2024-01-03 12:00:00", amount = 1100

  Winner: Record B (higher precombine value = more recent)

This happens:
1. Within a single batch (dedup within incoming data)
2. Between incoming data and existing table (upsert resolution)

Common precombine fields:
- updated_at / modified_at (timestamp)
- version (integer)
- event_time (timestamp)
- _hoodie_commit_time (auto-generated)
"""

# Write demo data
df_initial.write.mode("overwrite").parquet("/shared/hudi_demo/write_operations")

spark.stop()
