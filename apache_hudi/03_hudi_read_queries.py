"""
Topic: Hudi Read/Query Types - Snapshot, Incremental, Time Travel
==================================================================

Hudi supports multiple query types for different use cases.

Spark UI Behavior:
- Snapshot query: Same as reading Parquet (1 job, standard stages).
- Incremental query: Reads only changed files since a commit (fewer tasks).
- Time travel: Reads specific version of files (same as snapshot but older).
- MoR snapshot: Additional merge step (base + logs) adds processing time.
- MoR read-optimized: Same as CoW read (just base Parquet files).

Key Interview Points:
- Snapshot query: Latest state of all records (default).
- Incremental query: Only records changed since a given commit (CDC!).
- Time travel: Query data as it was at a specific point in time.
- Read-optimized (MoR only): Read only base files (fast but stale).
- Point-in-time: Reconstruct table state at exact commit instant.
- Incremental queries are Hudi's UNIQUE advantage over Delta/Iceberg.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder \
    .appName("03_Hudi_Read_Queries") \
    .master("local[*]") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# ============ 1. SNAPSHOT QUERY (Default) ============
"""
Returns the LATEST state of all records in the table.
This is the default query type.

For CoW: Reads latest version of Parquet files.
For MoR: Merges base Parquet files with log files (slower).

# Read latest snapshot
df = spark.read.format("hudi").load("/shared/hudi/orders")
df.show()

# Or with SQL
spark.sql("SELECT * FROM hudi_orders")

# Equivalent explicit option:
df = spark.read.format("hudi") \\
    .option("hoodie.datasource.query.type", "snapshot") \\
    .load("/shared/hudi/orders")
"""

print("=== 1. SNAPSHOT QUERY ===")
print("""
spark.read.format("hudi").load("/path/to/hudi/table")

Returns: Latest state of ALL records.
Use for: Regular analytics, dashboards, reports.
Performance: Fast for CoW, slower for MoR (merge needed).
""")

# ============ 2. INCREMENTAL QUERY (Hudi's Killer Feature!) ============
"""
Returns ONLY records that changed since a given commit timestamp.
This is like CDC (Change Data Capture) built into the storage layer!

# Read only changes since commit "20240101100000"
df_incremental = spark.read.format("hudi") \\
    .option("hoodie.datasource.query.type", "incremental") \\
    .option("hoodie.datasource.read.begin.instanttime", "20240101100000") \\
    .option("hoodie.datasource.read.end.instanttime", "20240103120000") \\
    .load("/shared/hudi/orders")

# This returns ONLY records that were inserted/updated/deleted
# between the two timestamps!

USE CASES:
1. Incremental ETL: Process only new/changed data (not full table scan!)
2. CDC pipelines: Capture changes and propagate downstream
3. Streaming sinks: Feed changes to Kafka/downstream systems
4. Efficient joins: Only join changed records with dimension tables

HUGE PERFORMANCE BENEFIT:
- Full table: 1 billion records, scan takes 30 minutes
- Incremental: 10,000 changed records, scan takes 5 seconds!
"""

print("=== 2. INCREMENTAL QUERY (CDC!) ===")
print("""
spark.read.format("hudi")
    .option("hoodie.datasource.query.type", "incremental")
    .option("hoodie.datasource.read.begin.instanttime", "20240101100000")
    .load("/path/to/hudi/table")

Returns: Only records changed since the given timestamp.
Use for: Incremental ETL, CDC, streaming pipelines.
Performance: MUCH faster than full scan (reads only changed files).

This is Hudi's BIGGEST advantage over plain Parquet/Delta!
""")

# ============ 3. TIME TRAVEL QUERY ============
"""
Query the table as it existed at a specific point in time.
Useful for auditing, debugging, reproducing results.

# Method 1: Using instanttime option
df_historical = spark.read.format("hudi") \\
    .option("as.of.instant", "20240101100000") \\
    .load("/shared/hudi/orders")

# Method 2: Using SQL
spark.sql("SELECT * FROM orders TIMESTAMP AS OF '2024-01-01 10:00:00'")

# Method 3: Using specific commit
df_at_commit = spark.read.format("hudi") \\
    .option("as.of.instant", "20240101100000") \\
    .load("/shared/hudi/orders")

USE CASES:
1. Auditing: "What did the data look like last Tuesday?"
2. Debugging: "When did this record change?"
3. Reproducibility: "Run the same report as last month"
4. Rollback: "Restore data to previous state"
"""

print("=== 3. TIME TRAVEL QUERY ===")
print("""
spark.read.format("hudi")
    .option("as.of.instant", "20240101100000")
    .load("/path/to/hudi/table")

Returns: Table state at that specific point in time.
Use for: Auditing, debugging, reproducibility.
""")

# ============ 4. READ-OPTIMIZED QUERY (MoR Only) ============
"""
For Merge-on-Read tables only.
Reads ONLY base Parquet files (ignores log files).
Faster but may return STALE data (logs not merged yet).

df_read_opt = spark.read.format("hudi") \\
    .option("hoodie.datasource.query.type", "read_optimized") \\
    .load("/shared/hudi/orders_mor")

TRADE-OFF:
- Snapshot query (MoR): Latest data, slower (merges logs)
- Read-optimized (MoR): Stale data, faster (skips logs)

After compaction runs, read-optimized catches up to snapshot.
"""

print("=== 4. READ-OPTIMIZED QUERY (MoR only) ===")
print("""
spark.read.format("hudi")
    .option("hoodie.datasource.query.type", "read_optimized")
    .load("/path/to/hudi/mor_table")

Returns: Data from base files only (may be stale).
Use for: Fast reads where slight staleness is acceptable.
Only applicable to Merge-on-Read tables.
""")

# ============ 5. POINT-IN-TIME QUERY ============
"""
Reconstruct table state at an exact commit instant.
Different from time travel: uses commit ID, not wall-clock time.

# List all commits on the timeline
spark.read.format("hudi") \\
    .load("/shared/hudi/orders") \\
    .select("_hoodie_commit_time").distinct().show()

# Query at specific commit
df_at_commit = spark.read.format("hudi") \\
    .option("as.of.instant", "20240103120000") \\
    .load("/shared/hudi/orders")
"""

# ============ HUDI METADATA COLUMNS ============
"""
Every Hudi table has these hidden metadata columns:

┌──────────────────────────┬─────────────────────────────────────────────┐
│ Column                   │ Description                                 │
├──────────────────────────┼─────────────────────────────────────────────┤
│ _hoodie_commit_time      │ Commit timestamp (when record was written)  │
│ _hoodie_commit_seqno     │ Sequence number within commit               │
│ _hoodie_record_key       │ Record key value                            │
│ _hoodie_partition_path   │ Partition path                              │
│ _hoodie_file_name        │ File containing this record                 │
└──────────────────────────┴─────────────────────────────────────────────┘

# Access metadata columns:
df = spark.read.format("hudi").load("/shared/hudi/orders")
df.select("_hoodie_commit_time", "_hoodie_record_key", "order_id", "amount").show()

# Useful for:
- Tracking when records were last modified
- Debugging data lineage
- Building incremental pipelines manually
"""

# ============ QUERY TYPE COMPARISON ============
"""
┌─────────────────────┬──────────────────┬──────────────────┬──────────────────┐
│ Query Type          │ Data Freshness   │ Performance      │ Use Case         │
├─────────────────────┼──────────────────┼──────────────────┼──────────────────┤
│ Snapshot            │ Latest           │ Fast (CoW)       │ Analytics        │
│                     │                  │ Medium (MoR)     │ Dashboards       │
├─────────────────────┼──────────────────┼──────────────────┼──────────────────┤
│ Incremental         │ Changes only     │ Very Fast        │ CDC, ETL         │
│                     │                  │ (reads few files)│ Streaming        │
├─────────────────────┼──────────────────┼──────────────────┼──────────────────┤
│ Time Travel         │ Historical       │ Same as snapshot │ Auditing         │
│                     │                  │                  │ Debugging        │
├─────────────────────┼──────────────────┼──────────────────┼──────────────────┤
│ Read-Optimized      │ Slightly stale   │ Fastest (MoR)    │ Fast reads       │
│ (MoR only)          │ (pre-compaction) │                  │ Dashboards       │
└─────────────────────┴──────────────────┴──────────────────┴──────────────────┘
"""

# Simulate reading with demo data
data = [
    ("ORD001", "user_1", "laptop", 1100.00, "2024-01-01", "20240103120000"),
    ("ORD002", "user_2", "phone", 800.00, "2024-01-01", "20240101100500"),
    ("ORD003", "user_3", "tablet", 400.00, "2024-01-01", "20240103120500"),
    ("ORD004", "user_1", "headphones", 200.00, "2024-01-02", "20240102090000"),
    ("ORD006", "user_5", "keyboard", 150.00, "2024-01-03", "20240103080000"),
    ("ORD007", "user_2", "mouse", 50.00, "2024-01-03", "20240103083000"),
    ("ORD008", "user_6", "camera", 900.00, "2024-01-03", "20240103121000"),
]

df_snapshot = spark.createDataFrame(data,
    ["order_id", "user_id", "product", "amount", "date", "_hoodie_commit_time"])

print("\n=== Simulated Snapshot Query Result ===")
df_snapshot.show()

# Simulate incremental query (only changes after 20240103000000)
df_incremental = df_snapshot.filter(col("_hoodie_commit_time") > "20240103000000")
print("=== Simulated Incremental Query (changes after 2024-01-03) ===")
df_incremental.show()

# Write demo
df_snapshot.write.mode("overwrite").parquet("/shared/hudi_demo/read_queries")

spark.stop()
