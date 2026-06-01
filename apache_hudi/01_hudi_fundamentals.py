"""
Topic: Apache Hudi Fundamentals
=================================

Apache Hudi (Hadoop Upserts Deletes and Incrementals) is a data lakehouse
storage layer that brings database-like capabilities to data lakes.

Spark UI Behavior:
- Hudi write operations trigger multiple jobs (index lookup + write + commit).
- Upsert: 2-3 jobs (read index, tag records, write + commit).
- Insert: 1-2 jobs (write + commit).
- Read: Same as reading Parquet (1 job, stages depend on query).
- Compaction (MoR): Separate job that merges log files into base files.

Key Interview Points:
- Hudi provides ACID transactions on data lakes (S3, HDFS, GCS).
- Supports Upsert, Delete, Insert operations (like a database).
- Two table types: Copy-on-Write (CoW) and Merge-on-Read (MoR).
- Time Travel: Query data as of any point in time.
- Incremental Queries: Read only changed data since a timestamp.
- Schema Evolution: Add/remove/rename columns without rewriting.
- Built-in indexing for fast record-level operations.
- Automatic file management (compaction, cleaning, clustering).

Architecture:
┌─────────────────────────────────────────────────────────────────────┐
│                        HUDI TABLE                                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  Timeline (metadata about all operations)                            │
│  ┌──────┬──────┬──────┬──────┬──────┐                              │
│  │commit│commit│commit│delta │clean │                              │
│  │  001 │  002 │  003 │commit│  005 │                              │
│  └──────┴──────┴──────┴──────┴──────┘                              │
│                                                                       │
│  File Groups (data organized by partition + file group)              │
│  ┌─────────────────────────────────────────────────────────┐        │
│  │ Partition: date=2024-01-01                                │        │
│  │  ├── FileGroup 1: [base_file.parquet] + [log1, log2]    │        │
│  │  ├── FileGroup 2: [base_file.parquet] + [log1]          │        │
│  │  └── FileGroup 3: [base_file.parquet]                    │        │
│  ├─────────────────────────────────────────────────────────┤        │
│  │ Partition: date=2024-01-02                                │        │
│  │  ├── FileGroup 1: [base_file.parquet]                    │        │
│  │  └── FileGroup 2: [base_file.parquet] + [log1]          │        │
│  └─────────────────────────────────────────────────────────┘        │
│                                                                       │
│  Metadata Table (file listings, column stats, bloom filters)         │
│                                                                       │
└─────────────────────────────────────────────────────────────────────┘
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp

# ============ SPARK SESSION WITH HUDI ============
"""
# Production setup (requires hudi-spark bundle)
spark = SparkSession.builder \\
    .appName("01_Hudi_Fundamentals") \\
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \\
    .config("spark.sql.catalog.spark_catalog", 
            "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \\
    .config("spark.sql.extensions", 
            "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \\
    .config("spark.jars.packages", 
            "org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.1") \\
    .getOrCreate()

# spark-submit command:
# spark-submit --packages org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.1 \\
#     --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \\
#     --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \\
#     my_hudi_app.py
"""

spark = SparkSession.builder \
    .appName("01_Hudi_Fundamentals") \
    .master("local[*]") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# ============ HUDI vs OTHER LAKEHOUSE FORMATS ============
"""
┌─────────────────────┬──────────────┬──────────────┬──────────────┐
│ Feature             │ Apache Hudi  │ Delta Lake   │ Apache Iceberg│
├─────────────────────┼──────────────┼──────────────┼──────────────┤
│ ACID Transactions   │ ✓            │ ✓            │ ✓            │
│ Upsert/Delete       │ ✓ (native)   │ ✓ (MERGE)    │ ✓ (MERGE)    │
│ Time Travel         │ ✓            │ ✓            │ ✓            │
│ Incremental Queries │ ✓ (native!)  │ ✓ (CDF)      │ ✓ (snapshots)│
│ Schema Evolution    │ ✓            │ ✓            │ ✓            │
│ Compaction          │ ✓ (built-in) │ ✓ (OPTIMIZE) │ ✓            │
│ Clustering          │ ✓            │ ✓ (Z-ORDER)  │ ✓ (sort)     │
│ Record-level Index  │ ✓ (native!)  │ ✗            │ ✗            │
│ Streaming Ingestion │ ✓ (native!)  │ ✓            │ ✓            │
│ CDC Support         │ ✓ (native!)  │ ✓ (CDF)      │ ✓            │
│ Multi-engine        │ Spark,Flink  │ Spark        │ Spark,Flink, │
│                     │ Presto,Trino │              │ Presto,Trino │
├─────────────────────┼──────────────┼──────────────┼──────────────┤
│ Best For            │ Streaming    │ Batch ETL    │ Analytics    │
│                     │ CDC, Upserts │ Spark-native │ Multi-engine │
└─────────────────────┴──────────────┴──────────────┴──────────────┘

HUDI's UNIQUE STRENGTHS:
1. Record-level indexing (fast upserts without full scan)
2. Native incremental query support (read only changes)
3. Built-in CDC (Change Data Capture) support
4. Near real-time ingestion (streaming-first design)
5. Flexible table types (CoW vs MoR trade-offs)
"""

# ============ HUDI TABLE TYPES ============
"""
┌─────────────────────────────────────────────────────────────────────┐
│                    COPY-ON-WRITE (CoW)                                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  Write: Rewrites ENTIRE file on every update                         │
│  Read: Simple Parquet read (fast!)                                   │
│                                                                       │
│  File Layout:                                                         │
│  commit_001: [file_1_v1.parquet] [file_2_v1.parquet]                │
│  commit_002: [file_1_v2.parquet] [file_2_v1.parquet]  ← file_1 rewritten│
│  commit_003: [file_1_v2.parquet] [file_2_v2.parquet]  ← file_2 rewritten│
│                                                                       │
│  Pros: Fast reads (just Parquet), simple, no compaction needed       │
│  Cons: Slow writes (rewrite entire file for 1 record change)        │
│  Best for: Read-heavy workloads, batch ETL, analytics                │
│                                                                       │
├─────────────────────────────────────────────────────────────────────┤
│                    MERGE-ON-READ (MoR)                                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  Write: Appends to LOG files (fast!)                                 │
│  Read: Merges base file + log files at query time                    │
│                                                                       │
│  File Layout:                                                         │
│  commit_001: [base_file.parquet]                                     │
│  commit_002: [base_file.parquet] + [log_1.avro]  ← log appended     │
│  commit_003: [base_file.parquet] + [log_1.avro] + [log_2.avro]      │
│  compaction: [new_base_file.parquet]  ← logs merged into base        │
│                                                                       │
│  Pros: Fast writes (append-only logs), good for streaming            │
│  Cons: Slower reads (merge at query time), needs compaction          │
│  Best for: Write-heavy, streaming ingestion, near real-time          │
│                                                                       │
│  Two query types for MoR:                                            │
│  - Snapshot query: Merges base + logs (latest data, slower)          │
│  - Read-optimized query: Only base files (stale but fast)            │
│                                                                       │
└─────────────────────────────────────────────────────────────────────┘
"""

# ============ HUDI RECORD KEY AND PRECOMBINE ============
"""
Every Hudi table requires:

1. RECORD KEY (hoodie.datasource.write.recordkey.field):
   - Uniquely identifies a record (like primary key)
   - Used for upsert/delete operations
   - Can be composite: "field1,field2"
   - Example: order_id, user_id, event_id

2. PRECOMBINE FIELD (hoodie.datasource.write.precombine.field):
   - Used to resolve duplicates (pick latest)
   - Higher value wins (usually timestamp or version)
   - Example: updated_at, version, event_time

3. PARTITION PATH (hoodie.datasource.write.partitionpath.field):
   - Optional but recommended for large tables
   - Determines directory structure
   - Example: date, year/month, region
"""

# ============ BASIC HUDI WRITE (Simulated) ============

# Sample data for demonstration
data = [
    ("ORD001", "user_1", "laptop", 1200.00, "2024-01-01 10:00:00", "created"),
    ("ORD002", "user_2", "phone", 800.00, "2024-01-01 10:05:00", "created"),
    ("ORD003", "user_3", "tablet", 500.00, "2024-01-01 10:10:00", "created"),
    ("ORD004", "user_1", "headphones", 200.00, "2024-01-01 10:15:00", "created"),
    ("ORD005", "user_4", "monitor", 600.00, "2024-01-01 10:20:00", "created"),
]

df = spark.createDataFrame(data, 
    ["order_id", "user_id", "product", "amount", "event_time", "status"])

print("=== Sample Data for Hudi ===")
df.show()

"""
# ACTUAL HUDI WRITE (requires hudi-spark bundle):

# Insert
df.write.format("hudi") \\
    .options(**{
        "hoodie.table.name": "orders",
        "hoodie.datasource.write.recordkey.field": "order_id",
        "hoodie.datasource.write.precombine.field": "event_time",
        "hoodie.datasource.write.partitionpath.field": "status",
        "hoodie.datasource.write.operation": "insert",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    }) \\
    .mode("overwrite") \\
    .save("/shared/hudi/orders")

# Read
df_hudi = spark.read.format("hudi").load("/shared/hudi/orders")
df_hudi.show()
"""

# Write as parquet for demo (simulating Hudi behavior)
df.write.mode("overwrite").parquet("/shared/hudi_demo/orders_initial")
print("Demo data written to /shared/hudi_demo/orders_initial")

spark.stop()
