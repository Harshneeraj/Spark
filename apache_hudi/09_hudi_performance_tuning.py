"""
Topic: Hudi Performance Tuning and Best Practices
===================================================

Optimizing Hudi for production workloads.

Key Interview Points:
- Index choice is #1 performance lever for writes.
- File sizing affects both read and write performance.
- Compaction strategy impacts MoR read latency.
- Metadata table speeds up file listing on cloud storage.
- Clustering improves read performance via data layout.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("09_Hudi_Performance_Tuning") \
    .master("local[*]") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

# ============ WRITE PERFORMANCE TUNING ============
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    WRITE PERFORMANCE OPTIMIZATION                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  1. INDEX SELECTION (biggest impact!)                                        ║
║  ─────────────────────────────────────                                       ║
║  • Small table (<10GB): BLOOM (default, good enough)                        ║
║  • Large table (>100GB): BUCKET (O(1), no lookup)                           ║
║  • Records move partitions: RECORD_INDEX or GLOBAL_BLOOM                    ║
║  • New tables (Hudi 0.14+): RECORD_INDEX (best overall)                    ║
║                                                                              ║
║  2. FILE SIZING                                                              ║
║  ─────────────────                                                           ║
║  • Target file size: 128MB - 256MB (Parquet sweet spot)                     ║
║  • Too small: Many files, slow listing, scheduling overhead                 ║
║  • Too large: Slow rewrites on update (CoW), memory pressure               ║
║  • hoodie.parquet.max.file.size = 134217728 (128MB)                         ║
║  • hoodie.parquet.small.file.limit = 104857600 (100MB)                      ║
║                                                                              ║
║  3. PARALLELISM                                                              ║
║  ─────────────────                                                           ║
║  • hoodie.insert.shuffle.parallelism = 200                                  ║
║  • hoodie.upsert.shuffle.parallelism = 200                                  ║
║  • hoodie.bulkinsert.shuffle.parallelism = 200                              ║
║  • Set based on data volume and cluster size                                ║
║  • Rule: 1 partition per 128MB of input data                                ║
║                                                                              ║
║  4. TABLE TYPE CHOICE                                                        ║
║  ─────────────────────                                                       ║
║  • Read-heavy, batch ETL: COPY_ON_WRITE                                     ║
║  • Write-heavy, streaming: MERGE_ON_READ                                    ║
║  • MoR writes are 5-10x faster than CoW (append-only logs)                 ║
║                                                                              ║
║  5. BULK INSERT FOR INITIAL LOAD                                             ║
║  ─────────────────────────────────                                           ║
║  • Always use bulk_insert for first load (no index overhead)                ║
║  • Sort by record key for optimal bloom filter effectiveness                ║
║  • hoodie.bulkinsert.sort.mode = GLOBAL_SORT                                ║
║                                                                              ║
║  6. DISABLE UNNECESSARY FEATURES DURING WRITE                                ║
║  ─────────────────────────────────────────────                               ║
║  • hoodie.metadata.enable = false (for initial load, enable after)          ║
║  • hoodie.clean.automatic = false (clean separately)                        ║
║  • hoodie.archive.automatic = false (archive separately)                    ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

# ============ READ PERFORMANCE TUNING ============
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    READ PERFORMANCE OPTIMIZATION                             ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  1. METADATA TABLE (critical for cloud storage!)                             ║
║  ─────────────────────────────────────────────────                           ║
║  • hoodie.metadata.enable = true                                            ║
║  • Avoids expensive S3/GCS file listing operations                          ║
║  • Stores: file listings, column stats, bloom filters                       ║
║  • Can speed up queries 2-10x on cloud storage                              ║
║  • hoodie.metadata.index.column.stats.enable = true                         ║
║                                                                              ║
║  2. DATA SKIPPING (column stats)                                             ║
║  ─────────────────────────────────                                           ║
║  • Uses min/max stats to skip files that don't match filter                 ║
║  • hoodie.metadata.index.column.stats.enable = true                         ║
║  • hoodie.enable.data.skipping = true                                       ║
║  • Works best with clustered/sorted data                                    ║
║                                                                              ║
║  3. CLUSTERING (data layout optimization)                                    ║
║  ─────────────────────────────────────────                                   ║
║  • Sort data by frequently queried columns                                  ║
║  • Enables effective data skipping                                          ║
║  • hoodie.clustering.plan.strategy.sort.columns = city,date                 ║
║  • Run periodically (daily/weekly)                                          ║
║                                                                              ║
║  4. PARTITION PRUNING                                                        ║
║  ─────────────────────                                                       ║
║  • Partition by frequently filtered columns                                 ║
║  • Query with partition filter: WHERE date = '2024-01-01'                   ║
║  • Skips entire partitions (directories) not matching filter                ║
║                                                                              ║
║  5. COMPACTION (MoR read performance)                                        ║
║  ─────────────────────────────────────                                       ║
║  • More frequent compaction = faster reads (fewer logs to merge)            ║
║  • Trade-off: Compaction uses resources                                     ║
║  • For read-heavy MoR: Compact every 2-3 commits                           ║
║  • For write-heavy MoR: Compact every 10+ commits                          ║
║                                                                              ║
║  6. READ-OPTIMIZED QUERIES (MoR)                                             ║
║  ─────────────────────────────────                                           ║
║  • Use read_optimized query type for fastest reads (stale OK)               ║
║  • Only reads base Parquet files (no log merge)                             ║
║  • Good for dashboards where slight staleness is acceptable                 ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

# ============ PRODUCTION CONFIGURATION TEMPLATES ============
"""
TEMPLATE 1: Streaming Ingestion (MoR, high throughput)
───────────────────────────────────────────────────────
hudi_options = {
    "hoodie.table.name": "events",
    "hoodie.datasource.write.table.type": "MERGE_ON_READ",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.recordkey.field": "event_id",
    "hoodie.datasource.write.precombine.field": "event_time",
    "hoodie.datasource.write.partitionpath.field": "date",
    
    # Index: Bucket for large tables
    "hoodie.index.type": "BUCKET",
    "hoodie.bucket.index.num.buckets": "256",
    
    # File sizing
    "hoodie.parquet.max.file.size": str(128 * 1024 * 1024),
    "hoodie.logfile.max.size": str(128 * 1024 * 1024),
    
    # Compaction (async)
    "hoodie.compact.inline": "false",
    "hoodie.compact.schedule.inline": "true",
    "hoodie.compact.inline.max.delta.commits": "3",
    
    # Cleaning
    "hoodie.clean.automatic": "true",
    "hoodie.cleaner.policy": "KEEP_LATEST_COMMITS",
    "hoodie.cleaner.commits.retained": "10",
    
    # Metadata
    "hoodie.metadata.enable": "true",
    
    # Parallelism
    "hoodie.upsert.shuffle.parallelism": "200",
}


TEMPLATE 2: Batch ETL (CoW, read-optimized)
─────────────────────────────────────────────
hudi_options = {
    "hoodie.table.name": "dim_customers",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.recordkey.field": "customer_id",
    "hoodie.datasource.write.precombine.field": "updated_at",
    "hoodie.datasource.write.partitionpath.field": "country",
    
    # Index: Bloom for medium tables
    "hoodie.index.type": "BLOOM",
    "hoodie.bloom.filter.num.entries": "60000",
    "hoodie.bloom.filter.fpp": "0.000000001",
    
    # File sizing
    "hoodie.parquet.max.file.size": str(256 * 1024 * 1024),
    "hoodie.parquet.small.file.limit": str(100 * 1024 * 1024),
    
    # Cleaning
    "hoodie.clean.automatic": "true",
    "hoodie.cleaner.policy": "KEEP_LATEST_BY_HOURS",
    "hoodie.cleaner.hours.retained": "168",  # 7 days
    
    # Metadata + data skipping
    "hoodie.metadata.enable": "true",
    "hoodie.metadata.index.column.stats.enable": "true",
    "hoodie.enable.data.skipping": "true",
    
    # Clustering (sort by query columns)
    "hoodie.clustering.inline": "true",
    "hoodie.clustering.inline.max.commits": "4",
    "hoodie.clustering.plan.strategy.sort.columns": "country,city",
    
    # Parallelism
    "hoodie.upsert.shuffle.parallelism": "100",
}


TEMPLATE 3: Initial Bulk Load
──────────────────────────────
hudi_options = {
    "hoodie.table.name": "fact_orders",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "bulk_insert",
    "hoodie.datasource.write.recordkey.field": "order_id",
    "hoodie.datasource.write.precombine.field": "updated_at",
    "hoodie.datasource.write.partitionpath.field": "order_date",
    
    # Bulk insert optimizations
    "hoodie.bulkinsert.shuffle.parallelism": "400",
    "hoodie.bulkinsert.sort.mode": "GLOBAL_SORT",
    
    # Disable features not needed for initial load
    "hoodie.metadata.enable": "false",  # Enable after load
    "hoodie.clean.automatic": "false",
    "hoodie.archive.automatic": "false",
    
    # Large file size for bulk
    "hoodie.parquet.max.file.size": str(256 * 1024 * 1024),
}
"""

# ============ COMMON PERFORMANCE ISSUES ============
"""
ISSUE 1: Slow upserts on large table
─────────────────────────────────────
Cause: Bloom index scanning too many files (false positives)
Fix: Switch to BUCKET index or RECORD_INDEX
Monitor: Check "index lookup time" in Hudi metrics

ISSUE 2: Slow reads on MoR table
──────────────────────────────────
Cause: Too many uncompacted log files
Fix: Increase compaction frequency or run manual compaction
Monitor: Number of log files per file group

ISSUE 3: Small file problem
─────────────────────────────
Cause: Many small writes creating tiny files
Fix: 
  - Increase hoodie.parquet.small.file.limit
  - Enable clustering
  - Batch more data before writing
Monitor: Average file size in table

ISSUE 4: Slow file listing on S3
──────────────────────────────────
Cause: S3 LIST operations are slow for many files
Fix: Enable metadata table (hoodie.metadata.enable = true)
Monitor: Time spent in file listing vs actual processing

ISSUE 5: OOM during upsert
────────────────────────────
Cause: Index loading too much data into memory
Fix:
  - Switch to BUCKET index (no memory for index)
  - Increase executor memory
  - Increase parallelism (less data per task)
Monitor: Executor memory usage, GC time
"""

print("=== Performance Tuning Summary ===")
print("""
WRITE optimization:
  1. Choose right index (BUCKET for large, BLOOM for medium)
  2. Use MoR for write-heavy workloads
  3. Use bulk_insert for initial loads
  4. Tune parallelism based on data volume

READ optimization:
  1. Enable metadata table (critical for cloud storage)
  2. Enable data skipping with column stats
  3. Cluster data by query columns
  4. Compact MoR tables frequently
  5. Use read-optimized queries when staleness is OK
""")

spark.stop()
