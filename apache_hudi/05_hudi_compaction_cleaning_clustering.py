"""
Topic: Compaction, Cleaning, and Clustering - Table Management
===============================================================

Hudi's built-in table maintenance operations.

Spark UI Behavior:
- Compaction: Separate job(s) that merge log files into base files (MoR).
- Cleaning: Background job that removes old file versions.
- Clustering: Reorganizes data layout for better query performance.
- These can run inline (during write) or async (separate process).

Key Interview Points:
- Compaction: MoR only. Merges log files into new base Parquet files.
- Cleaning: Removes old file versions beyond retention period.
- Clustering: Reorganizes data for better read performance (like Z-ORDER).
- Inline vs Async: Inline runs during write (adds latency), async runs separately.
- Compaction is REQUIRED for MoR tables to keep read performance acceptable.
- Without cleaning: Storage grows unbounded (old versions kept forever).
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("05_Hudi_Compaction_Cleaning_Clustering") \
    .master("local[*]") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

# ============ 1. COMPACTION (MoR Tables Only) ============
"""
WHAT: Merges log files (deltas) into base Parquet files.
WHY: Without compaction, reads get slower (more logs to merge at query time).
WHEN: Periodically, based on strategy.

Before Compaction:
  FileGroup 1: [base_v1.parquet] + [log_1.avro] + [log_2.avro] + [log_3.avro]
  Read: Must merge base + 3 logs (SLOW)

After Compaction:
  FileGroup 1: [base_v2.parquet]  (all logs merged into new base)
  Read: Just read 1 Parquet file (FAST)

COMPACTION STRATEGIES:
┌─────────────────────────────┬─────────────────────────────────────────────┐
│ Strategy                    │ Description                                 │
├─────────────────────────────┼─────────────────────────────────────────────┤
│ num_commits (default)       │ Compact after N commits (default: 5)        │
│ time_elapsed                │ Compact after N seconds since last compaction│
│ num_and_time                │ Compact when either condition is met         │
│ num_or_time                 │ Compact when both conditions are met         │
└─────────────────────────────┴─────────────────────────────────────────────┘

CONFIGURATION:
# Inline compaction (runs during write - adds latency)
hoodie.compact.inline = true
hoodie.compact.inline.max.delta.commits = 5

# Async compaction (runs separately - recommended for production)
hoodie.compact.inline = false
# Then run compaction as a separate job:
# spark-submit --class org.apache.hudi.utilities.HoodieCompactor ...

# Schedule compaction (creates compaction plan without executing)
hoodie.compact.schedule.inline = true
hoodie.compact.inline = false
# Then execute plans asynchronously

INLINE vs ASYNC:
┌──────────────┬──────────────────────────────────────────────────────────┐
│ Mode         │ Behavior                                                 │
├──────────────┼──────────────────────────────────────────────────────────┤
│ Inline       │ Compaction runs DURING write operation.                  │
│              │ Pros: Simple, no separate job needed.                    │
│              │ Cons: Adds latency to writes, blocks ingestion.          │
│              │ Use for: Low-throughput, batch workloads.                │
├──────────────┼──────────────────────────────────────────────────────────┤
│ Async        │ Compaction runs as SEPARATE process.                     │
│              │ Pros: Doesn't block writes, better throughput.           │
│              │ Cons: More complex (separate job to manage).             │
│              │ Use for: High-throughput, streaming ingestion.           │
└──────────────┴──────────────────────────────────────────────────────────┘
"""

print("=== 1. COMPACTION ===")
print("""
Purpose: Merge log files into base Parquet (MoR only).
Without it: Reads get progressively slower.
Strategies: num_commits (default=5), time_elapsed, etc.
Modes: Inline (during write) or Async (separate job).
""")

# ============ 2. CLEANING ============
"""
WHAT: Removes old file versions that are no longer needed.
WHY: Without cleaning, storage grows forever (all old versions kept).
WHEN: After commits, based on retention policy.

Example:
  Commit 1: file_v1.parquet (100MB)
  Commit 2: file_v2.parquet (100MB) ← file_v1 is now "old"
  Commit 3: file_v3.parquet (100MB) ← file_v1, file_v2 are "old"
  
  Without cleaning: 300MB used (all versions kept)
  With cleaning (retain 1): 100MB used (only latest kept)

CLEANING POLICIES:
┌─────────────────────────────┬─────────────────────────────────────────────┐
│ Policy                      │ Description                                 │
├─────────────────────────────┼─────────────────────────────────────────────┤
│ KEEP_LATEST_COMMITS         │ Keep files from last N commits (default: 10)│
│ KEEP_LATEST_FILE_VERSIONS   │ Keep last N versions of each file           │
│ KEEP_LATEST_BY_HOURS        │ Keep files from last N hours                │
└─────────────────────────────┴─────────────────────────────────────────────┘

CONFIGURATION:
hoodie.clean.automatic = true  (default: true)
hoodie.cleaner.policy = KEEP_LATEST_COMMITS
hoodie.cleaner.commits.retained = 10  (keep last 10 commits)

# Or by file versions:
hoodie.cleaner.policy = KEEP_LATEST_FILE_VERSIONS
hoodie.cleaner.fileversions.retained = 3

# Or by time:
hoodie.cleaner.policy = KEEP_LATEST_BY_HOURS
hoodie.cleaner.hours.retained = 24  (keep last 24 hours)

IMPORTANT:
- Cleaning is IRREVERSIBLE (old versions are deleted!)
- Time travel only works within retention window
- Set retention based on your time travel/audit needs
- Cleaning runs automatically after each commit (if enabled)
"""

print("=== 2. CLEANING ===")
print("""
Purpose: Remove old file versions to reclaim storage.
Without it: Storage grows unbounded.
Policies: KEEP_LATEST_COMMITS (default=10), KEEP_LATEST_FILE_VERSIONS, KEEP_LATEST_BY_HOURS.
Note: Time travel only works within retention window!
""")

# ============ 3. CLUSTERING ============
"""
WHAT: Reorganizes data layout for better query performance.
WHY: Over time, data layout becomes suboptimal (small files, poor sorting).
WHEN: Periodically, as a maintenance operation.

CLUSTERING DOES:
1. Combines small files into larger ones (solves small file problem)
2. Re-sorts data by specified columns (better predicate pushdown)
3. Can change file sizes to target optimal size

Before Clustering:
  file_1.parquet (10MB, mixed cities)
  file_2.parquet (5MB, mixed cities)
  file_3.parquet (8MB, mixed cities)
  file_4.parquet (3MB, mixed cities)

After Clustering (sorted by city):
  file_1.parquet (128MB, cities A-M)
  file_2.parquet (128MB, cities N-Z)
  
  Query: WHERE city = 'New York' → Only reads file_1 (skip file_2!)

CLUSTERING STRATEGIES:
┌─────────────────────────────┬─────────────────────────────────────────────┐
│ Strategy                    │ Description                                 │
├─────────────────────────────┼─────────────────────────────────────────────┤
│ SparkSortAndSizeExecutionStrategy│ Sort by columns + target file size    │
│ SparkSizeBasedClusteringExecutionStrategy│ Only resize, no sorting       │
└─────────────────────────────┴─────────────────────────────────────────────┘

CONFIGURATION:
# Enable inline clustering
hoodie.clustering.inline = true
hoodie.clustering.inline.max.commits = 4

# Clustering plan strategy
hoodie.clustering.plan.strategy.class = 
    org.apache.hudi.client.clustering.plan.strategy.SparkSizeBasedClusteringPlanStrategy

# Execution strategy (sort + size)
hoodie.clustering.execution.strategy.class = 
    org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy

# Sort columns (for data skipping)
hoodie.clustering.plan.strategy.sort.columns = city,date

# Target file size
hoodie.clustering.plan.strategy.target.file.max.bytes = 134217728  (128MB)
hoodie.clustering.plan.strategy.small.file.limit = 104857600  (100MB)

CLUSTERING vs COMPACTION:
┌──────────────┬──────────────────────────────┬──────────────────────────────┐
│ Aspect       │ Compaction                   │ Clustering                   │
├──────────────┼──────────────────────────────┼──────────────────────────────┤
│ Purpose      │ Merge logs into base files   │ Reorganize data layout       │
│ Table Type   │ MoR only                     │ Both CoW and MoR             │
│ Trigger      │ After N commits/time         │ After N commits/time         │
│ Effect       │ Faster reads (fewer merges)  │ Faster reads (data skipping) │
│ Data Change  │ No (same data, new format)   │ No (same data, new layout)   │
│ Blocking     │ Non-blocking (MoR)           │ Can be blocking              │
└──────────────┴──────────────────────────────┴──────────────────────────────┘
"""

print("=== 3. CLUSTERING ===")
print("""
Purpose: Reorganize data for better query performance.
Does: Combines small files, re-sorts data by columns.
Benefit: Better predicate pushdown, fewer files to scan.
Works on: Both CoW and MoR tables.
Like: Delta Lake's OPTIMIZE + Z-ORDER.
""")

# ============ 4. ARCHIVAL ============
"""
WHAT: Archives old timeline metadata (commits, rollbacks, etc.)
WHY: Timeline metadata grows over time, slowing down operations.
WHEN: Automatically, based on retention.

CONFIGURATION:
hoodie.keep.min.commits = 20  (min commits to keep on active timeline)
hoodie.keep.max.commits = 30  (max commits before archiving)

Archived commits are moved to .hoodie/archived/ directory.
They can still be used for time travel but are not on active timeline.
"""

# ============ MAINTENANCE SCHEDULE RECOMMENDATION ============
"""
PRODUCTION MAINTENANCE SCHEDULE:

For Streaming Ingestion (MoR):
  - Compaction: Every 5 commits (inline schedule, async execute)
  - Cleaning: After each commit (retain last 24 hours)
  - Clustering: Every 4 hours (async, sort by query columns)

For Batch ETL (CoW):
  - Compaction: Not needed (CoW doesn't have logs)
  - Cleaning: After each commit (retain last 7 days)
  - Clustering: Daily (sort by frequently filtered columns)

For CDC Pipeline:
  - Compaction: Every 3 commits (low latency needed)
  - Cleaning: Retain last 48 hours (for reprocessing)
  - Clustering: Weekly (less critical for CDC)
"""

spark.stop()
