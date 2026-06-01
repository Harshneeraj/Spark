"""
Topic: Hudi Indexing - Fast Record Lookup for Upserts
======================================================

Indexing is how Hudi quickly finds which file contains a given record key.

Spark UI Behavior:
- Index lookup appears as an extra stage/job before the write.
- Bloom index: Reads Parquet footers (bloom filters) - fast, no extra files.
- Simple index: Reads all base files to find keys - slower but accurate.
- Bucket index: Hash-based, no lookup needed (O(1)) - fastest for writes.
- HBase index: External lookup to HBase - for very large tables.

Key Interview Points:
- Index answers: "Which file group contains record key X?"
- Without index: Must scan ALL files to find a record (slow!).
- Bloom index (default): Uses bloom filters in Parquet footer. Fast but false positives.
- Simple index: Loads all keys from base files. Accurate but slow for large tables.
- Bucket index: Hash(key) determines file group. No lookup, O(1). Best for large tables.
- Global vs Non-global: Global checks ALL partitions, non-global only target partition.
- Index choice is the #1 performance tuning knob for Hudi writes.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("04_Hudi_Indexing") \
    .master("local[*]") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

# ============ WHY INDEXING MATTERS ============
"""
PROBLEM: During UPSERT, Hudi needs to know:
  "Does record key 'ORD001' already exist? If yes, which file is it in?"

WITHOUT INDEX (brute force):
  Scan ALL files in ALL partitions -> Find record -> Update
  For 1TB table with 10,000 files: Read 10,000 file footers!
  Time: Minutes

WITH INDEX:
  Lookup index -> Get file location -> Update only that file
  Time: Seconds

INDEX FLOW:
  Incoming records → Index Lookup → Tag (INSERT or UPDATE) → Write
                         │
                         ▼
              ┌─────────────────────┐
              │ Record ORD001:      │
              │   Found in file_3   │
              │   → Tag as UPDATE   │
              │                     │
              │ Record ORD099:      │
              │   Not found         │
              │   → Tag as INSERT   │
              └─────────────────────┘
"""

# ============ INDEX TYPES ============
"""
┌─────────────────┬────────────────────────────────────────────────────────────┐
│ Index Type      │ Description                                                │
├─────────────────┼────────────────────────────────────────────────────────────┤
│                 │                                                            │
│ BLOOM           │ DEFAULT. Uses bloom filters stored in Parquet footer.      │
│ (default)       │ Bloom filter: Probabilistic data structure.                │
│                 │   - "Definitely NOT in file" (100% accurate)              │
│                 │   - "Might be in file" (false positives possible)          │
│                 │ On false positive: Opens file to verify (extra I/O).       │
│                 │                                                            │
│                 │ Pros: Fast, no external dependency, works well for         │
│                 │       partitioned data with known partition path.          │
│                 │ Cons: False positives cause extra reads. Doesn't work      │
│                 │       well when records move between partitions.           │
│                 │                                                            │
│                 │ Config:                                                    │
│                 │   hoodie.index.type = BLOOM                               │
│                 │   hoodie.bloom.filter.num.entries = 60000                  │
│                 │   hoodie.bloom.filter.fpp = 0.000000001                   │
│                 │                                                            │
├─────────────────┼────────────────────────────────────────────────────────────┤
│                 │                                                            │
│ SIMPLE          │ Loads ALL record keys from base files into memory.         │
│                 │ Joins incoming keys with loaded keys to find matches.      │
│                 │                                                            │
│                 │ Pros: 100% accurate (no false positives).                 │
│                 │ Cons: Slow for large tables (loads all keys).             │
│                 │       High memory usage.                                   │
│                 │                                                            │
│                 │ Config:                                                    │
│                 │   hoodie.index.type = SIMPLE                              │
│                 │                                                            │
├─────────────────┼────────────────────────────────────────────────────────────┤
│                 │                                                            │
│ BUCKET          │ Hash-based: hash(record_key) % num_buckets = file group.  │
│                 │ NO lookup needed! O(1) to determine file location.         │
│                 │                                                            │
│                 │ Pros: Fastest writes (no index lookup step).              │
│                 │       Consistent performance regardless of table size.     │
│                 │       Best for very large tables.                          │
│                 │ Cons: Fixed number of buckets (hard to change later).     │
│                 │       May have uneven distribution if keys are skewed.     │
│                 │                                                            │
│                 │ Config:                                                    │
│                 │   hoodie.index.type = BUCKET                              │
│                 │   hoodie.bucket.index.num.buckets = 256                   │
│                 │   hoodie.bucket.index.hash.field = order_id               │
│                 │                                                            │
├─────────────────┼────────────────────────────────────────────────────────────┤
│                 │                                                            │
│ GLOBAL_BLOOM    │ Same as BLOOM but searches ALL partitions.                │
│                 │ Use when: Record may move between partitions.             │
│                 │ Slower than BLOOM (checks all partitions).                │
│                 │                                                            │
│                 │ Config:                                                    │
│                 │   hoodie.index.type = GLOBAL_BLOOM                        │
│                 │                                                            │
├─────────────────┼────────────────────────────────────────────────────────────┤
│                 │                                                            │
│ GLOBAL_SIMPLE   │ Same as SIMPLE but searches ALL partitions.               │
│                 │ Most accurate but slowest.                                │
│                 │                                                            │
│                 │ Config:                                                    │
│                 │   hoodie.index.type = GLOBAL_SIMPLE                       │
│                 │                                                            │
├─────────────────┼────────────────────────────────────────────────────────────┤
│                 │                                                            │
│ RECORD_INDEX    │ Stores record-to-file mapping in Hudi metadata table.     │
│ (Hudi 0.14+)   │ Fast lookup, works globally, scales well.                 │
│                 │ Recommended for new tables.                                │
│                 │                                                            │
│                 │ Config:                                                    │
│                 │   hoodie.index.type = RECORD_INDEX                        │
│                 │   hoodie.metadata.enable = true                           │
│                 │                                                            │
└─────────────────┴────────────────────────────────────────────────────────────┘
"""

# ============ GLOBAL vs NON-GLOBAL INDEX ============
"""
NON-GLOBAL (BLOOM, SIMPLE, BUCKET):
  - Only looks in the TARGET partition for the record
  - Incoming record must specify correct partition path
  - If record moves between partitions: DUPLICATE created!
  - Faster (searches fewer files)

GLOBAL (GLOBAL_BLOOM, GLOBAL_SIMPLE, RECORD_INDEX):
  - Searches ALL partitions for the record
  - Handles records moving between partitions correctly
  - Slower (searches more files)
  - Required when: partition path can change for a record

Example:
  Record: order_id=ORD001, status=created → partition: status=created
  Update: order_id=ORD001, status=shipped → partition: status=shipped
  
  Non-global index: Creates DUPLICATE (doesn't find ORD001 in status=shipped)
  Global index: Finds ORD001 in status=created, deletes old, inserts in status=shipped
"""

# ============ CHOOSING THE RIGHT INDEX ============
"""
DECISION GUIDE:

1. Table size < 10GB, partitioned, records don't move partitions:
   → BLOOM (default, good enough)

2. Table size > 100GB, high write throughput needed:
   → BUCKET (no lookup overhead, O(1))

3. Records may move between partitions:
   → GLOBAL_BLOOM or RECORD_INDEX

4. Need 100% accuracy, small-medium table:
   → SIMPLE

5. New table, want best overall performance (Hudi 0.14+):
   → RECORD_INDEX (metadata-based, fast, global)

PERFORMANCE COMPARISON:
┌─────────────────┬──────────────┬──────────────┬──────────────┐
│ Index           │ Write Speed  │ Accuracy     │ Memory       │
├─────────────────┼──────────────┼──────────────┼──────────────┤
│ BUCKET          │ ★★★★★       │ ★★★★★       │ ★★★★★ (low) │
│ BLOOM           │ ★★★★        │ ★★★★        │ ★★★★        │
│ RECORD_INDEX    │ ★★★★        │ ★★★★★       │ ★★★★        │
│ SIMPLE          │ ★★           │ ★★★★★       │ ★★ (high)   │
│ GLOBAL_BLOOM    │ ★★★          │ ★★★★        │ ★★★         │
│ GLOBAL_SIMPLE   │ ★            │ ★★★★★       │ ★ (highest) │
└─────────────────┴──────────────┴──────────────┴──────────────┘
"""

# ============ BLOOM FILTER TUNING ============
"""
Bloom filter parameters:

hoodie.bloom.filter.num.entries = 60000 (default)
  Number of keys the bloom filter is sized for per file.
  Increase if files have more records.

hoodie.bloom.filter.fpp = 0.000000001 (default)
  False positive probability.
  Lower = more accurate but larger bloom filter.
  
hoodie.bloom.filter.dynamic.max.entries = 100000
  Max entries for dynamic bloom filter sizing.

hoodie.index.bloom.num.range.info.entries = 10000
  Number of range entries for bloom index pruning.

TUNING TIP:
  If you see many "false positive" lookups in Hudi metrics:
  → Increase num.entries or decrease fpp
  → Or switch to BUCKET/RECORD_INDEX
"""

print("=== Hudi Index Types Summary ===")
print("""
BLOOM (default): Bloom filters in Parquet footer. Good for most cases.
SIMPLE: Load all keys. Accurate but slow for large tables.
BUCKET: Hash-based, O(1). Best for large tables, high throughput.
RECORD_INDEX: Metadata-based. Best overall (Hudi 0.14+).
GLOBAL_*: Search all partitions. Use when records move partitions.
""")

spark.stop()
