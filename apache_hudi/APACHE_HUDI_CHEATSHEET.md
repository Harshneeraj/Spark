# Apache Hudi Complete Cheatsheet - Interview Ready

## Table of Contents
- [1. What is Hudi](#1-what-is-hudi)
- [2. Table Types (CoW vs MoR)](#2-table-types-cow-vs-mor)
- [3. Write Operations](#3-write-operations)
- [4. Read/Query Types](#4-readquery-types)
- [5. Indexing](#5-indexing)
- [6. Schema Evolution](#6-schema-evolution)
- [7. Compaction, Cleaning, Clustering](#7-compaction-cleaning-clustering)
- [8. CDC and Incremental Pipelines](#8-cdc-and-incremental-pipelines)
- [9. Concurrency and Timeline](#9-concurrency-and-timeline)
- [10. Partitioning](#10-partitioning)
- [11. Configuration Reference](#11-configuration-reference)
- [12. Performance Tuning](#12-performance-tuning)
- [13. Hudi vs Delta vs Iceberg](#13-hudi-vs-delta-vs-iceberg)
- [14. Interview Questions](#14-interview-questions)

---

## 1. What is Hudi

Apache Hudi = **H**adoop **U**pserts **D**eletes and **I**ncrementals

A data lakehouse storage layer that brings database-like capabilities to data lakes.

**Core Features:**
- ACID transactions on data lakes (S3, HDFS, GCS)
- Upsert, Delete, Insert operations
- Time Travel (query historical data)
- Incremental Queries (read only changes)
- Schema Evolution
- Built-in indexing for fast record-level operations
- Automatic table management (compaction, cleaning, clustering)

---

## 2. Table Types (CoW vs MoR)

| Aspect | Copy-on-Write (CoW) | Merge-on-Read (MoR) |
|--------|---------------------|---------------------|
| **Write** | Rewrites entire file | Appends to log files |
| **Read** | Fast (just Parquet) | Slower (merge base + logs) |
| **Write Speed** | Slow | Fast (5-10x faster) |
| **Read Speed** | Fast | Depends on compaction |
| **Compaction** | Not needed | Required periodically |
| **Best For** | Read-heavy, batch ETL | Write-heavy, streaming |
| **File Layout** | Parquet files only | Parquet + Avro log files |

```
CoW: [file_v1.parquet] → update → [file_v2.parquet] (rewrite)
MoR: [base.parquet] + [log1.avro] + [log2.avro] → compact → [new_base.parquet]
```

**Decision:** Use CoW for analytics/batch. Use MoR for streaming/CDC.

---

## 3. Write Operations

```python
# Common options for all writes
hudi_options = {
    "hoodie.table.name": "orders",
    "hoodie.datasource.write.recordkey.field": "order_id",
    "hoodie.datasource.write.precombine.field": "updated_at",
    "hoodie.datasource.write.partitionpath.field": "date",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
}
```

| Operation | Index? | Speed | Use Case |
|-----------|--------|-------|----------|
| `bulk_insert` | No | Fastest | Initial load, backfill |
| `insert` | No | Fast | Append-only (logs, events) |
| `upsert` | Yes | Medium | CDC, updates (most common!) |
| `delete` | Yes | Medium | GDPR, corrections |
| `insert_overwrite` | No | Fast | Full partition refresh |

```python
# UPSERT (most important!)
df.write.format("hudi") \
    .options(**{**hudi_options, "hoodie.datasource.write.operation": "upsert"}) \
    .mode("append") \
    .save("/shared/hudi/orders")

# BULK INSERT (initial load)
df.write.format("hudi") \
    .options(**{**hudi_options, "hoodie.datasource.write.operation": "bulk_insert"}) \
    .mode("overwrite") \
    .save("/shared/hudi/orders")

# DELETE
df_to_delete.write.format("hudi") \
    .options(**{**hudi_options, "hoodie.datasource.write.operation": "delete"}) \
    .mode("append") \
    .save("/shared/hudi/orders")
```

**Record Key + Precombine:**
- Record Key: Uniquely identifies a record (like primary key)
- Precombine: Resolves duplicates (higher value wins, usually timestamp)

---

## 4. Read/Query Types

| Query Type | Returns | Performance | Use Case |
|-----------|---------|-------------|----------|
| **Snapshot** | Latest state of all records | Fast (CoW), Medium (MoR) | Analytics, dashboards |
| **Incremental** | Only changed records since timestamp | Very fast | CDC, ETL pipelines |
| **Time Travel** | Table state at specific time | Same as snapshot | Auditing, debugging |
| **Read-Optimized** | Base files only (MoR) | Fastest | Fast reads, staleness OK |

```python
# Snapshot (default)
df = spark.read.format("hudi").load("/shared/hudi/orders")

# Incremental (only changes!)
df = spark.read.format("hudi") \
    .option("hoodie.datasource.query.type", "incremental") \
    .option("hoodie.datasource.read.begin.instanttime", "20240101100000") \
    .load("/shared/hudi/orders")

# Time Travel
df = spark.read.format("hudi") \
    .option("as.of.instant", "20240101100000") \
    .load("/shared/hudi/orders")

# Read-Optimized (MoR only)
df = spark.read.format("hudi") \
    .option("hoodie.datasource.query.type", "read_optimized") \
    .load("/shared/hudi/orders")
```

**Hudi Metadata Columns:**
| Column | Description |
|--------|-------------|
| `_hoodie_commit_time` | When record was written |
| `_hoodie_record_key` | Record key value |
| `_hoodie_partition_path` | Partition path |
| `_hoodie_file_name` | File containing record |

---

## 5. Indexing

Index answers: "Which file contains record key X?"

| Index | Lookup | Speed | Memory | Best For |
|-------|--------|-------|--------|----------|
| **BLOOM** (default) | Bloom filters in Parquet footer | Fast | Low | Medium tables, partitioned |
| **BUCKET** | Hash(key) % buckets = file | O(1) | None | Large tables, high throughput |
| **RECORD_INDEX** | Metadata table lookup | Fast | Low | New tables (Hudi 0.14+) |
| **SIMPLE** | Load all keys | Slow | High | Small tables, 100% accuracy |
| **GLOBAL_BLOOM** | Bloom across all partitions | Medium | Low | Records move partitions |

```python
# Bucket index (best for large tables)
"hoodie.index.type": "BUCKET",
"hoodie.bucket.index.num.buckets": "256",

# Bloom index (default)
"hoodie.index.type": "BLOOM",

# Record index (recommended for new tables)
"hoodie.index.type": "RECORD_INDEX",
"hoodie.metadata.enable": "true",
```

**Global vs Non-Global:**
- Non-global: Searches only target partition (fast, but duplicates if record moves)
- Global: Searches all partitions (slower, handles partition changes)

---

## 6. Schema Evolution

```python
# Enable schema evolution
"hoodie.datasource.write.reconcile.schema": "true"
```

| Change | Supported? |
|--------|-----------|
| Add nullable column | ✓ YES |
| Widen type (int→long) | ✓ YES |
| Make column nullable | ✓ YES |
| Remove column | ✗ NO (soft deprecate) |
| Rename column | ✗ NO (add new + deprecate) |
| Narrow type (long→int) | ✗ NO |

Old records return NULL for newly added columns (schema-on-read).

---

## 7. Compaction, Cleaning, Clustering

### Compaction (MoR only)
Merges log files into base Parquet files.
```python
"hoodie.compact.inline": "true",
"hoodie.compact.inline.max.delta.commits": "5",
```

### Cleaning
Removes old file versions to reclaim storage.
```python
"hoodie.clean.automatic": "true",
"hoodie.cleaner.policy": "KEEP_LATEST_COMMITS",
"hoodie.cleaner.commits.retained": "10",
```

### Clustering
Reorganizes data layout for better read performance.
```python
"hoodie.clustering.inline": "true",
"hoodie.clustering.inline.max.commits": "4",
"hoodie.clustering.plan.strategy.sort.columns": "city,date",
```

| Operation | Purpose | Table Type | Frequency |
|-----------|---------|-----------|-----------|
| Compaction | Merge logs → base | MoR only | Every 3-5 commits |
| Cleaning | Remove old versions | Both | After each commit |
| Clustering | Optimize layout | Both | Daily/weekly |

---

## 8. CDC and Incremental Pipelines

```
Source DB → CDC Tool (Debezium) → Kafka → Hudi DeltaStreamer → Hudi Table
                                                                    │
                                                    Incremental Query ↓
                                                              Downstream
```

**Incremental Pipeline Pattern:**
```python
# Read only changes since last run (NOT full table scan!)
last_commit = read_checkpoint()

df_changes = spark.read.format("hudi") \
    .option("hoodie.datasource.query.type", "incremental") \
    .option("hoodie.datasource.read.begin.instanttime", last_commit) \
    .load("/hudi/source_table")

# Process only changed records
df_enriched = df_changes.join(broadcast(dim_table), "key")

# Write downstream
df_enriched.write.format("hudi").options(**opts).mode("append").save("/hudi/target")

# Update checkpoint
save_checkpoint(new_commit_time)
```

**Why this matters:** 1B row table, 10K changes → process 10K rows, not 1B!

---

## 9. Concurrency and Timeline

**Timeline:** Ordered log of all operations (commits, compactions, cleans).
```
.hoodie/
├── 20240101100000.commit          ← Completed write
├── 20240102090000.deltacommit     ← MoR log write
├── 20240103080000.compaction      ← Compaction
└── 20240103090000.clean           ← Cleaning
```

**Concurrency:**
- Single writer (default): No locking needed
- Multi-writer: Requires lock provider (ZooKeeper, DynamoDB)
- MVCC: Readers never blocked by writers

**Multi-writer config:**
```python
"hoodie.write.concurrency.mode": "optimistic_concurrency_control",
"hoodie.write.lock.provider": "org.apache.hudi.aws.transaction.lock.DynamoDBBasedLockProvider",
"hoodie.write.lock.dynamodb.table": "hudi-locks",
```

---

## 10. Partitioning

```python
# Single partition
"hoodie.datasource.write.partitionpath.field": "date"

# Multi-level
"hoodie.datasource.write.partitionpath.field": "year,month,day"

# Non-partitioned
"hoodie.datasource.write.partitionpath.field": ""
"hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.NonpartitionedKeyGenerator"
```

**Best Practices:**
- Partition by most common filter column (usually date)
- Target 100MB - 1GB per partition
- Avoid high-cardinality keys (user_id = bad!)
- Date-based partitioning is most common

---

## 11. Configuration Reference

### Essential Write Configs
| Config | Default | Description |
|--------|---------|-------------|
| `hoodie.table.name` | - | Table name (required) |
| `hoodie.datasource.write.recordkey.field` | - | Primary key field(s) |
| `hoodie.datasource.write.precombine.field` | - | Dedup field (higher wins) |
| `hoodie.datasource.write.partitionpath.field` | - | Partition column |
| `hoodie.datasource.write.operation` | upsert | insert/upsert/delete/bulk_insert |
| `hoodie.datasource.write.table.type` | COPY_ON_WRITE | COPY_ON_WRITE / MERGE_ON_READ |
| `hoodie.index.type` | BLOOM | BLOOM/BUCKET/SIMPLE/RECORD_INDEX |

### File Sizing
| Config | Default | Description |
|--------|---------|-------------|
| `hoodie.parquet.max.file.size` | 128MB | Target max file size |
| `hoodie.parquet.small.file.limit` | 100MB | Files below this get more data |
| `hoodie.logfile.max.size` | 128MB | Max log file size (MoR) |

### Compaction (MoR)
| Config | Default | Description |
|--------|---------|-------------|
| `hoodie.compact.inline` | false | Compact during write |
| `hoodie.compact.inline.max.delta.commits` | 5 | Compact after N commits |
| `hoodie.compact.schedule.inline` | false | Schedule (don't execute) inline |

### Cleaning
| Config | Default | Description |
|--------|---------|-------------|
| `hoodie.clean.automatic` | true | Auto-clean after commit |
| `hoodie.cleaner.policy` | KEEP_LATEST_COMMITS | Retention policy |
| `hoodie.cleaner.commits.retained` | 10 | Commits to retain |

---

## 12. Performance Tuning

### Write Performance
1. **Index:** BUCKET for large tables, BLOOM for medium
2. **Table type:** MoR for write-heavy (5-10x faster writes)
3. **Initial load:** Always use `bulk_insert` (no index overhead)
4. **Parallelism:** Match to data volume (1 partition per 128MB)

### Read Performance
1. **Metadata table:** `hoodie.metadata.enable = true` (critical for S3!)
2. **Data skipping:** `hoodie.enable.data.skipping = true`
3. **Clustering:** Sort by query columns
4. **Compaction:** More frequent = faster MoR reads
5. **Partition pruning:** Filter on partition column

### Quick Sizing Guide
| Table Size | Index | Table Type | Parallelism |
|-----------|-------|-----------|-------------|
| < 1GB | SIMPLE | CoW | 4-8 |
| 1-10GB | BLOOM | CoW | 20-50 |
| 10-100GB | BLOOM/BUCKET | CoW or MoR | 50-200 |
| > 100GB | BUCKET | MoR | 200-1000 |

---

## 13. Hudi vs Delta vs Iceberg

| Feature | Hudi | Delta Lake | Iceberg |
|---------|------|-----------|---------|
| **Upsert** | Native (indexed) | MERGE command | MERGE command |
| **Incremental Query** | Native (built-in!) | CDF (Change Data Feed) | Snapshot diff |
| **Record-level Index** | ✓ (BLOOM, BUCKET) | ✗ | ✗ |
| **Table Types** | CoW + MoR | Single (CoW-like) | Single |
| **Compaction** | Built-in (MoR) | OPTIMIZE | Rewrite |
| **Streaming** | Native (DeltaStreamer) | Structured Streaming | Flink/Spark |
| **CDC** | Native support | CDF (Spark 3.x) | Snapshot-based |
| **Multi-engine** | Spark, Flink, Presto | Spark (primary) | Spark, Flink, Presto, Trino |
| **Best For** | Streaming CDC, upserts | Batch ETL, Spark-native | Multi-engine analytics |

---

## 14. Interview Questions

### Q: What is Apache Hudi and why use it?
**A:** Hudi is a data lakehouse storage layer providing ACID transactions, upserts, deletes, time travel, and incremental queries on data lakes. Use it when you need database-like operations (update/delete) on data lake storage (S3/HDFS) with streaming ingestion support.

### Q: Difference between CoW and MoR?
**A:** CoW rewrites entire files on update (slow writes, fast reads). MoR appends to log files (fast writes, slower reads until compaction). Choose CoW for read-heavy batch workloads, MoR for write-heavy streaming ingestion.

### Q: What is the precombine field?
**A:** It resolves conflicts when multiple records have the same record key. The record with the higher precombine value wins. Typically a timestamp (updated_at) or version number.

### Q: How does Hudi indexing work?
**A:** Index maps record keys to file groups. During upsert, Hudi looks up the index to determine if a record exists (UPDATE) or is new (INSERT). BLOOM uses bloom filters (fast, some false positives). BUCKET uses hash (O(1), no lookup). RECORD_INDEX uses metadata table.

### Q: What is an incremental query?
**A:** It reads only records that changed since a given commit timestamp. Instead of scanning the entire table (expensive), you process only the delta. This is Hudi's killer feature for building efficient ETL pipelines.

### Q: How does compaction work in MoR?
**A:** MoR writes append to log files (fast). Over time, logs accumulate and reads slow down (must merge base + logs). Compaction merges logs into new base Parquet files, restoring read performance. Can run inline (during write) or async (separate job).

### Q: How does Hudi achieve exactly-once?
**A:** Through the timeline (transaction log). Each write is atomic - either fully committed or rolled back. Combined with checkpointing in Spark Streaming, this provides exactly-once end-to-end. The timeline records all operations with their state (requested → inflight → committed/rolled back).

### Q: How to handle CDC with Hudi?
**A:** Source DB → CDC tool (Debezium) → Kafka → Hudi DeltaStreamer/Spark → Hudi table. CDC events (insert/update/delete) are applied as upserts/deletes. Downstream consumers use incremental queries to read only changes.

### Q: Cleaning vs Compaction vs Clustering?
**A:** Cleaning removes old file versions (reclaims storage). Compaction merges MoR log files into base files (improves read speed). Clustering reorganizes data layout by sorting (improves query performance via data skipping).

### Q: When would you choose Hudi over Delta Lake?
**A:** Choose Hudi when: 1) Need fast record-level upserts on large tables (indexed), 2) Building CDC/streaming pipelines (native incremental queries), 3) Need MoR for write-heavy workloads, 4) Need multi-engine support (Flink, Presto). Choose Delta when: Spark-only environment, simpler batch ETL, prefer SQL-first approach.

---

## Spark Submit Command

```bash
spark-submit \
    --packages org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.1 \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
    my_hudi_app.py
```

---

*All scripts write DataFrames to `/shared` path. Generated for Apache Hudi interview preparation.*
