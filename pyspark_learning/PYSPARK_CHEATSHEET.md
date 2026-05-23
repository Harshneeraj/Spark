# PySpark Complete Cheatsheet - Interview Ready

## Table of Contents
- [1. SparkSession](#1-sparksession)
- [2. DataFrame Creation](#2-dataframe-creation)
- [3. Transformations vs Actions](#3-transformations-vs-actions)
- [4. Column Operations](#4-column-operations)
- [5. Filtering](#5-filtering)
- [6. Aggregations](#6-aggregations)
- [7. Joins](#7-joins)
- [8. Window Functions](#8-window-functions)
- [9. File Formats](#9-file-formats)
- [10. Partitioning](#10-partitioning)
- [11. Caching](#11-caching)
- [12. Spark SQL](#12-spark-sql)
- [13. UDFs](#13-udfs)
- [14. Optimization Techniques](#14-optimization-techniques)
- [15. Data Skew Handling](#15-data-skew-handling)
- [16. Broadcast Join](#16-broadcast-join)
- [17. AQE](#17-adaptive-query-execution)
- [18. Memory Management](#18-memory-management)
- [19. Key Configurations](#19-key-configurations)
- [20. Spark Execution Model](#20-spark-execution-model)
- [21. Spark UI Reading Guide](#21-spark-ui-reading-guide)
- [22. Interview Quick Answers](#22-interview-quick-answers)

---

## 1. SparkSession

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# Access SparkContext
sc = spark.sparkContext
```

**Spark UI:** No job triggered on session creation.

---

## 2. DataFrame Creation

```python
# From list (explicit schema - PREFERRED)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True)
])
df = spark.createDataFrame(data, schema)

# From file
df = spark.read.parquet("/path/to/file")
df = spark.read.option("header", "true").schema(schema).csv("/path")
df = spark.read.json("/path")
```

---

## 3. Transformations vs Actions

| Type | Examples | Triggers Job? | Shuffle? |
|------|----------|---------------|----------|
| **Narrow Transform** | select, filter, withColumn, map | No | No |
| **Wide Transform** | groupBy, join, repartition, distinct | No | Yes (on action) |
| **Action** | show, count, collect, write, take | **YES** | Depends |

**Key Rule:** Transformations are LAZY. Actions trigger execution.

---

## 4. Column Operations

```python
from pyspark.sql.functions import col, lit, when, concat, upper, lower

# Select
df.select("name", "salary")
df.select(col("name"), col("salary") * 1.1)

# Add/Modify column
df.withColumn("bonus", col("salary") * 0.1)
df.withColumn("upper_name", upper(col("name")))

# Rename
df.withColumnRenamed("name", "employee_name")

# Drop
df.drop("column_name")

# Cast
df.withColumn("salary", col("salary").cast("double"))

# Conditional
df.withColumn("band", when(col("salary") > 70000, "High")
                      .when(col("salary") > 50000, "Mid")
                      .otherwise("Low"))
```

---

## 5. Filtering

```python
# Basic
df.filter(col("salary") > 50000)
df.where(col("department") == "Engineering")  # same as filter

# Multiple conditions
df.filter((col("age") > 25) & (col("dept") == "Eng"))
df.filter((col("age") > 30) | (col("salary") > 70000))

# IN / NOT IN
df.filter(col("dept").isin("Eng", "HR"))
df.filter(~col("dept").isin("Eng", "HR"))

# Null checks
df.filter(col("name").isNull())
df.filter(col("name").isNotNull())

# Pattern matching
df.filter(col("name").like("A%"))
df.filter(col("name").rlike("^[A-C].*"))

# Between
df.filter(col("salary").between(50000, 80000))
```

---

## 6. Aggregations

```python
from pyspark.sql.functions import count, sum, avg, min, max, collect_list, countDistinct

# GroupBy + Agg
df.groupBy("department").agg(
    count("*").alias("emp_count"),
    avg("salary").alias("avg_salary"),
    max("salary").alias("max_salary"),
    sum("salary").alias("total_salary"),
    countDistinct("level").alias("distinct_levels"),
    collect_list("name").alias("names")
)

# Without groupBy (whole table)
df.agg(count("*"), avg("salary"))
```

**Spark UI:** groupBy -> 2 stages (partial agg | shuffle + final agg)

---

## 7. Joins

```python
from pyspark.sql.functions import broadcast

# Join types
df1.join(df2, "key", "inner")       # Only matching
df1.join(df2, "key", "left")        # All left + matching right
df1.join(df2, "key", "right")       # All right + matching left
df1.join(df2, "key", "outer")       # All from both
df1.join(df2, "key", "leftsemi")    # Left rows that HAVE match (like EXISTS)
df1.join(df2, "key", "leftanti")    # Left rows that DON'T match (like NOT EXISTS)
df1.crossJoin(df2)                   # Cartesian product (M x N rows!)

# Different column names
df1.join(df2, df1["col_a"] == df2["col_b"], "inner")

# Broadcast join (small table)
df_large.join(broadcast(df_small), "key", "inner")
```

**Spark UI:** Regular join -> 3 stages. Broadcast join -> 2 stages.

---

## 8. Window Functions

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, lead, lag, sum

# Define window
w = Window.partitionBy("department").orderBy(col("salary").desc())

# Ranking (COMMON INTERVIEW QUESTION)
df.withColumn("row_num", row_number().over(w))   # 1,2,3,4 (unique)
df.withColumn("rank", rank().over(w))             # 1,2,2,4 (skip)
df.withColumn("dense_rank", dense_rank().over(w)) # 1,2,2,3 (no skip)

# Lead/Lag
df.withColumn("next_salary", lead("salary", 1).over(w))
df.withColumn("prev_salary", lag("salary", 1).over(w))

# Running total
w_running = Window.partitionBy("dept").orderBy("date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
df.withColumn("running_total", sum("amount").over(w_running))

# Top N per group
df.withColumn("rn", row_number().over(w)).filter(col("rn") <= 3)
```

**Spark UI:** Window functions cause shuffle -> 2 stages.

---

## 9. File Formats

| Format | Type | Column Pruning | Predicate Pushdown | Schema | Compression | Best For |
|--------|------|---------------|-------------------|--------|-------------|----------|
| **Parquet** | Columnar | ✓ YES | ✓ YES | Embedded | Excellent | Analytics/Spark |
| **ORC** | Columnar | ✓ YES | ✓ YES | Embedded | Excellent | Hive |
| **Avro** | Row | ✗ NO | Partial | Embedded | Good | Kafka/Streaming |
| **CSV** | Row/Text | ✗ NO | ✗ NO | None | Poor | Interchange |
| **JSON** | Row/Text | ✗ NO | ✗ NO | Inferred | Poor | APIs/Logs |

### Read/Write Quick Reference

```python
# PARQUET (ALWAYS PREFER)
df.write.mode("overwrite").parquet("/shared/output")
df = spark.read.parquet("/shared/output")

# CSV
df.write.option("header","true").csv("/shared/output")
df = spark.read.option("header","true").schema(schema).csv("/shared/input")

# JSON
df.write.json("/shared/output")
df = spark.read.schema(schema).json("/shared/input")

# ORC
df.write.orc("/shared/output")
df = spark.read.orc("/shared/input")

# AVRO
df.write.format("avro").save("/shared/output")
df = spark.read.format("avro").load("/shared/input")
```

### Why Parquet is Fastest
1. **Columnar** - reads only needed columns (10 cols, need 2 = read 2)
2. **Statistics** - min/max per row group enables skipping
3. **Encoding** - dictionary, RLE, delta encoding per column
4. **Compression** - similar values in column compress better
5. **No inference** - schema embedded, no extra scan job

---

## 10. Partitioning

```python
# Repartition (SHUFFLE - can increase or decrease)
df.repartition(8)                    # Even distribution, 8 partitions
df.repartition(8, "department")      # Hash by column

# Coalesce (NO SHUFFLE - can only decrease)
df.coalesce(2)                       # Combine partitions locally

# Check partitions
df.rdd.getNumPartitions()

# Write partitioned (directory structure)
df.write.partitionBy("date").parquet("/shared/output")

# Partition pruning on read
spark.read.parquet("/shared/output").filter(col("date") == "2024-01-01")
# Only reads the date=2024-01-01 directory!
```

| Operation | Shuffle? | Use When |
|-----------|----------|----------|
| `repartition(N)` | YES | Increase partitions, need even distribution |
| `repartition(N, col)` | YES | Co-locate data by key before join/groupBy |
| `coalesce(N)` | NO | Reduce partitions before write |

---

## 11. Caching

```python
from pyspark import StorageLevel

# Cache (MEMORY_ONLY)
df.cache()

# Persist with storage level
df.persist(StorageLevel.MEMORY_AND_DISK)
df.persist(StorageLevel.MEMORY_ONLY_SER)
df.persist(StorageLevel.DISK_ONLY)

# Unpersist (free memory!)
df.unpersist()

# Check if cached
df.is_cached
```

**When to cache:** DataFrame reused multiple times.  
**When NOT to cache:** Used once, too large for memory.

---

## 12. Spark SQL

```python
# Register temp view
df.createOrReplaceTempView("employees")

# Run SQL
result = spark.sql("""
    SELECT department, AVG(salary) as avg_sal
    FROM employees
    WHERE salary > 50000
    GROUP BY department
    HAVING AVG(salary) > 60000
    ORDER BY avg_sal DESC
""")

# CTE
spark.sql("""
    WITH dept_stats AS (
        SELECT department, AVG(salary) as avg_sal FROM employees GROUP BY department
    )
    SELECT e.*, d.avg_sal FROM employees e JOIN dept_stats d ON e.department = d.department
""")
```

**Key Point:** SQL and DataFrame API produce the SAME execution plan.

---

## 13. UDFs

```python
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import StringType
import pandas as pd

# Regular UDF (SLOW - row by row serialization)
@udf(returnType=StringType())
def my_udf(value):
    return value.upper() if value else None

# Pandas UDF (FAST - vectorized with Arrow)
@pandas_udf(StringType())
def my_pandas_udf(series: pd.Series) -> pd.Series:
    return series.str.upper()

# Apply
df.withColumn("result", my_udf(col("name")))
df.withColumn("result", my_pandas_udf(col("name")))
```

**Performance:** Built-in functions > Pandas UDF > Regular UDF (10-100x difference)

---

## 14. Optimization Techniques

### Quick Checklist
1. ✅ Use Parquet format
2. ✅ Filter early (reduce data before joins/groupBy)
3. ✅ Select only needed columns
4. ✅ Broadcast small tables in joins
5. ✅ Set appropriate shuffle partitions
6. ✅ Cache reused DataFrames
7. ✅ Use built-in functions (not UDFs)
8. ✅ Enable AQE
9. ✅ Coalesce before write (avoid small files)
10. ✅ Provide explicit schema (avoid inferSchema)

### Avoid
- ❌ `collect()` on large DataFrames
- ❌ UDFs when built-in functions exist
- ❌ Too many/few shuffle partitions
- ❌ Unnecessary actions (each = new job)
- ❌ `df.count()` just to check if empty (use `df.head(1)`)

---

## 15. Data Skew Handling

### Identify Skew
- Spark UI -> Stages -> Task duration: one task 10-100x slower
- `df.groupBy("key").count().orderBy(desc("count"))` - check distribution

### Solution 1: Salting (Most Important)
```python
SALT = 8
# Large table: add random salt
df_large = df_large.withColumn("salt", (rand() * SALT).cast("int"))
df_large = df_large.withColumn("salted_key", concat(col("key"), lit("_"), col("salt")))

# Small table: explode with all salt values
salt_df = spark.range(SALT).withColumnRenamed("id", "salt")
df_small = df_small.crossJoin(salt_df)
df_small = df_small.withColumn("salted_key", concat(col("key"), lit("_"), col("salt")))

# Join on salted key
result = df_large.join(df_small, "salted_key")
```

### Solution 2: Broadcast Join
```python
df_large.join(broadcast(df_small), "key")  # No shuffle!
```

### Solution 3: Isolate Hot Key
```python
df_hot = df.filter(col("key") == "hot_value")
df_normal = df.filter(col("key") != "hot_value")
# Process separately, union results
```

### Solution 4: Two-Phase Aggregation
```python
# Phase 1: Partial agg with salt
df.withColumn("salt", (rand() * N).cast("int")) \
  .groupBy("key", "salt").agg(sum("val").alias("partial")) \
  .groupBy("key").agg(sum("partial").alias("total"))
```

---

## 16. Broadcast Join

```python
from pyspark.sql.functions import broadcast

# Force broadcast (small table sent to all executors)
df_large.join(broadcast(df_small), "key", "inner")

# Auto-broadcast threshold (default 10MB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")

# Disable auto-broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
```

| Join Strategy | Stages | When Used |
|--------------|--------|-----------|
| Broadcast Hash Join | 2 | One side < 10MB |
| Sort-Merge Join | 3 | Both sides large (default) |
| Shuffle Hash Join | 3 | One side much smaller |

---

## 17. Adaptive Query Execution

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")                    # Enable AQE
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true") # Merge small partitions
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")           # Auto-handle skew
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true") # Local shuffle read
```

**AQE does 3 things automatically:**
1. Coalesces small post-shuffle partitions
2. Converts sort-merge join to broadcast at runtime
3. Splits skewed partitions

---

## 18. Memory Management

```
Total JVM Heap (spark.executor.memory)
├── Reserved: 300MB
├── User Memory: 40% (UDFs, data structures)
└── Spark Memory: 60% (spark.memory.fraction)
    ├── Storage: 50% (cache, broadcast)
    └── Execution: 50% (shuffle, join, sort)
```

**OOM Fixes:**
- Increase `spark.executor.memory`
- Increase `spark.sql.shuffle.partitions` (less data per task)
- Fix data skew
- Avoid `collect()` on large data
- Use `MEMORY_AND_DISK` for cache

---

## 19. Key Configurations

| Config | Default | Description |
|--------|---------|-------------|
| `spark.sql.shuffle.partitions` | 200 | Partitions after shuffle |
| `spark.sql.autoBroadcastJoinThreshold` | 10MB | Auto-broadcast limit |
| `spark.sql.adaptive.enabled` | true* | Enable AQE |
| `spark.executor.memory` | 1g | Executor heap |
| `spark.executor.cores` | 1 | Cores per executor |
| `spark.driver.memory` | 1g | Driver heap |
| `spark.memory.fraction` | 0.6 | Heap for Spark |
| `spark.memory.storageFraction` | 0.5 | Spark memory for cache |
| `spark.serializer` | Java | Use Kryo for speed |
| `spark.sql.files.maxPartitionBytes` | 128MB | Max input partition |
| `spark.speculation` | false | Speculative execution |
| `spark.dynamicAllocation.enabled` | false | Auto-scale executors |

---

## 20. Spark Execution Model

```
Action (show/count/write)
    → Job (1 action = 1 job)
        → Stages (split at shuffle boundaries)
            → Tasks (1 per partition per stage)
```

| Operation | Jobs | Stages | Why |
|-----------|------|--------|-----|
| `df.show()` | 1 | 1 | No shuffle |
| `df.groupBy().count().show()` | 1 | 2 | Shuffle for groupBy |
| `df.join(df2).show()` | 1 | 3 | Shuffle both sides + join |
| `df.orderBy().show()` | 1 | 2 | Range shuffle for sort |
| `df.distinct().show()` | 1 | 2 | Shuffle for dedup |
| `df.coalesce(N).show()` | 1 | 1 | No shuffle (narrow) |

---

## 21. Spark UI Reading Guide

### Where to Look
- **Jobs tab:** One entry per action
- **Stages tab:** Stages within a job (split at shuffles)
- **Storage tab:** Cached DataFrames
- **SQL tab:** Query plans with metrics
- **Executors tab:** Memory, GC, task counts

### Red Flags
| Symptom | Cause | Fix |
|---------|-------|-----|
| One task 10x slower | Data skew | Salting, broadcast |
| High GC time | Memory pressure | More memory, more partitions |
| Spill to disk | Not enough memory/task | Increase partitions |
| Many tasks, tiny input | Too many partitions | Reduce partitions, coalesce |
| Large shuffle write | Unnecessary shuffle | Broadcast, filter early |

---

## 22. Interview Quick Answers

### Q: Difference between repartition and coalesce?
**A:** `repartition(N)` does a full shuffle (can increase/decrease, even distribution). `coalesce(N)` combines partitions locally without shuffle (can only decrease, may be uneven).

### Q: Difference between cache and persist?
**A:** `cache()` = `persist(MEMORY_ONLY)`. `persist()` allows choosing storage level (MEMORY_AND_DISK, DISK_ONLY, etc.).

### Q: How to handle data skew?
**A:** 1) Salting (split hot key into sub-keys), 2) Broadcast join for small tables, 3) Isolate hot keys and process separately, 4) Enable AQE skew join, 5) Two-phase aggregation.

### Q: Why is Parquet faster than CSV?
**A:** Columnar (reads only needed columns), predicate pushdown (skips row groups using statistics), better compression (similar values together), embedded schema (no inference job), binary encoding (no text parsing).

### Q: Difference between client and cluster deploy mode?
**A:** Client mode runs driver on submitting machine (good for debugging). Cluster mode runs driver on cluster node (fault-tolerant, for production).

### Q: What is a shuffle?
**A:** Redistribution of data across partitions. Happens during wide transformations (groupBy, join, repartition). Expensive because it involves disk I/O, serialization, and network transfer.

### Q: row_number vs rank vs dense_rank?
**A:** For values [100, 90, 90, 80]: `row_number` = 1,2,3,4 (unique). `rank` = 1,2,2,4 (ties get same rank, skip). `dense_rank` = 1,2,2,3 (ties get same rank, no skip).

### Q: What is predicate pushdown?
**A:** Pushing filter conditions to the data source level so only matching data is read from disk. Works with Parquet/ORC (uses column statistics). Doesn't work with CSV/JSON.

### Q: How to size a Spark cluster?
**A:** executor.cores = 5 (HDFS optimal). Executors per node = (total_cores - 1) / 5. executor.memory = (total_RAM - overhead) / executors_per_node. Leave 1 core and some RAM for OS.

### Q: What is AQE?
**A:** Adaptive Query Execution optimizes at runtime using actual data statistics. It: 1) Coalesces small partitions, 2) Converts to broadcast join if data is small enough, 3) Splits skewed partitions.

### Q: Narrow vs Wide transformation?
**A:** Narrow: each input partition maps to at most one output partition (no shuffle). Wide: input partitions contribute to multiple output partitions (shuffle required, new stage boundary).

### Q: What causes OOM in Spark?
**A:** 1) Data skew (one partition too large), 2) collect() on large data, 3) Broadcast table too large, 4) Too few partitions (too much data per task), 5) Memory-intensive UDFs.

### Q: Dynamic partition overwrite vs static?
**A:** Static (default) deletes ALL partitions and rewrites. Dynamic only overwrites partitions present in new data, preserving others. Critical for incremental daily loads.

### Q: What is bucketing?
**A:** Pre-partitioning data by a column during write. Subsequent joins/groupBy on that column skip shuffle entirely because data is already co-located.

---

## File Format Decision Matrix

```
Need analytics/queries?     → PARQUET
Need Hive ACID?             → ORC  
Need Kafka/streaming?       → AVRO
Need human-readable?        → CSV
Need semi-structured/APIs?  → JSON
Need schema evolution?      → AVRO
Need best compression?      → PARQUET/ORC
Need fastest writes?        → AVRO/CSV
Need fastest reads?         → PARQUET/ORC
```

---

## Compression Codecs

| Codec | Ratio | Speed | Use Case |
|-------|-------|-------|----------|
| **Snappy** | ~2-3x | Very Fast | Default. Hot data. |
| **LZ4** | ~2-3x | Very Fast | Alternative to Snappy |
| **ZSTD** | ~4-5x | Fast | Best balance |
| **GZIP** | ~4-5x | Slow | Archival, cold data |
| **None** | 1x | Fastest | CPU-bound workloads |

---

*Generated for PySpark interview preparation. All scripts write to `/shared` path.*
