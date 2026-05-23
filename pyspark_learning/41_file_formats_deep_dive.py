"""
Topic: File Formats Deep Dive - Parquet, ORC, CSV, JSON, Avro
==============================================================

Complete comparison of all major file formats in Spark with performance
analysis, internal architecture, and read/write behavior.

Spark UI Behavior:
- CSV/JSON READ with inferSchema=True: 1 EXTRA job to scan and infer types.
- CSV/JSON READ with explicit schema: No extra job.
- Parquet/ORC/Avro READ: No extra job (schema embedded in file).
- ALL WRITES: 1 job, stages depend on prior transformations.
- Parquet/ORC with filter: Predicate pushdown reduces input bytes (visible in stage metrics).
- CSV/JSON with filter: Full scan always (no pushdown).

Key Interview Points:
- Parquet: Columnar, best for analytics, predicate pushdown, column pruning.
- ORC: Columnar, optimized for Hive, ACID support, good compression.
- CSV: Row-based, human-readable, no schema, no optimization. Interchange only.
- JSON: Semi-structured, schema inference, larger files. API/log data.
- Avro: Row-based, schema evolution, compact binary. Good for streaming/Kafka.
- ALWAYS use Parquet for Spark analytical workloads.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, DateType, ArrayType, MapType, BooleanType
)
from pyspark.sql.functions import col, count, sum, avg, current_timestamp
import time

spark = SparkSession.builder \
    .appName("41_File_Formats_Deep_Dive") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .config("spark.sql.orc.compression.codec", "snappy") \
    .getOrCreate()

# ============ SAMPLE DATA ============
data = []
for i in range(1, 1001):
    data.append((
        i,
        f"Employee_{i}",
        f"employee_{i}@company.com",
        ["Engineering", "Marketing", "HR", "Finance", "Operations"][i % 5],
        ["Junior", "Mid", "Senior", "Lead", "Director"][i % 5],
        30000 + (i * 50),
        f"2020-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
        i % 2 == 0
    ))

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("department", StringType(), True),
    StructField("level", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("join_date", StringType(), True),
    StructField("is_active", BooleanType(), True)
])

df = spark.createDataFrame(data, schema)
print(f"Sample data: {df.count()} rows, {len(df.columns)} columns")
df.show(5)


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                         1. PARQUET FORMAT                                    ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
"""
PARQUET - Columnar Storage Format
===================================

Architecture:
┌─────────────────────────────────────────────────────────┐
│ Parquet File                                             │
├─────────────────────────────────────────────────────────┤
│ Row Group 1 (default 128MB)                              │
│   ├── Column Chunk: id      [1, 2, 3, 4, ...]          │
│   │   ├── Page 1 (data page, ~1MB)                      │
│   │   ├── Page 2                                        │
│   │   └── Column metadata (min, max, null count)        │
│   ├── Column Chunk: name    ["Alice", "Bob", ...]       │
│   ├── Column Chunk: salary  [90000, 45000, ...]         │
│   └── Column Chunk: ...                                  │
├─────────────────────────────────────────────────────────┤
│ Row Group 2                                              │
│   ├── Column Chunk: id                                   │
│   ├── Column Chunk: name                                 │
│   └── ...                                                │
├─────────────────────────────────────────────────────────┤
│ Footer                                                   │
│   ├── File metadata (schema, row groups info)            │
│   ├── Column statistics (min, max, null_count per chunk) │
│   └── Row group offsets                                  │
└─────────────────────────────────────────────────────────┘

WHY PARQUET IS FAST:
1. COLUMNAR: Only reads columns you need (column pruning)
   SELECT name, salary FROM table -> Only reads 2 columns, skips rest
   
2. PREDICATE PUSHDOWN: Uses min/max stats to skip row groups
   WHERE salary > 80000 -> If row group max(salary) = 50000, SKIP entire group
   
3. ENCODING: Type-specific encoding reduces size
   - Dictionary encoding for low-cardinality strings
   - Run-length encoding for repeated values
   - Delta encoding for sorted integers
   
4. COMPRESSION: Column-level compression (similar values compress well)
   - Snappy (default): Fast, moderate compression
   - GZIP: Better compression, slower
   - ZSTD: Best balance of speed and compression
   - LZ4: Fastest, least compression

5. SCHEMA EMBEDDED: No extra job to infer schema on read
"""

# Write Parquet
print("\n=== WRITING PARQUET ===")
start = time.time()
df.write.mode("overwrite").parquet("/shared/formats/parquet_snappy")
print(f"Parquet (Snappy) write time: {time.time() - start:.3f}s")

# Write with different compression
df.write.mode("overwrite") \
    .option("compression", "gzip") \
    .parquet("/shared/formats/parquet_gzip")

df.write.mode("overwrite") \
    .option("compression", "none") \
    .parquet("/shared/formats/parquet_none")

# Read Parquet
print("\n=== READING PARQUET ===")
start = time.time()
df_parquet = spark.read.parquet("/shared/formats/parquet_snappy")
cnt = df_parquet.count()
print(f"Parquet read + count time: {time.time() - start:.3f}s ({cnt} rows)")

# Column pruning demo - only reads 2 columns
print("\n=== PARQUET COLUMN PRUNING ===")
df_pruned = spark.read.parquet("/shared/formats/parquet_snappy").select("name", "salary")
df_pruned.explain()
# ReadSchema shows only name and salary (other columns NOT read from disk)

# Predicate pushdown demo
print("\n=== PARQUET PREDICATE PUSHDOWN ===")
df_pushed = spark.read.parquet("/shared/formats/parquet_snappy") \
    .filter(col("salary") > 60000)
df_pushed.explain()
# PushedFilters: [IsNotNull(salary), GreaterThan(salary,60000)]


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                         2. ORC FORMAT                                        ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
"""
ORC (Optimized Row Columnar) - Columnar Storage Format
========================================================

Architecture:
┌─────────────────────────────────────────────────────────┐
│ ORC File                                                 │
├─────────────────────────────────────────────────────────┤
│ Stripe 1 (default 64MB, configurable)                    │
│   ├── Index Data (min, max, sum, count per 10K rows)    │
│   ├── Row Data                                           │
│   │   ├── Stream: id      [encoded column data]         │
│   │   ├── Stream: name    [encoded column data]         │
│   │   └── Stream: salary  [encoded column data]         │
│   └── Stripe Footer (encoding info, stream positions)    │
├─────────────────────────────────────────────────────────┤
│ Stripe 2                                                 │
│   ├── Index Data                                         │
│   ├── Row Data                                           │
│   └── Stripe Footer                                      │
├─────────────────────────────────────────────────────────┤
│ File Footer                                              │
│   ├── Schema (type information)                          │
│   ├── Stripe information (offset, length, row count)     │
│   └── Column statistics (min, max, sum, count)           │
├─────────────────────────────────────────────────────────┤
│ Postscript (compression info, footer length)             │
└─────────────────────────────────────────────────────────┘

ORC vs PARQUET:
- ORC has built-in indexes (every 10K rows) -> finer-grained skipping
- ORC supports ACID transactions (with Hive)
- ORC has better compression for Hive workloads
- Parquet has wider ecosystem support (Spark, Impala, Drill, Arrow)
- Parquet is Spark's native/default format
- Both support predicate pushdown and column pruning

WHY ORC IS GOOD:
1. Built-in lightweight indexes (bloom filters, min/max per 10K rows)
2. Better compression than Parquet in some cases (ZLIB default)
3. ACID support with Hive (INSERT, UPDATE, DELETE)
4. Stripe-level statistics for predicate pushdown
5. Self-describing (schema in footer)
"""

# Write ORC
print("\n=== WRITING ORC ===")
start = time.time()
df.write.mode("overwrite").orc("/shared/formats/orc_snappy")
print(f"ORC (Snappy) write time: {time.time() - start:.3f}s")

df.write.mode("overwrite") \
    .option("compression", "zlib") \
    .orc("/shared/formats/orc_zlib")

# Read ORC
print("\n=== READING ORC ===")
start = time.time()
df_orc = spark.read.orc("/shared/formats/orc_snappy")
cnt = df_orc.count()
print(f"ORC read + count time: {time.time() - start:.3f}s ({cnt} rows)")

# ORC also supports predicate pushdown
print("\n=== ORC PREDICATE PUSHDOWN ===")
df_orc_filtered = spark.read.orc("/shared/formats/orc_snappy") \
    .filter(col("department") == "Engineering")
df_orc_filtered.explain()

# ORC column pruning
print("\n=== ORC COLUMN PRUNING ===")
spark.read.orc("/shared/formats/orc_snappy").select("name", "salary").explain()


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                         3. CSV FORMAT                                        ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
"""
CSV (Comma-Separated Values) - Row-Based Text Format
======================================================

Architecture:
┌─────────────────────────────────────────────────────────┐
│ CSV File                                                 │
├─────────────────────────────────────────────────────────┤
│ Header Row (optional):                                   │
│   id,name,email,department,level,salary,join_date        │
├─────────────────────────────────────────────────────────┤
│ Data Rows:                                               │
│   1,Alice,alice@email.com,Engineering,Senior,90000,...   │
│   2,Bob,bob@email.com,Marketing,Junior,45000,...         │
│   3,Charlie,charlie@email.com,HR,Mid,65000,...           │
│   ...                                                    │
└─────────────────────────────────────────────────────────┘

WHY CSV IS SLOW:
1. ROW-BASED: Must read ENTIRE row even if you need 1 column
   SELECT salary FROM table -> Still reads name, email, dept, etc.
   
2. NO SCHEMA: Must infer types (extra scan job) or treat all as strings
   inferSchema=True triggers an extra job to scan all data
   
3. NO COMPRESSION AWARENESS: Text format doesn't compress well
   Numbers stored as text: "90000" = 5 bytes vs 4 bytes as int
   
4. NO PREDICATE PUSHDOWN: Can't skip rows without reading them
   WHERE salary > 80000 -> Must read and parse EVERY row
   
5. PARSING OVERHEAD: Must handle quotes, escapes, delimiters
   "Alice, Jr." -> Need to handle comma inside quotes
   
6. NO COLUMN STATISTICS: No min/max metadata for skipping

WHEN TO USE CSV:
- Data interchange with non-Spark systems
- Human-readable output for small datasets
- Legacy system compatibility
- One-time imports from external sources
"""

# Write CSV
print("\n=== WRITING CSV ===")
start = time.time()
df.write.mode("overwrite") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("quote", '"') \
    .option("escape", '"') \
    .csv("/shared/formats/csv_data")
print(f"CSV write time: {time.time() - start:.3f}s")

# Read CSV with inferSchema (SLOW - extra job!)
print("\n=== READING CSV (inferSchema=True - EXTRA JOB!) ===")
start = time.time()
df_csv_infer = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/shared/formats/csv_data")
cnt = df_csv_infer.count()
print(f"CSV read (infer) + count time: {time.time() - start:.3f}s ({cnt} rows)")
print("Schema inferred:")
df_csv_infer.printSchema()

# Read CSV with explicit schema (FASTER - no extra job)
print("\n=== READING CSV (explicit schema - NO extra job) ===")
start = time.time()
df_csv_explicit = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv("/shared/formats/csv_data")
cnt = df_csv_explicit.count()
print(f"CSV read (explicit) + count time: {time.time() - start:.3f}s ({cnt} rows)")

# CSV has NO predicate pushdown
print("\n=== CSV NO PREDICATE PUSHDOWN ===")
df_csv_filtered = spark.read.option("header", "true").schema(schema) \
    .csv("/shared/formats/csv_data") \
    .filter(col("salary") > 60000)
df_csv_filtered.explain()
# Notice: NO PushedFilters! Filter happens AFTER full scan.

# CSV has NO column pruning (reads all columns regardless)
print("\n=== CSV NO COLUMN PRUNING ===")
spark.read.option("header", "true").schema(schema) \
    .csv("/shared/formats/csv_data") \
    .select("name", "salary").explain()
# ReadSchema still shows all columns being read

# CSV options for handling edge cases
"""
Important CSV options:
  .option("header", "true")           # First row is header
  .option("inferSchema", "true")      # Auto-detect types (SLOW!)
  .option("delimiter", ",")           # Column separator
  .option("quote", '"')               # Quote character
  .option("escape", '"')              # Escape character
  .option("multiLine", "true")        # Handle multi-line values
  .option("nullValue", "NULL")        # String representing null
  .option("dateFormat", "yyyy-MM-dd") # Date parsing format
  .option("mode", "PERMISSIVE")       # Error handling mode
    - PERMISSIVE: Set malformed fields to null (default)
    - DROPMALFORMED: Drop malformed rows
    - FAILFAST: Throw exception on malformed data
"""


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                         4. JSON FORMAT                                       ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
"""
JSON (JavaScript Object Notation) - Semi-Structured Text Format
================================================================

Architecture:
┌─────────────────────────────────────────────────────────┐
│ JSON File (one JSON object per line - JSON Lines format) │
├─────────────────────────────────────────────────────────┤
│ {"id":1,"name":"Alice","salary":90000,"dept":"Eng"}     │
│ {"id":2,"name":"Bob","salary":45000,"dept":"Mkt"}       │
│ {"id":3,"name":"Charlie","salary":65000}                │ <- missing field OK!
│ {"id":4,"name":"Diana","salary":55000,"extra":"val"}    │ <- extra field OK!
│ ...                                                      │
└─────────────────────────────────────────────────────────┘

WHY JSON IS SLOW:
1. ROW-BASED: Same as CSV - must read entire record for any column
2. TEXT OVERHEAD: Field names repeated in EVERY row
   {"salary": 90000} = 16 bytes vs just 4 bytes in Parquet
3. PARSING OVERHEAD: JSON parsing is CPU-intensive
4. NO PREDICATE PUSHDOWN: Must parse every record
5. NO COLUMN PRUNING: Must read full JSON object
6. LARGER FILE SIZE: Field names + formatting add significant overhead
   Typically 2-5x larger than Parquet for same data

WHY JSON IS USEFUL:
1. SEMI-STRUCTURED: Handles nested data naturally
2. SCHEMA FLEXIBILITY: Different rows can have different fields
3. HUMAN-READABLE: Easy to inspect and debug
4. UNIVERSAL: Every language/system can read JSON
5. API COMPATIBILITY: REST APIs return JSON
6. SCHEMA EVOLUTION: New fields added without breaking readers
"""

# Write JSON
print("\n=== WRITING JSON ===")
start = time.time()
df.write.mode("overwrite").json("/shared/formats/json_data")
print(f"JSON write time: {time.time() - start:.3f}s")

# Write with compression
df.write.mode("overwrite") \
    .option("compression", "gzip") \
    .json("/shared/formats/json_gzip")

# Read JSON (schema inference by default - scans data!)
print("\n=== READING JSON (with inference) ===")
start = time.time()
df_json = spark.read.json("/shared/formats/json_data")
cnt = df_json.count()
print(f"JSON read + count time: {time.time() - start:.3f}s ({cnt} rows)")

# Read JSON with explicit schema (faster)
print("\n=== READING JSON (explicit schema) ===")
start = time.time()
df_json_schema = spark.read.schema(schema).json("/shared/formats/json_data")
cnt = df_json_schema.count()
print(f"JSON read (explicit) + count time: {time.time() - start:.3f}s ({cnt} rows)")

# JSON NO predicate pushdown
print("\n=== JSON NO PREDICATE PUSHDOWN ===")
spark.read.json("/shared/formats/json_data") \
    .filter(col("salary") > 60000).explain()

# JSON with nested data (its strength!)
print("\n=== JSON NESTED DATA (JSON's strength) ===")
nested_json_data = [
    (1, "Alice", {"street": "123 Main St", "city": "NYC", "zip": "10001"}),
    (2, "Bob", {"street": "456 Oak Ave", "city": "LA", "zip": "90001"}),
]
df_nested = spark.createDataFrame(nested_json_data, ["id", "name", "address"])
df_nested.write.mode("overwrite").json("/shared/formats/json_nested")
spark.read.json("/shared/formats/json_nested").show(truncate=False)
spark.read.json("/shared/formats/json_nested").printSchema()

"""
Important JSON options:
  .option("multiLine", "true")        # Multi-line JSON (not JSON Lines)
  .option("allowComments", "true")    # Allow // and /* */ comments
  .option("dateFormat", "yyyy-MM-dd") # Date parsing
  .option("timestampFormat", "...")    # Timestamp parsing
  .option("mode", "PERMISSIVE")       # Error handling
  .option("columnNameOfCorruptRecord", "_corrupt") # Store bad records
"""


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                         5. AVRO FORMAT                                       ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
"""
AVRO - Row-Based Binary Format (Apache)
=========================================

Architecture:
┌─────────────────────────────────────────────────────────┐
│ Avro File                                                │
├─────────────────────────────────────────────────────────┤
│ File Header                                              │
│   ├── Magic bytes ("Obj" + version)                     │
│   ├── Schema (JSON format, embedded in file)             │
│   └── Sync marker (16 bytes, unique per file)            │
├─────────────────────────────────────────────────────────┤
│ Data Block 1                                             │
│   ├── Count (number of objects in block)                 │
│   ├── Size (byte size of serialized objects)             │
│   ├── Serialized objects (binary, row-by-row)            │
│   └── Sync marker                                        │
├─────────────────────────────────────────────────────────┤
│ Data Block 2                                             │
│   ├── Count                                              │
│   ├── Size                                               │
│   ├── Serialized objects                                 │
│   └── Sync marker                                        │
└─────────────────────────────────────────────────────────┘

WHY AVRO:
1. SCHEMA EVOLUTION: Add/remove/rename fields without breaking readers
   - Forward compatibility: Old reader can read new writer's data
   - Backward compatibility: New reader can read old writer's data
   
2. COMPACT BINARY: Much smaller than JSON/CSV (no field names per row)
   Field names stored ONCE in header, data is just values

3. SPLITTABLE: Sync markers allow splitting for parallel processing

4. SCHEMA IN FILE: Self-describing, no external schema registry needed

5. RICH DATA TYPES: Supports unions, enums, fixed, maps, arrays

AVRO vs PARQUET:
- Avro is ROW-BASED -> better for write-heavy, full-row access
- Parquet is COLUMNAR -> better for read-heavy, column-selective queries
- Avro has better schema evolution support
- Parquet has better query performance (column pruning, pushdown)
- Avro is preferred for: Kafka messages, data serialization, streaming
- Parquet is preferred for: Analytics, data warehousing, Spark queries

WHEN TO USE AVRO:
- Kafka message serialization (row-at-a-time)
- Data that needs strong schema evolution
- Write-heavy workloads (append-only logs)
- When you need to read entire rows (not selective columns)
- Intermediate format between processing stages
"""

# Write Avro (requires spark-avro package)
print("\n=== WRITING AVRO ===")
start = time.time()
try:
    df.write.mode("overwrite").format("avro").save("/shared/formats/avro_data")
    print(f"Avro write time: {time.time() - start:.3f}s")
    
    # Read Avro
    print("\n=== READING AVRO ===")
    start = time.time()
    df_avro = spark.read.format("avro").load("/shared/formats/avro_data")
    cnt = df_avro.count()
    print(f"Avro read + count time: {time.time() - start:.3f}s ({cnt} rows)")
    
    # Avro has LIMITED predicate pushdown (only on partition columns)
    print("\n=== AVRO LIMITED PUSHDOWN ===")
    df_avro.filter(col("salary") > 60000).explain()
    
except Exception as e:
    print(f"Avro not available (need spark-avro package): {e}")
    print("Install with: --packages org.apache.spark:spark-avro_2.12:3.x.x")
    
    # Simulate Avro behavior description
    print("\nAvro would behave similarly to reading a row-based format:")
    print("- No column pruning (reads full rows)")
    print("- Limited predicate pushdown")
    print("- But much faster than CSV/JSON due to binary encoding")


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                    6. PERFORMANCE COMPARISON                                 ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

print("\n" + "=" * 70)
print("         COMPREHENSIVE FORMAT COMPARISON")
print("=" * 70)

"""
┌──────────────────────────────────────────────────────────────────────────────────┐
│                    FILE FORMAT PERFORMANCE COMPARISON                              │
├────────────┬──────────┬──────────┬──────────┬──────────┬──────────┬──────────────┤
│ Feature    │ Parquet  │ ORC      │ CSV      │ JSON     │ Avro     │ Winner       │
├────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────────┤
│ Storage    │ Columnar │ Columnar │ Row      │ Row      │ Row      │ Parquet/ORC  │
│ Layout     │          │          │          │          │          │              │
├────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────────┤
│ Encoding   │ Binary   │ Binary   │ Text     │ Text     │ Binary   │ Binary fmts  │
├────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────────┤
│ File Size  │ ★★★★★   │ ★★★★★   │ ★★       │ ★        │ ★★★★    │ Parquet/ORC  │
│ (smaller)  │ Smallest │ Smallest │ Large    │ Largest  │ Small    │              │
├────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────────┤
│ Read Speed │ ★★★★★   │ ★★★★★   │ ★★       │ ★★       │ ★★★★    │ Parquet/ORC  │
│ (full scan)│          │          │          │          │          │              │
├────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────────┤
│ Read Speed │ ★★★★★   │ ★★★★★   │ ★        │ ★        │ ★★       │ Parquet/ORC  │
│ (few cols) │ 10-100x  │ 10-100x │ Same     │ Same     │ Same     │              │
├────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────────┤
│ Write Speed│ ★★★★    │ ★★★★    │ ★★★★★   │ ★★★★    │ ★★★★★   │ CSV/Avro     │
│            │ Encoding │ Encoding │ Simple   │ Simple   │ Simple   │              │
├────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────────┤
│ Column     │ ✓ YES    │ ✓ YES    │ ✗ NO     │ ✗ NO     │ ✗ NO     │ Parquet/ORC  │
│ Pruning    │          │          │          │          │          │              │
├────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────────┤
│ Predicate  │ ✓ YES    │ ✓ YES    │ ✗ NO     │ ✗ NO     │ Partial  │ Parquet/ORC  │
│ Pushdown   │ (stats)  │ (stats+  │          │          │          │              │
│            │          │  bloom)  │          │          │          │              │
├────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────────┤
│ Schema     │ ✓ YES    │ ✓ YES    │ ✗ NO     │ Partial  │ ✓ YES    │ All binary   │
│ Embedded   │ (footer) │ (footer) │ (header  │ (infer)  │ (header) │              │
│            │          │          │  only)   │          │          │              │
├────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────────┤
│ Schema     │ ★★★     │ ★★★     │ ★        │ ★★★     │ ★★★★★   │ Avro         │
│ Evolution  │ Add cols │ Add cols │ Manual   │ Flexible │ Full     │              │
├────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────────┤
│ Compression│ ★★★★★   │ ★★★★★   │ ★★       │ ★★       │ ★★★★    │ Parquet/ORC  │
│ Ratio      │ Excellent│ Excellent│ Poor     │ Poor     │ Good     │              │
├────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────────┤
│ Splittable │ ✓ YES    │ ✓ YES    │ ✓ YES*   │ ✓ YES*   │ ✓ YES    │ All          │
│            │          │          │ *if not  │ *if not  │          │              │
│            │          │          │ compress │ compress │          │              │
├────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────────┤
│ Human      │ ✗ NO     │ ✗ NO     │ ✓ YES    │ ✓ YES    │ ✗ NO     │ CSV/JSON     │
│ Readable   │ (binary) │ (binary) │          │          │ (binary) │              │
├────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────────┤
│ Nested     │ ✓ YES    │ ✓ YES    │ ✗ NO     │ ✓ YES    │ ✓ YES    │ JSON/Avro    │
│ Data       │          │          │          │ (native) │          │              │
├────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────────┤
│ Streaming  │ ★★       │ ★★       │ ★★★     │ ★★★     │ ★★★★★   │ Avro         │
│ Use Case   │          │          │          │          │ (Kafka)  │              │
├────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────────┤
│ ACID       │ ✗ NO*    │ ✓ YES    │ ✗ NO     │ ✗ NO     │ ✗ NO     │ ORC (Hive)   │
│ Support    │ *Delta   │ (Hive)   │          │          │          │              │
├────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────────┤
│ Best For   │ Analytics│ Hive/    │ Inter-   │ APIs/    │ Kafka/   │              │
│            │ Spark    │ Analytics│ change   │ Logs     │ Streaming│              │
└────────────┴──────────┴──────────┴──────────┴──────────┴──────────┴──────────────┘


RELATIVE FILE SIZES (same 1000 rows):
  JSON (uncompressed):  ~150KB  (1.0x baseline - LARGEST)
  CSV (uncompressed):   ~80KB   (0.53x)
  Avro (uncompressed):  ~40KB   (0.27x)
  Parquet (snappy):     ~15KB   (0.10x)
  ORC (zlib):           ~12KB   (0.08x - SMALLEST)

RELATIVE READ SPEEDS (full table scan):
  Parquet:  1.0x  (baseline - FASTEST)
  ORC:      1.1x
  Avro:     2.0x
  CSV:      5-10x slower
  JSON:     5-15x slower

RELATIVE READ SPEEDS (2 columns out of 10):
  Parquet:  1.0x  (only reads 2 columns!)
  ORC:      1.1x  (only reads 2 columns!)
  Avro:     5x    (reads all columns, discards 8)
  CSV:      10x   (reads all columns as text, parses, discards 8)
  JSON:     15x   (reads all columns, parses JSON, discards 8)
"""


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                    7. READ/WRITE COMPARISON CODE                             ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

print("\n" + "=" * 70)
print("         READ/WRITE TIMING COMPARISON")
print("=" * 70)

# ============ WRITE COMPARISON ============
print("\n--- WRITE Performance ---")

formats_write = {
    "Parquet (Snappy)": lambda: df.write.mode("overwrite").parquet("/shared/formats/bench_parquet"),
    "ORC (Snappy)": lambda: df.write.mode("overwrite").orc("/shared/formats/bench_orc"),
    "CSV": lambda: df.write.mode("overwrite").option("header", "true").csv("/shared/formats/bench_csv"),
    "JSON": lambda: df.write.mode("overwrite").json("/shared/formats/bench_json"),
}

write_times = {}
for name, write_fn in formats_write.items():
    start = time.time()
    write_fn()
    elapsed = time.time() - start
    write_times[name] = elapsed
    print(f"  {name:20s}: {elapsed:.4f}s")

# ============ READ COMPARISON (Full Scan) ============
print("\n--- READ Performance (Full Scan - count()) ---")

formats_read = {
    "Parquet": lambda: spark.read.parquet("/shared/formats/bench_parquet").count(),
    "ORC": lambda: spark.read.orc("/shared/formats/bench_orc").count(),
    "CSV (infer)": lambda: spark.read.option("header", "true").option("inferSchema", "true").csv("/shared/formats/bench_csv").count(),
    "CSV (schema)": lambda: spark.read.option("header", "true").schema(schema).csv("/shared/formats/bench_csv").count(),
    "JSON": lambda: spark.read.json("/shared/formats/bench_json").count(),
}

read_times = {}
for name, read_fn in formats_read.items():
    start = time.time()
    read_fn()
    elapsed = time.time() - start
    read_times[name] = elapsed
    print(f"  {name:20s}: {elapsed:.4f}s")

# ============ READ COMPARISON (Column Pruning - 2 columns) ============
print("\n--- READ Performance (2 columns only - select + count) ---")

formats_selective = {
    "Parquet (2 cols)": lambda: spark.read.parquet("/shared/formats/bench_parquet").select("name", "salary").count(),
    "ORC (2 cols)": lambda: spark.read.orc("/shared/formats/bench_orc").select("name", "salary").count(),
    "CSV (2 cols)": lambda: spark.read.option("header", "true").schema(schema).csv("/shared/formats/bench_csv").select("name", "salary").count(),
    "JSON (2 cols)": lambda: spark.read.json("/shared/formats/bench_json").select("name", "salary").count(),
}

for name, read_fn in formats_selective.items():
    start = time.time()
    read_fn()
    elapsed = time.time() - start
    print(f"  {name:20s}: {elapsed:.4f}s")

# ============ READ COMPARISON (With Filter - Predicate Pushdown) ============
print("\n--- READ Performance (with filter - predicate pushdown) ---")

formats_filter = {
    "Parquet (filter)": lambda: spark.read.parquet("/shared/formats/bench_parquet").filter(col("salary") > 60000).count(),
    "ORC (filter)": lambda: spark.read.orc("/shared/formats/bench_orc").filter(col("salary") > 60000).count(),
    "CSV (filter)": lambda: spark.read.option("header", "true").schema(schema).csv("/shared/formats/bench_csv").filter(col("salary") > 60000).count(),
    "JSON (filter)": lambda: spark.read.json("/shared/formats/bench_json").filter(col("salary") > 60000).count(),
}

for name, read_fn in formats_filter.items():
    start = time.time()
    read_fn()
    elapsed = time.time() - start
    print(f"  {name:20s}: {elapsed:.4f}s")


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                    8. COMPRESSION COMPARISON                                 ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

print("\n" + "=" * 70)
print("         COMPRESSION CODEC COMPARISON")
print("=" * 70)

"""
┌────────────────┬───────────────┬───────────────┬──────────────────────────────┐
│ Codec          │ Compression   │ Speed         │ Best For                     │
│                │ Ratio         │ (decompress)  │                              │
├────────────────┼───────────────┼───────────────┼──────────────────────────────┤
│ None           │ 1.0x (none)   │ Fastest       │ When CPU is bottleneck       │
│ Snappy         │ ~2-3x         │ Very Fast     │ DEFAULT. Best balance.       │
│ LZ4            │ ~2-3x         │ Very Fast     │ Similar to Snappy            │
│ GZIP/ZLIB      │ ~4-5x         │ Slow          │ Storage-constrained, archive │
│ ZSTD           │ ~4-5x         │ Fast          │ Best ratio with good speed   │
│ Brotli         │ ~5-6x         │ Slow          │ Web content, rarely in Spark │
└────────────────┴───────────────┴───────────────┴──────────────────────────────┘

RECOMMENDATIONS:
- Hot data (frequently queried): Snappy or LZ4 (fast decompression)
- Cold data (archival): ZSTD or GZIP (better compression)
- Streaming: Snappy (low latency)
- Default: Snappy (Spark's default for Parquet)

SPLITTABILITY:
- Snappy (block): Splittable when used with Parquet/ORC (container handles splitting)
- GZIP: NOT splittable as standalone file (but OK within Parquet/ORC)
- ZSTD: Splittable with frame format
- LZ4: Splittable with frame format

IMPORTANT: Compression in Parquet/ORC is per-column-chunk, so the file
is always splittable regardless of codec (the container format handles it).
Compression only affects splittability for raw text files (CSV, JSON).
"""

# Write Parquet with different compression codecs
compressions = ["none", "snappy", "gzip", "zstd", "lz4"]
print("\n--- Parquet Write with Different Compression ---")

for codec in compressions:
    try:
        start = time.time()
        df.write.mode("overwrite") \
            .option("compression", codec) \
            .parquet(f"/shared/formats/parquet_{codec}")
        elapsed = time.time() - start
        print(f"  Parquet ({codec:6s}): write={elapsed:.4f}s")
    except Exception as e:
        print(f"  Parquet ({codec:6s}): not available - {e}")

# Read back with different compression
print("\n--- Parquet Read with Different Compression ---")
for codec in compressions:
    try:
        start = time.time()
        spark.read.parquet(f"/shared/formats/parquet_{codec}").count()
        elapsed = time.time() - start
        print(f"  Parquet ({codec:6s}): read={elapsed:.4f}s")
    except Exception:
        pass


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                    9. BEST PRACTICES & DECISION GUIDE                        ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

"""
DECISION FLOWCHART: Which format to use?
==========================================

Is it for Spark/analytical queries?
├── YES → Is it a data warehouse / lake?
│   ├── YES → PARQUET (with Delta Lake / Iceberg for ACID)
│   └── NO → PARQUET (standard)
└── NO → Is it for streaming / Kafka?
    ├── YES → AVRO (schema evolution, compact, row-based)
    └── NO → Is it for data exchange with external systems?
        ├── YES → Is the consumer technical?
        │   ├── YES → PARQUET or AVRO
        │   └── NO → CSV (human-readable, universal)
        └── NO → Is it semi-structured / nested?
            ├── YES → JSON or AVRO
            └── NO → PARQUET


BEST PRACTICES:
================

1. PARQUET BEST PRACTICES:
   - Use Snappy compression (default, fast)
   - Partition by date/category for partition pruning
   - Target 128MB-1GB file sizes
   - Use explicit schema on read (avoid inference)
   - Enable predicate pushdown (default in Spark)

2. ORC BEST PRACTICES:
   - Use with Hive ecosystem
   - Enable bloom filters for frequently filtered columns
   - Use ZLIB for better compression (default for ORC)
   - Good for ACID operations in Hive

3. CSV BEST PRACTICES:
   - ALWAYS provide explicit schema (avoid inferSchema)
   - Use header=true for clarity
   - Handle multiLine if data has newlines in values
   - Set appropriate mode (PERMISSIVE/FAILFAST)
   - Convert to Parquet ASAP after ingestion

4. JSON BEST PRACTICES:
   - Use JSON Lines format (one object per line)
   - Provide explicit schema when possible
   - Convert to Parquet for repeated analytical queries
   - Good for one-time ingestion of API data

5. AVRO BEST PRACTICES:
   - Use with Kafka (native Confluent support)
   - Leverage schema registry for evolution
   - Good for write-heavy append workloads
   - Convert to Parquet for analytical queries


INTERVIEW ANSWER TEMPLATE:
============================
"For our Spark analytical workload, I would use Parquet because:
1. Columnar format enables column pruning - we only read needed columns
2. Predicate pushdown skips entire row groups using min/max statistics
3. Excellent compression due to similar values in columns being together
4. Schema is embedded - no extra job for schema inference
5. It's Spark's native format with the best optimization support

For streaming data from Kafka, I would use Avro because:
1. Row-based format is efficient for write-heavy, record-at-a-time access
2. Strong schema evolution support (add/remove fields safely)
3. Compact binary encoding (much smaller than JSON)
4. Native integration with Confluent Schema Registry

I would avoid CSV/JSON for large-scale processing because:
1. No column pruning (must read all columns)
2. No predicate pushdown (must scan all rows)
3. Text parsing overhead
4. Poor compression (text doesn't compress as well as binary)
5. Schema inference requires an extra scan job"
"""

# Final write
df.write.mode("overwrite").parquet("/shared/formats/final_parquet")

print("\n" + "=" * 70)
print("  FILE FORMATS DEEP DIVE COMPLETE")
print("  All outputs written to /shared/formats/")
print("=" * 70)

spark.stop()
