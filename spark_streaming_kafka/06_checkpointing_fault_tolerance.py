"""
Topic: Checkpointing and Fault Tolerance
==========================================

Checkpointing enables exactly-once processing and failure recovery.

Spark UI Behavior:
- Checkpoint writes happen at the END of each micro-batch.
- No visible job for checkpointing itself (happens within batch job).
- After restart: Spark reads checkpoint, resumes from last committed offset.
- In Streaming tab: "Last Batch ID" shows progress.

Key Interview Points:
- Checkpoint stores: offsets, state, and commit log.
- Without checkpoint: On restart, streaming starts fresh (data loss/duplication).
- With checkpoint: On restart, resumes exactly where it left off.
- Checkpoint location must be on reliable storage (HDFS, S3, not local disk).
- Each streaming query needs its OWN unique checkpoint location.
- Changing query logic may require deleting checkpoint (breaking change).
- Exactly-once = checkpoint + idempotent sink + replayable source.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder \
    .appName("06_Checkpointing") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# ============ WHAT CHECKPOINT STORES ============
"""
Checkpoint Directory Structure:
/shared/checkpoints/my-query/
├── offsets/                    # Kafka offsets per batch
│   ├── 0                      # Batch 0 offsets
│   ├── 1                      # Batch 1 offsets
│   ├── 2                      # Batch 2 offsets
│   └── ...
├── commits/                   # Which batches are committed (completed)
│   ├── 0
│   ├── 1
│   └── ...
├── state/                     # Stateful operation state (aggregations, joins)
│   ├── 0/                     # State store for operator 0
│   │   ├── partition-0/
│   │   ├── partition-1/
│   │   └── ...
│   └── ...
├── metadata                   # Query metadata (ID, run ID)
└── sources/                   # Source-specific metadata
    └── 0/                     # Source 0 (Kafka)

THREE COMPONENTS:
1. OFFSETS: Which Kafka offsets to read in each batch
   - Batch 0: {"orders":{"0":0,"1":0,"2":0}}  (start from 0)
   - Batch 1: {"orders":{"0":5,"1":3,"2":4}}  (start from here)
   
2. COMMITS: Which batches have been fully processed and written to sink
   - If batch 5 is committed, we know offsets up to batch 5 are done
   
3. STATE: For stateful operations (groupBy, window, join)
   - Running aggregation values
   - Window state
   - Stream-stream join state
"""

# ============ HOW FAULT TOLERANCE WORKS ============
"""
SCENARIO: Streaming job fails mid-batch

Timeline:
  Batch 0: Read offsets 0-4 → Process → Write to sink → Commit ✓
  Batch 1: Read offsets 5-9 → Process → Write to sink → Commit ✓
  Batch 2: Read offsets 10-14 → Process → CRASH! ✗
  
On Restart:
  1. Spark reads checkpoint
  2. Finds: Batch 1 committed, Batch 2 NOT committed
  3. Replays Batch 2 from offset 10 (Kafka is replayable!)
  4. Processes and writes to sink
  5. Commits Batch 2
  6. Continues with Batch 3

EXACTLY-ONCE GUARANTEE requires:
  1. Replayable source (Kafka ✓ - can re-read from any offset)
  2. Checkpointing (tracks progress)
  3. Idempotent sink (writing same data twice = same result)
     - File sink: Uses atomic rename (write to temp, rename on commit)
     - Kafka sink: Idempotent producer
     - Database: Use upsert/merge instead of insert
"""

# ============ CHECKPOINT CONFIGURATION ============
"""
# REQUIRED: Set checkpoint location for every streaming query

query = df_stream \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "output-topic") \
    .option("checkpointLocation", "/shared/checkpoints/my-query-v1") \
    .outputMode("append") \
    .start()

RULES:
1. Each query MUST have a UNIQUE checkpoint location
2. Location must be on RELIABLE storage (HDFS, S3, ADLS, GCS)
3. Do NOT use local filesystem in production (lost on node failure)
4. Do NOT share checkpoint between different queries
5. Do NOT delete checkpoint unless you want to reprocess from scratch
"""

# ============ WHEN TO DELETE CHECKPOINT ============
"""
You MUST delete checkpoint when making INCOMPATIBLE changes:

COMPATIBLE (checkpoint preserved):
✓ Adding new columns to output
✓ Changing filter conditions
✓ Adding new transformations that don't affect state
✓ Changing trigger interval
✓ Changing number of shuffle partitions (with care)

INCOMPATIBLE (must delete checkpoint):
✗ Changing aggregation columns (groupBy keys)
✗ Changing window duration
✗ Changing watermark duration
✗ Changing output mode
✗ Changing source topic/schema fundamentally
✗ Adding/removing stateful operations

After deleting checkpoint:
- Query starts fresh from startingOffsets
- All state is lost
- May cause duplicate processing of already-processed data
- Plan for this in your sink (idempotent writes)
"""

# ============ EXACTLY-ONCE SEMANTICS ============
"""
END-TO-END EXACTLY-ONCE:

Source (Kafka) → Spark Processing → Sink

1. SOURCE must be REPLAYABLE:
   - Kafka: ✓ (can re-read from any offset)
   - Files: ✓ (files are immutable)
   - Socket: ✗ (data lost once read)

2. PROCESSING must be CHECKPOINTED:
   - Spark tracks offsets and state
   - On failure, replays from last committed batch

3. SINK must be IDEMPOTENT:
   - File sink: ✓ (atomic write with temp files)
   - Kafka sink: ✓ (with idempotent producer)
   - Database: Use UPSERT/MERGE (not INSERT)
   - Console: ✗ (may print duplicates on retry)

If ANY component breaks the chain, you get AT-LEAST-ONCE (duplicates possible).
"""

# ============ PRACTICAL PATTERNS ============

# Pattern 1: Basic streaming with checkpoint
"""
spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "input-topic") \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .writeStream \
    .format("parquet") \
    .option("path", "/shared/output/events") \
    .option("checkpointLocation", "/shared/checkpoints/events-v1") \
    .trigger(processingTime="1 minute") \
    .outputMode("append") \
    .start()
"""

# Pattern 2: Stateful aggregation with checkpoint
"""
spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .load() \
    .select(from_json(col("value").cast("string"), order_schema).alias("order")) \
    .select("order.*") \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(window("event_time", "5 minutes"), "product") \
    .agg(sum("amount").alias("revenue")) \
    .writeStream \
    .format("parquet") \
    .option("path", "/shared/output/revenue_windows") \
    .option("checkpointLocation", "/shared/checkpoints/revenue-v1") \
    .trigger(processingTime="1 minute") \
    .outputMode("append") \
    .start()
"""

# ============ MONITORING STREAMING QUERIES ============
"""
# Get active queries
spark.streams.active

# Get query status
query.status
# Returns: {'message': 'Processing new data', 'isDataAvailable': True, ...}

# Get recent progress
query.recentProgress
# Returns list of StreamingQueryProgress objects with metrics:
# - inputRowsPerSecond
# - processedRowsPerSecond
# - batchDuration
# - stateOperators (rows in state, memory used)

# Get last progress
query.lastProgress

# Stop query gracefully
query.stop()

# Wait for termination
query.awaitTermination()
query.awaitTermination(timeout=60)  # Wait max 60 seconds
"""

print("=== Checkpointing Summary ===")
print("""
1. ALWAYS set checkpointLocation for production queries
2. Use reliable storage (HDFS/S3/GCS), never local disk
3. Each query needs UNIQUE checkpoint location
4. Delete checkpoint only for incompatible schema/logic changes
5. Exactly-once = replayable source + checkpoint + idempotent sink
""")

# Write demo
spark.createDataFrame([("checkpoint_demo",)], ["info"]) \
    .write.mode("overwrite").parquet("/shared/checkpoint_demo")

spark.stop()
