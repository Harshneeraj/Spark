"""
Topic: Streaming Performance Optimization
============================================

Tuning Spark Structured Streaming for production workloads.

Spark UI Behavior:
- Monitor: Streaming tab -> Input Rate vs Processing Rate.
- If Processing Rate < Input Rate: BACKLOG building up!
- Batch Duration should be < Trigger Interval.
- State size visible in streaming progress metrics.
- Shuffle stages per batch = same optimization as batch Spark.

Key Interview Points:
- Processing rate must exceed input rate (otherwise backlog grows).
- Reduce batch processing time: fewer shuffles, broadcast joins, proper partitioning.
- Kafka partitions = initial parallelism (more partitions = more parallel reads).
- State management: Use RocksDB for large state, watermark for cleanup.
- Backpressure: maxOffsetsPerTrigger to control input rate.
- Trigger interval: Balance latency vs throughput.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("12_Streaming_Optimizations") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# ============ OPTIMIZATION CHECKLIST ============
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║              STREAMING OPTIMIZATION CHECKLIST                                ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  1. KAFKA PARALLELISM                                                        ║
║  ─────────────────────                                                       ║
║  ✓ Kafka partitions >= Spark executor cores (for read parallelism)          ║
║  ✓ More Kafka partitions = more parallel consumers                          ║
║  ✓ Key distribution should be even (avoid hot partitions)                   ║
║  ✓ Consider repartition() after read if Kafka partitions are few            ║
║                                                                              ║
║  2. SHUFFLE OPTIMIZATION                                                     ║
║  ─────────────────────                                                       ║
║  ✓ Reduce spark.sql.shuffle.partitions for streaming (default 200 too high) ║
║  ✓ Typical: 2-4x number of cores for streaming                             ║
║  ✓ Use broadcast join for small lookup tables                               ║
║  ✓ Minimize number of shuffles per batch                                    ║
║                                                                              ║
║  3. STATE MANAGEMENT                                                         ║
║  ─────────────────────                                                       ║
║  ✓ Always use watermark for stateful operations                             ║
║  ✓ Use RocksDB state store for large state (Spark 3.2+)                    ║
║  ✓ Monitor state size (numRowsTotal in progress)                            ║
║  ✓ Keep watermark as short as business allows                               ║
║  ✓ Use time constraints in stream-stream joins                              ║
║                                                                              ║
║  4. TRIGGER TUNING                                                           ║
║  ─────────────────────                                                       ║
║  ✓ processingTime should be > actual batch processing time                  ║
║  ✓ Shorter trigger = lower latency but more overhead                        ║
║  ✓ Longer trigger = higher throughput but higher latency                    ║
║  ✓ Use availableNow for catch-up/backfill scenarios                         ║
║                                                                              ║
║  5. BACKPRESSURE                                                             ║
║  ─────────────────────                                                       ║
║  ✓ Set maxOffsetsPerTrigger to prevent overwhelming processing              ║
║  ✓ Monitor input rate vs processing rate                                    ║
║  ✓ If backlog grows: increase resources or reduce processing complexity     ║
║                                                                              ║
║  6. SERIALIZATION                                                            ║
║  ─────────────────────                                                       ║
║  ✓ Parse JSON once, cache schema (don't infer per batch)                    ║
║  ✓ Use Avro with schema registry for efficient deserialization              ║
║  ✓ Avoid complex nested JSON if possible                                    ║
║                                                                              ║
║  7. SINK OPTIMIZATION                                                        ║
║  ─────────────────────                                                       ║
║  ✓ Use foreachBatch for batch-level optimizations                           ║
║  ✓ Batch database writes (not row-by-row)                                   ║
║  ✓ Use connection pooling for external systems                              ║
║  ✓ Coalesce before file writes (avoid small files)                          ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

# ============ KEY CONFIGURATIONS FOR STREAMING ============
"""
# Shuffle partitions (CRITICAL - reduce from default 200!)
spark.sql.shuffle.partitions = 8  # 2-4x cores for streaming

# State store backend (use RocksDB for large state)
spark.sql.streaming.stateStore.providerClass = 
    org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider

# Checkpoint interval for state
spark.sql.streaming.stateStore.minDeltasForSnapshot = 10

# Kafka consumer configs
kafka.fetch.min.bytes = 1          # Min data to fetch (increase for throughput)
kafka.fetch.max.wait.ms = 500      # Max wait for min.bytes
kafka.max.partition.fetch.bytes = 1048576  # Max per partition per fetch
kafka.max.poll.records = 500       # Max records per poll

# Memory for streaming
spark.sql.streaming.metricsEnabled = true  # Enable metrics
spark.executor.memory = 4g
spark.driver.memory = 2g
"""

# ============ MONITORING AND ALERTING ============
"""
KEY METRICS TO MONITOR:

1. inputRowsPerSecond: Rate of incoming data
   Alert if: Suddenly drops to 0 (source issue)

2. processedRowsPerSecond: Rate of processing
   Alert if: processedRows < inputRows (falling behind!)

3. batchDuration: Time to process each batch
   Alert if: batchDuration > trigger interval (can't keep up)

4. numRowsTotal (state): Total keys in state
   Alert if: Growing unbounded (missing watermark!)

5. memoryUsedBytes (state): Memory used by state
   Alert if: Approaching executor memory limit

6. numInputRows: Rows in current batch
   Alert if: 0 for extended period (no data flowing)

MONITORING CODE:
query = df.writeStream...start()

# Check progress
progress = query.lastProgress
print(f"Input rate: {progress['inputRowsPerSecond']}")
print(f"Process rate: {progress['processedRowsPerSecond']}")
print(f"Batch duration: {progress['batchDuration']}ms")
if progress.get('stateOperators'):
    state = progress['stateOperators'][0]
    print(f"State rows: {state['numRowsTotal']}")
    print(f"State memory: {state['memoryUsedBytes']} bytes")
"""

# ============ COMMON STREAMING ISSUES AND FIXES ============
"""
ISSUE 1: Batch duration increasing over time
─────────────────────────────────────────────
Cause: State growing unbounded (no watermark)
Fix: Add watermark to bound state
Monitor: numRowsTotal in state metrics

ISSUE 2: Processing rate < Input rate (falling behind)
─────────────────────────────────────────────────────
Cause: Processing too slow for data volume
Fix: 
  - Increase parallelism (more executors/cores)
  - Reduce shuffle partitions
  - Use broadcast joins
  - Simplify transformations
  - Increase Kafka partitions

ISSUE 3: High GC pauses in streaming
─────────────────────────────────────
Cause: Large state in JVM heap
Fix:
  - Switch to RocksDB state store (off-heap)
  - Reduce watermark duration
  - Increase executor memory
  - Use G1GC

ISSUE 4: Small files in file sink
─────────────────────────────────
Cause: Each micro-batch writes small files
Fix:
  - Use foreachBatch with coalesce
  - Increase trigger interval
  - Use Delta Lake (auto-compaction)
  - Run periodic compaction job

ISSUE 5: Checkpoint taking too long
────────────────────────────────────
Cause: Large state being checkpointed
Fix:
  - Use RocksDB (incremental checkpointing)
  - Reduce state size (shorter watermark)
  - Use faster storage for checkpoint (SSD, local HDFS)

ISSUE 6: Data loss after restart
─────────────────────────────────
Cause: Checkpoint on unreliable storage / deleted
Fix:
  - Use HDFS/S3/GCS for checkpoint
  - Never delete checkpoint in production
  - Use failOnDataLoss=false for graceful handling
"""

# ============ PRODUCTION CONFIGURATION TEMPLATE ============
"""
# Production streaming job configuration

spark = SparkSession.builder \
    .appName("production-streaming-job") \
    .config("spark.sql.shuffle.partitions", "16") \
    .config("spark.sql.streaming.stateStore.providerClass",
        "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider") \
    .config("spark.sql.streaming.metricsEnabled", "true") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.instances", "5") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

query = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092,broker3:9092") \
    .option("subscribe", "input-events") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 100000) \
    .option("failOnDataLoss", "false") \
    .option("kafka.group.id", "spark-prod-consumer") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("event")) \
    .select("event.*") \
    .withWatermark("event_time", "15 minutes") \
    .groupBy(window("event_time", "5 minutes"), "category") \
    .agg(count("*").alias("count"), sum("amount").alias("revenue")) \
    .writeStream \
    .format("parquet") \
    .option("path", "/data/warehouse/event_aggregates") \
    .option("checkpointLocation", "hdfs:///checkpoints/event-agg-v1") \
    .trigger(processingTime="1 minute") \
    .outputMode("append") \
    .start()

query.awaitTermination()
"""

print("=== Streaming Optimization Summary ===")
print("""
Top 5 optimizations:
1. Reduce shuffle partitions (200 -> 8-16 for streaming)
2. Use broadcast joins for lookup tables
3. Always set watermark for stateful operations
4. Use RocksDB state store for large state
5. Set maxOffsetsPerTrigger for backpressure control
""")

spark.stop()
