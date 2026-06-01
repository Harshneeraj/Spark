"""
Topic: Reading from Kafka with Spark Structured Streaming
==========================================================

How to configure and read streaming data from Kafka topics.

Spark UI Behavior:
- Each micro-batch triggers 1 job.
- Read stage: tasks = number of Kafka partitions being consumed.
- If query has groupBy/join: additional shuffle stages per batch.
- Streaming tab shows: inputRowsPerSecond, processedRowsPerSecond, batchDuration.
- Each batch's job is visible in Jobs tab with incrementing IDs.

Key Interview Points:
- spark-sql-kafka connector is required (not bundled by default).
- Kafka messages arrive as binary key/value - must deserialize.
- startingOffsets: earliest, latest, or specific JSON offsets.
- failOnDataLoss: whether to fail if Kafka data is deleted.
- maxOffsetsPerTrigger: rate limiting (backpressure).
- Kafka consumer properties prefixed with "kafka." in options.
- One Spark task per Kafka partition in the read stage.

Dependencies:
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, expr,
    window, count, sum, avg, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, LongType
)

spark = SparkSession.builder \
    .appName("03_Reading_From_Kafka") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# ============ READING FROM KAFKA (STREAMING) ============
"""
# PRODUCTION CODE - Reading from Kafka as a stream

df_kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092") \
    .option("subscribe", "orders,user-events") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 10000) \
    .option("failOnDataLoss", "false") \
    .option("kafka.group.id", "spark-streaming-app") \
    .load()

# Schema of df_kafka_stream:
# key: binary
# value: binary
# topic: string
# partition: int
# offset: long
# timestamp: timestamp
# timestampType: int
"""

# ============ ALL KAFKA READ OPTIONS ============
"""
┌─────────────────────────────────┬─────────────────────────────────────────────┐
│ Option                          │ Description                                 │
├─────────────────────────────────┼─────────────────────────────────────────────┤
│ kafka.bootstrap.servers         │ Kafka broker addresses (REQUIRED)           │
│ subscribe                       │ Topic(s) to subscribe (comma-separated)     │
│ subscribePattern                │ Topic pattern regex (e.g., "events-.*")     │
│ assign                          │ Specific partitions JSON                    │
├─────────────────────────────────┼─────────────────────────────────────────────┤
│ startingOffsets                  │ "earliest", "latest", or JSON offsets       │
│ endingOffsets                    │ For batch reads only                        │
│ startingTimestamp                │ Start from timestamp (Kafka 0.10.1+)       │
├─────────────────────────────────┼─────────────────────────────────────────────┤
│ maxOffsetsPerTrigger            │ Rate limit: max messages per trigger        │
│ minOffsetsPerTrigger            │ Min messages before triggering (Spark 3.3+) │
│ maxTriggerDelay                 │ Max wait time for minOffsets                │
├─────────────────────────────────┼─────────────────────────────────────────────┤
│ failOnDataLoss                  │ Fail if offsets are out of range (true)     │
│ kafka.group.id                  │ Consumer group ID                           │
│ kafka.security.protocol         │ PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL   │
│ kafka.sasl.mechanism            │ PLAIN, SCRAM-SHA-256, GSSAPI               │
│ includeHeaders                  │ Include Kafka headers (false)               │
└─────────────────────────────────┴─────────────────────────────────────────────┘

SUBSCRIBE OPTIONS (use exactly ONE):
1. subscribe: "topic1,topic2" - specific topics
2. subscribePattern: "events-.*" - regex pattern
3. assign: '{"topic1":[0,1],"topic2":[2,3]}' - specific partitions
"""

# ============ STARTING OFFSETS ============
"""
startingOffsets options:

1. "earliest" - Read from beginning of topic (all historical data)
   Use for: First run, backfill, reprocessing

2. "latest" - Read only NEW messages (skip existing)
   Use for: Real-time only, don't need history
   DEFAULT for streaming

3. Specific offsets (JSON):
   '{"topic1":{"0":100,"1":200},"topic2":{"0":-2}}'
   -2 = earliest, -1 = latest
   Use for: Resume from specific point

NOTE: After first run with checkpointing, startingOffsets is IGNORED.
Spark resumes from checkpointed offsets automatically.
"""

# ============ BATCH READ FROM KAFKA ============
"""
# Read Kafka as a BATCH (not streaming) - useful for backfill/testing

df_kafka_batch = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

# This reads ALL messages between startingOffsets and endingOffsets
# Useful for: Testing, backfill, one-time processing
"""

# ============ SIMULATED KAFKA READ + DESERIALIZATION ============

# Simulate raw Kafka data (as it would come from readStream)
raw_messages = [
    ("user_1", '{"order_id":"ORD001","user_id":"user_1","product":"laptop","amount":1200.00,"ts":"2024-01-01 10:00:00"}', "orders", 0, 0, "2024-01-01 10:00:00"),
    ("user_2", '{"order_id":"ORD002","user_id":"user_2","product":"phone","amount":800.00,"ts":"2024-01-01 10:00:05"}', "orders", 1, 0, "2024-01-01 10:00:05"),
    ("user_1", '{"order_id":"ORD003","user_id":"user_1","product":"tablet","amount":500.00,"ts":"2024-01-01 10:00:10"}', "orders", 0, 1, "2024-01-01 10:00:10"),
    ("user_3", '{"order_id":"ORD004","user_id":"user_3","product":"laptop","amount":1300.00,"ts":"2024-01-01 10:00:15"}', "orders", 2, 0, "2024-01-01 10:00:15"),
    ("user_2", '{"order_id":"ORD005","user_id":"user_2","product":"headphones","amount":200.00,"ts":"2024-01-01 10:00:20"}', "orders", 1, 1, "2024-01-01 10:00:20"),
    ("user_4", '{"order_id":"ORD006","user_id":"user_4","product":"monitor","amount":600.00,"ts":"2024-01-01 10:01:00"}', "orders", 0, 2, "2024-01-01 10:01:00"),
    ("user_1", '{"order_id":"ORD007","user_id":"user_1","product":"keyboard","amount":150.00,"ts":"2024-01-01 10:01:30"}', "orders", 0, 3, "2024-01-01 10:01:30"),
    ("user_5", '{"order_id":"ORD008","user_id":"user_5","product":"mouse","amount":50.00,"ts":"2024-01-01 10:02:00"}', "orders", 1, 2, "2024-01-01 10:02:00"),
]

df_raw = spark.createDataFrame(raw_messages, 
    ["key", "value", "topic", "partition", "offset", "timestamp"])

print("=== Raw Kafka Messages (simulated) ===")
df_raw.show(truncate=False)

# ============ STEP-BY-STEP DESERIALIZATION ============

# Step 1: Define the schema of your JSON payload
order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("ts", StringType(), True)
])

# Step 2: Cast binary to string and parse JSON
df_parsed = df_raw.select(
    col("key").cast("string").alias("kafka_key"),
    from_json(col("value").cast("string"), order_schema).alias("order"),
    col("topic"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
    col("timestamp").alias("kafka_timestamp")
)

# Step 3: Flatten the struct
df_orders = df_parsed.select(
    "kafka_key",
    "order.order_id",
    "order.user_id",
    "order.product",
    "order.amount",
    col("order.ts").cast("timestamp").alias("event_time"),
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp"
)

print("=== Parsed Orders ===")
df_orders.show(truncate=False)

# ============ MULTIPLE TOPICS ============
"""
# Subscribe to multiple topics
df_multi = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders,payments,refunds") \
    .load()

# Use 'topic' column to differentiate
df_orders = df_multi.filter(col("topic") == "orders")
df_payments = df_multi.filter(col("topic") == "payments")

# Or use subscribePattern for dynamic topics
df_pattern = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribePattern", "events-.*") \
    .load()
"""

# ============ RATE LIMITING (BACKPRESSURE) ============
"""
maxOffsetsPerTrigger controls how many messages Spark reads per batch:

Without rate limiting:
  Spark reads ALL available messages -> may overwhelm processing
  
With rate limiting:
  .option("maxOffsetsPerTrigger", 10000)
  Spark reads at most 10000 messages per trigger across all partitions
  
  If topic has 4 partitions: ~2500 messages per partition per batch

Use when:
- Initial backfill (don't want to read millions at once)
- Processing is slower than ingestion rate
- Want predictable batch sizes
"""

# Write demo output
df_orders.write.mode("overwrite").parquet("/shared/kafka_orders_parsed")

spark.stop()
