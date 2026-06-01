"""
Topic: Kafka Basics and Architecture (for Spark Integration)
=============================================================

Understanding Kafka is essential before integrating with Spark Streaming.

Key Interview Points:
- Kafka is a distributed event streaming platform.
- Data is organized into Topics -> Partitions -> Offsets.
- Producers write to topics, Consumers read from topics.
- Spark acts as a Kafka CONSUMER (and optionally PRODUCER).
- Each Kafka partition maps to a Spark partition (parallelism!).
- Offsets track position in each partition (like a bookmark).
- Consumer Groups allow parallel consumption.

Kafka Architecture:
┌─────────────────────────────────────────────────────────────────────┐
│                        KAFKA CLUSTER                                  │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  Topic: "orders"                                                      │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │ Partition 0: [msg0][msg1][msg2][msg3][msg4][msg5]...        │    │
│  │              offset: 0    1     2     3     4     5          │    │
│  ├─────────────────────────────────────────────────────────────┤    │
│  │ Partition 1: [msg0][msg1][msg2][msg3]...                    │    │
│  │              offset: 0    1     2     3                      │    │
│  ├─────────────────────────────────────────────────────────────┤    │
│  │ Partition 2: [msg0][msg1][msg2][msg3][msg4]...              │    │
│  │              offset: 0    1     2     3     4                │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                       │
│  Brokers: [Broker 1] [Broker 2] [Broker 3]                          │
│  ZooKeeper / KRaft: Cluster coordination                             │
│                                                                       │
└─────────────────────────────────────────────────────────────────────┘

         │                                    ▲
         │ Produce                            │ Consume
         ▼                                    │
┌──────────────┐                    ┌──────────────────┐
│  Producers   │                    │  Spark Streaming │
│  (Apps/ETL)  │                    │  (Consumer)      │
└──────────────┘                    └──────────────────┘
"""

# This file is documentation + batch simulation (no live Kafka needed)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

spark = SparkSession.builder \
    .appName("02_Kafka_Basics") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# ============ KAFKA CORE CONCEPTS ============
"""
1. TOPIC:
   - Logical channel/category for messages
   - Like a "table" in a database
   - Examples: "orders", "user-events", "page-views"
   - Topics are split into partitions for parallelism

2. PARTITION:
   - Ordered, immutable sequence of messages
   - Each message has a unique OFFSET within partition
   - Partitions enable parallel processing
   - Messages with same KEY go to same partition (ordering guarantee per key)
   - Number of partitions = max parallelism for consumers

3. OFFSET:
   - Sequential ID for each message within a partition
   - Consumers track their position using offsets
   - Spark checkpoints offsets for exactly-once processing
   - Types: earliest (beginning), latest (end), specific offset

4. CONSUMER GROUP:
   - Group of consumers that share work
   - Each partition assigned to exactly ONE consumer in the group
   - Spark Streaming acts as a consumer group
   - Group ID determines offset tracking

5. KEY:
   - Optional message key
   - Determines which partition a message goes to (hash(key) % num_partitions)
   - Messages with same key are always in same partition (ordering!)
   - Example: user_id as key ensures all events for a user are ordered

6. VALUE:
   - The actual message payload
   - Usually JSON, Avro, or Protobuf serialized
   - Spark reads this as binary and you deserialize it

7. BROKER:
   - A Kafka server that stores data and serves clients
   - Multiple brokers form a cluster
   - Each partition has a leader broker and replica brokers
"""

# ============ WHAT SPARK READS FROM KAFKA ============
"""
When Spark reads from Kafka, each message has these columns:

┌──────────┬──────────┬─────────────────────────────────────────────┐
│ Column   │ Type     │ Description                                 │
├──────────┼──────────┼─────────────────────────────────────────────┤
│ key      │ binary   │ Message key (needs casting to string)       │
│ value    │ binary   │ Message payload (needs deserialization)      │
│ topic    │ string   │ Kafka topic name                            │
│ partition│ int      │ Kafka partition number                       │
│ offset   │ long     │ Message offset within partition              │
│ timestamp│ timestamp│ Message timestamp                            │
│ timestampType│ int  │ 0=CreateTime, 1=LogAppendTime               │
└──────────┴──────────┴─────────────────────────────────────────────┘

IMPORTANT: key and value are BINARY! You must cast/deserialize them.
"""

# Simulate what Kafka messages look like in Spark
kafka_messages = [
    (b"user_1", b'{"user_id":"user_1","event":"click","page":"home","ts":"2024-01-01 10:00:00"}', 
     "user-events", 0, 0, "2024-01-01 10:00:00"),
    (b"user_2", b'{"user_id":"user_2","event":"purchase","page":"cart","ts":"2024-01-01 10:00:01"}',
     "user-events", 1, 0, "2024-01-01 10:00:01"),
    (b"user_1", b'{"user_id":"user_1","event":"click","page":"product","ts":"2024-01-01 10:00:02"}',
     "user-events", 0, 1, "2024-01-01 10:00:02"),
    (b"user_3", b'{"user_id":"user_3","event":"click","page":"home","ts":"2024-01-01 10:00:03"}',
     "user-events", 2, 0, "2024-01-01 10:00:03"),
    (b"user_2", b'{"user_id":"user_2","event":"click","page":"profile","ts":"2024-01-01 10:00:04"}',
     "user-events", 1, 1, "2024-01-01 10:00:04"),
]

df_kafka_raw = spark.createDataFrame(kafka_messages, 
    ["key", "value", "topic", "partition", "offset", "timestamp"])

print("=== Raw Kafka Message Format ===")
df_kafka_raw.show(truncate=False)

# ============ DESERIALIZING KAFKA MESSAGES ============

# Step 1: Cast binary key and value to string
df_string = df_kafka_raw.select(
    col("key").cast("string").alias("key"),
    col("value").cast("string").alias("value"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp")
)

print("=== After casting to string ===")
df_string.show(truncate=False)

# Step 2: Parse JSON value into structured columns
event_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("event", StringType(), True),
    StructField("page", StringType(), True),
    StructField("ts", StringType(), True)
])

df_parsed = df_string.select(
    col("key"),
    from_json(col("value"), event_schema).alias("data"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp")
).select(
    col("key"),
    col("data.user_id"),
    col("data.event"),
    col("data.page"),
    col("data.ts").alias("event_time"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp")
)

print("=== Parsed/Deserialized Messages ===")
df_parsed.show(truncate=False)

# ============ KAFKA PARTITION -> SPARK PARTITION MAPPING ============
"""
CRITICAL CONCEPT:
- Each Kafka partition maps to exactly ONE Spark partition
- If Kafka topic has 10 partitions -> Spark reads with 10 tasks (parallel)
- This is the INITIAL parallelism (before any shuffle)

Kafka Partitions    →    Spark Tasks (Read Stage)
┌────────────┐          ┌────────────┐
│ Partition 0 │    →    │   Task 0    │
├────────────┤          ├────────────┤
│ Partition 1 │    →    │   Task 1    │
├────────────┤          ├────────────┤
│ Partition 2 │    →    │   Task 2    │
└────────────┘          └────────────┘

IMPLICATION:
- More Kafka partitions = more parallelism in Spark
- If Kafka has 3 partitions but Spark has 100 cores -> underutilized!
- Recommendation: Kafka partitions >= Spark executor cores for read stage
"""

# ============ WRITING BACK TO KAFKA ============
"""
To write to Kafka, DataFrame must have 'value' column (required)
and optionally 'key', 'topic', 'partition', 'headers' columns.

# Prepare data for Kafka write
df_to_kafka = df_parsed.select(
    col("user_id").alias("key"),
    to_json(struct("user_id", "event", "page", "event_time")).alias("value")
)

# Write to Kafka
df_to_kafka.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "processed-events") \
    .save()
"""

# Simulate Kafka write preparation
print("=== Preparing data for Kafka write ===")
df_to_kafka = df_parsed.select(
    col("user_id").alias("key"),
    to_json(struct("user_id", "event", "page", "event_time")).alias("value")
)
df_to_kafka.show(truncate=False)

# Write to local path for demo
df_parsed.write.mode("overwrite").parquet("/shared/kafka_basics_demo")

spark.stop()
