"""
Topic: Writing to Kafka from Spark Structured Streaming
=========================================================

How to produce/write messages back to Kafka topics.

Spark UI Behavior:
- Write to Kafka adds a final stage to each micro-batch job.
- No additional shuffle unless prior transformations require it.
- In Spark UI: look for KafkaWriter in the DAG.
- Kafka write is the SINK - it's what triggers execution.

Key Interview Points:
- DataFrame MUST have a 'value' column (required, string or binary).
- 'key' column is optional (determines Kafka partition).
- 'topic' column is optional (overrides static topic option).
- 'headers' column is optional (Kafka headers).
- Can write to single topic (option) or per-row topic (column).
- Exactly-once delivery requires idempotent producer + transactions.
- Kafka write is an atomic operation per micro-batch.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_json, struct, from_json, lit, 
    concat, current_timestamp, expr
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder \
    .appName("04_Writing_To_Kafka") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# ============ KAFKA WRITE REQUIREMENTS ============
"""
Required columns for Kafka write:
┌──────────┬──────────┬──────────┬─────────────────────────────────────────┐
│ Column   │ Type     │ Required │ Description                             │
├──────────┼──────────┼──────────┼─────────────────────────────────────────┤
│ value    │ string/  │ YES      │ Message payload                         │
│          │ binary   │          │                                         │
├──────────┼──────────┼──────────┼─────────────────────────────────────────┤
│ key      │ string/  │ No       │ Message key (partition routing)         │
│          │ binary   │          │                                         │
├──────────┼──────────┼──────────┼─────────────────────────────────────────┤
│ topic    │ string   │ No*      │ Target topic (overrides option)         │
│          │          │          │ *Required if not set in option          │
├──────────┼──────────┼──────────┼─────────────────────────────────────────┤
│ headers  │ array    │ No       │ Kafka message headers                   │
├──────────┼──────────┼──────────┼─────────────────────────────────────────┤
│ partition│ int      │ No       │ Specific partition (overrides key hash) │
└──────────┴──────────┴──────────┴─────────────────────────────────────────┘
"""

# Sample processed data (after some transformation)
processed_data = [
    ("user_1", "ORD001", "laptop", 1200.00, "2024-01-01 10:00:00"),
    ("user_2", "ORD002", "phone", 800.00, "2024-01-01 10:00:05"),
    ("user_1", "ORD003", "tablet", 500.00, "2024-01-01 10:00:10"),
    ("user_3", "ORD004", "laptop", 1300.00, "2024-01-01 10:00:15"),
    ("user_2", "ORD005", "headphones", 200.00, "2024-01-01 10:00:20"),
]

df_processed = spark.createDataFrame(processed_data, 
    ["user_id", "order_id", "product", "amount", "event_time"])

# ============ METHOD 1: Write entire row as JSON value ============

print("=== Method 1: Entire row as JSON value ===")
df_to_kafka_v1 = df_processed.select(
    col("user_id").alias("key"),  # Key for partition routing
    to_json(struct("*")).alias("value")  # All columns as JSON
)
df_to_kafka_v1.show(truncate=False)

"""
# Write to Kafka (streaming)
df_to_kafka_v1.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "processed-orders") \
    .option("checkpointLocation", "/shared/checkpoints/processed-orders") \
    .outputMode("append") \
    .start()

# Write to Kafka (batch)
df_to_kafka_v1.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "processed-orders") \
    .save()
"""

# ============ METHOD 2: Custom JSON structure ============

print("=== Method 2: Custom JSON structure ===")
df_to_kafka_v2 = df_processed.select(
    col("user_id").alias("key"),
    to_json(struct(
        col("order_id"),
        col("product"),
        col("amount"),
        lit("processed").alias("status"),
        current_timestamp().alias("processed_at")
    )).alias("value")
)
df_to_kafka_v2.show(truncate=False)

# ============ METHOD 3: Write to different topics per row ============

print("=== Method 3: Dynamic topic routing ===")
df_to_kafka_v3 = df_processed.select(
    col("user_id").alias("key"),
    to_json(struct("order_id", "product", "amount")).alias("value"),
    # Route high-value orders to different topic
    expr("""
        CASE 
            WHEN amount > 1000 THEN 'high-value-orders'
            ELSE 'normal-orders'
        END
    """).alias("topic")
)
df_to_kafka_v3.show(truncate=False)

"""
# When 'topic' column exists, each row goes to its specified topic
df_to_kafka_v3.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("checkpointLocation", "/shared/checkpoints/routed-orders") \
    .outputMode("append") \
    .start()
"""

# ============ METHOD 4: Write aggregation results to Kafka ============

print("=== Method 4: Aggregation results to Kafka ===")
df_agg = df_processed.groupBy("product").agg(
    {"amount": "sum", "order_id": "count"}
).withColumnRenamed("sum(amount)", "total_revenue") \
 .withColumnRenamed("count(order_id)", "order_count")

df_agg_kafka = df_agg.select(
    col("product").alias("key"),
    to_json(struct("product", "total_revenue", "order_count")).alias("value")
)
df_agg_kafka.show(truncate=False)

# ============ KAFKA WRITE OPTIONS ============
"""
┌─────────────────────────────────────┬─────────────────────────────────────────┐
│ Option                              │ Description                             │
├─────────────────────────────────────┼─────────────────────────────────────────┤
│ kafka.bootstrap.servers             │ Broker addresses (REQUIRED)             │
│ topic                               │ Target topic (if not in DataFrame)      │
├─────────────────────────────────────┼─────────────────────────────────────────┤
│ kafka.acks                          │ "all", "1", "0" (durability)            │
│ kafka.retries                       │ Number of retries on failure            │
│ kafka.batch.size                    │ Batch size in bytes (16384)             │
│ kafka.linger.ms                     │ Wait time to batch messages (0)         │
│ kafka.buffer.memory                 │ Total buffer memory (33554432)          │
│ kafka.max.request.size              │ Max request size (1048576)              │
│ kafka.compression.type              │ none, gzip, snappy, lz4, zstd          │
├─────────────────────────────────────┼─────────────────────────────────────────┤
│ kafka.enable.idempotence            │ Exactly-once producer (true)            │
│ kafka.transactional.id              │ For transactional writes                │
└─────────────────────────────────────┴─────────────────────────────────────────┘

EXACTLY-ONCE WRITE TO KAFKA:
  .option("kafka.enable.idempotence", "true")
  .option("kafka.acks", "all")
  .option("kafka.retries", "3")
  .option("kafka.max.in.flight.requests.per.connection", "1")
"""

# ============ FULL STREAMING PIPELINE: READ -> TRANSFORM -> WRITE ============
"""
# Complete pipeline: Read from Kafka, transform, write back to Kafka

# Read
df_input = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "raw-orders") \
    .option("startingOffsets", "latest") \
    .load()

# Transform
order_schema = StructType([...])

df_transformed = df_input \
    .select(from_json(col("value").cast("string"), order_schema).alias("order")) \
    .select("order.*") \
    .filter(col("amount") > 0) \
    .withColumn("tax", col("amount") * 0.1) \
    .withColumn("total", col("amount") + col("amount") * 0.1)

# Write back to Kafka
query = df_transformed \
    .select(
        col("user_id").alias("key"),
        to_json(struct("*")).alias("value")
    ) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "enriched-orders") \
    .option("checkpointLocation", "/shared/checkpoints/enriched-orders") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()
"""

# Write demo
df_to_kafka_v1.write.mode("overwrite").parquet("/shared/kafka_write_demo")

spark.stop()
