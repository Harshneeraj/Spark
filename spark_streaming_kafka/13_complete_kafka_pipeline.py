"""
Topic: Complete End-to-End Kafka Streaming Pipeline
=====================================================

A production-ready pipeline: Kafka -> Spark -> Multiple Sinks.

This script demonstrates a real-world streaming architecture
with all best practices applied.

Architecture:
┌──────────┐     ┌─────────────────────────────────────────────┐     ┌──────────┐
│  Kafka   │     │           Spark Streaming                    │     │  Sinks   │
│  Source  │────▶│  Read → Parse → Validate → Enrich → Agg    │────▶│  Kafka   │
│  Topic   │     │                                              │     │  Parquet  │
└──────────┘     │  Checkpoint: HDFS/S3                         │     │  DB       │
                 └─────────────────────────────────────────────┘     └──────────┘
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_json, struct, lit,
    window, count, sum, avg, max as spark_max,
    current_timestamp, to_timestamp, expr,
    when, broadcast, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, TimestampType, BooleanType
)

spark = SparkSession.builder \
    .appName("13_Complete_Kafka_Pipeline") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# ============ STEP 1: DEFINE SCHEMAS ============

# Schema for incoming order events from Kafka
order_event_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("event_time", StringType(), True),
    StructField("order_status", StringType(), True),  # created, paid, shipped, delivered
    StructField("payment_method", StringType(), True),
    StructField("shipping_address_city", StringType(), True)
])

# ============ STEP 2: DIMENSION/LOOKUP TABLES ============

# Product catalog (static, refreshed periodically)
products = [
    ("P001", "MacBook Pro", "Electronics", "Apple", 2499.99),
    ("P002", "iPhone 15", "Electronics", "Apple", 999.99),
    ("P003", "Running Shoes", "Sports", "Nike", 129.99),
    ("P004", "Python Book", "Books", "OReilly", 49.99),
    ("P005", "Coffee Maker", "Home", "Breville", 299.99),
    ("P006", "Headphones", "Electronics", "Sony", 349.99),
    ("P007", "Yoga Mat", "Sports", "Lululemon", 79.99),
    ("P008", "Desk Lamp", "Home", "IKEA", 39.99),
]

df_products = spark.createDataFrame(products,
    ["product_id", "product_name", "category", "brand", "list_price"])

# User segments (static lookup)
user_segments = [
    ("user_1", "Premium", "US"),
    ("user_2", "Standard", "UK"),
    ("user_3", "Premium", "US"),
    ("user_4", "Standard", "IN"),
    ("user_5", "New", "US"),
]

df_users = spark.createDataFrame(user_segments,
    ["user_id", "segment", "country"])

# ============ STEP 3: SIMULATE KAFKA INPUT ============

# Simulated raw Kafka messages (in production: spark.readStream.format("kafka"))
raw_events = [
    ('{"order_id":"ORD001","user_id":"user_1","product_id":"P001","quantity":1,"unit_price":2499.99,"event_time":"2024-01-01 10:00:00","order_status":"created","payment_method":"credit_card","shipping_address_city":"New York"}'),
    ('{"order_id":"ORD002","user_id":"user_2","product_id":"P002","quantity":2,"unit_price":999.99,"event_time":"2024-01-01 10:00:30","order_status":"created","payment_method":"paypal","shipping_address_city":"London"}'),
    ('{"order_id":"ORD003","user_id":"user_3","product_id":"P003","quantity":1,"unit_price":129.99,"event_time":"2024-01-01 10:01:00","order_status":"created","payment_method":"credit_card","shipping_address_city":"Chicago"}'),
    ('{"order_id":"ORD004","user_id":"user_1","product_id":"P005","quantity":1,"unit_price":299.99,"event_time":"2024-01-01 10:01:30","order_status":"created","payment_method":"credit_card","shipping_address_city":"New York"}'),
    ('{"order_id":"ORD005","user_id":"user_4","product_id":"P004","quantity":3,"unit_price":49.99,"event_time":"2024-01-01 10:02:00","order_status":"created","payment_method":"debit_card","shipping_address_city":"Mumbai"}'),
    ('{"order_id":"ORD006","user_id":"user_2","product_id":"P006","quantity":1,"unit_price":349.99,"event_time":"2024-01-01 10:02:30","order_status":"created","payment_method":"paypal","shipping_address_city":"London"}'),
    ('{"order_id":"ORD007","user_id":"user_5","product_id":"P007","quantity":2,"unit_price":79.99,"event_time":"2024-01-01 10:03:00","order_status":"created","payment_method":"credit_card","shipping_address_city":"San Francisco"}'),
    ('{"order_id":"ORD001","user_id":"user_1","product_id":"P001","quantity":1,"unit_price":2499.99,"event_time":"2024-01-01 10:05:00","order_status":"paid","payment_method":"credit_card","shipping_address_city":"New York"}'),
    ('{"order_id":"BAD_DATA","user_id":"","product_id":"","quantity":-1,"unit_price":0,"event_time":"invalid","order_status":"","payment_method":"","shipping_address_city":""}'),
]

df_raw = spark.createDataFrame([(msg,) for msg in raw_events], ["value"])

# ============ STEP 4: PARSE AND VALIDATE ============

print("=== Step 4: Parse and Validate ===")

# Parse JSON
df_parsed = df_raw.select(
    from_json(col("value"), order_event_schema).alias("event")
).select("event.*")

# Validate and add quality flags
df_validated = df_parsed \
    .withColumn("event_time", to_timestamp("event_time")) \
    .withColumn("total_amount", col("quantity") * col("unit_price")) \
    .withColumn("is_valid",
        (col("order_id").isNotNull()) &
        (col("user_id") != "") &
        (col("quantity") > 0) &
        (col("unit_price") > 0) &
        (col("event_time").isNotNull())
    )

# Split into valid and invalid
df_valid = df_validated.filter(col("is_valid") == True).drop("is_valid")
df_invalid = df_validated.filter(col("is_valid") == False)

print(f"Valid records: {df_valid.count()}")
print(f"Invalid records (dead letter): {df_invalid.count()}")

# ============ STEP 5: ENRICH WITH DIMENSIONS ============

print("\n=== Step 5: Enrich with Dimensions ===")

df_enriched = df_valid \
    .join(broadcast(df_products), "product_id", "left") \
    .join(broadcast(df_users), "user_id", "left") \
    .withColumn("discount_pct",
        when(col("segment") == "Premium", 0.10)
        .when(col("segment") == "Standard", 0.05)
        .otherwise(0.0)
    ) \
    .withColumn("final_amount", col("total_amount") * (1 - col("discount_pct"))) \
    .withColumn("processed_at", current_timestamp())

print("Enriched orders:")
df_enriched.select(
    "order_id", "user_id", "product_name", "category", 
    "segment", "total_amount", "final_amount", "order_status"
).show(truncate=False)

# ============ STEP 6: BUSINESS LOGIC ============

print("\n=== Step 6: Business Logic ===")

# Deduplicate (same order_id, keep latest status)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

w = Window.partitionBy("order_id").orderBy(desc("event_time"))
df_deduped = df_enriched \
    .withColumn("rn", row_number().over(w)) \
    .filter(col("rn") == 1) \
    .drop("rn")

print(f"After dedup: {df_deduped.count()} unique orders")

# ============ STEP 7: AGGREGATIONS ============

print("\n=== Step 7: Real-time Aggregations ===")

# Aggregation 1: Revenue by category (5-minute windows)
df_category_revenue = df_deduped \
    .filter(col("order_status") == "created") \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window("event_time", "5 minutes"),
        "category"
    ).agg(
        count("*").alias("order_count"),
        sum("final_amount").alias("revenue"),
        avg("final_amount").alias("avg_order_value")
    )

print("Category Revenue (5-min windows):")
df_category_revenue.select(
    "window.start", "window.end", "category", 
    "order_count", "revenue", "avg_order_value"
).orderBy("window.start", "category").show(truncate=False)

# Aggregation 2: User spending summary
df_user_summary = df_deduped \
    .filter(col("order_status") == "created") \
    .groupBy("user_id", "segment", "country") \
    .agg(
        count("*").alias("total_orders"),
        sum("final_amount").alias("total_spent"),
        avg("final_amount").alias("avg_order_value"),
        spark_max("event_time").alias("last_order_time")
    )

print("User Spending Summary:")
df_user_summary.show(truncate=False)

# ============ STEP 8: WRITE TO MULTIPLE SINKS ============

print("\n=== Step 8: Write to Multiple Sinks ===")

# Sink 1: Raw enriched data to data lake (Parquet)
df_deduped.write.mode("overwrite") \
    .partitionBy("order_status") \
    .parquet("/shared/pipeline/data_lake/orders")
print("  ✓ Written to data lake (Parquet, partitioned by status)")

# Sink 2: Aggregations to serving layer
df_category_revenue.write.mode("overwrite") \
    .parquet("/shared/pipeline/serving/category_revenue")
print("  ✓ Written category revenue to serving layer")

# Sink 3: User summary
df_user_summary.write.mode("overwrite") \
    .parquet("/shared/pipeline/serving/user_summary")
print("  ✓ Written user summary to serving layer")

# Sink 4: Dead letter queue (invalid records)
df_invalid.write.mode("overwrite") \
    .json("/shared/pipeline/dead_letter")
print("  ✓ Written invalid records to dead letter queue")

# Sink 5: High-value order alerts (would go to Kafka in production)
df_alerts = df_deduped.filter(col("final_amount") > 1000) \
    .select(
        col("order_id").alias("key"),
        to_json(struct(
            "order_id", "user_id", "product_name", 
            "final_amount", "event_time"
        )).alias("value")
    )
df_alerts.write.mode("overwrite").parquet("/shared/pipeline/alerts")
print("  ✓ Written high-value alerts")

# ============ FULL STREAMING VERSION ============
"""
# Production streaming version of this pipeline:

def process_batch(batch_df: DataFrame, batch_id: int):
    '''Process each micro-batch through the full pipeline.'''
    
    # Parse
    parsed = batch_df.select(
        from_json(col("value").cast("string"), order_event_schema).alias("event")
    ).select("event.*")
    
    # Validate
    validated = parsed.withColumn("event_time", to_timestamp("event_time")) \\
        .withColumn("total_amount", col("quantity") * col("unit_price")) \\
        .withColumn("is_valid", ...)
    
    valid = validated.filter(col("is_valid"))
    invalid = validated.filter(~col("is_valid"))
    
    # Enrich
    enriched = valid \\
        .join(broadcast(df_products), "product_id", "left") \\
        .join(broadcast(df_users), "user_id", "left")
    
    # Write to multiple sinks
    enriched.write.mode("append").partitionBy("order_status") \\
        .parquet("/shared/pipeline/data_lake/orders")
    
    invalid.write.mode("append").json("/shared/pipeline/dead_letter")
    
    # Alerts to Kafka
    alerts = enriched.filter(col("total_amount") > 1000)
    if alerts.count() > 0:
        alerts.select(col("order_id").alias("key"), to_json(struct("*")).alias("value")) \\
            .write.format("kafka") \\
            .option("kafka.bootstrap.servers", "localhost:9092") \\
            .option("topic", "order-alerts") \\
            .save()

# Start pipeline
query = spark.readStream \\
    .format("kafka") \\
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092") \\
    .option("subscribe", "orders") \\
    .option("startingOffsets", "latest") \\
    .option("maxOffsetsPerTrigger", 50000) \\
    .option("failOnDataLoss", "false") \\
    .load() \\
    .writeStream \\
    .foreachBatch(process_batch) \\
    .option("checkpointLocation", "hdfs:///checkpoints/orders-pipeline-v1") \\
    .trigger(processingTime="30 seconds") \\
    .start()

query.awaitTermination()
"""

print("\n=== Pipeline Complete ===")
print("All outputs written to /shared/pipeline/")

spark.stop()
