"""
Topic: foreach() and foreachBatch() - Custom Sinks
====================================================

Write streaming output to any external system using custom logic.

Spark UI Behavior:
- foreachBatch: Each micro-batch triggers the batch function.
  The function's operations appear as normal jobs in Spark UI.
- foreach: Per-row processing, visible as tasks within stages.
- foreachBatch is preferred (batch-level optimizations possible).

Key Interview Points:
- foreachBatch: Receives a DataFrame + batchId for each micro-batch.
  Can use ANY batch DataFrame operation (write to DB, API calls, etc.).
- foreach: Receives one row at a time (ForeachWriter interface).
  Less efficient but useful for per-record logic.
- foreachBatch enables: Multi-sink writes, dedup logic, upserts.
- foreachBatch is the most flexible sink option.
- Use foreachBatch for: Database writes, API calls, custom file formats.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_json, struct, current_timestamp,
    lit, count, sum, window, to_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder \
    .appName("09_ForEach_ForEachBatch") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Sample data (simulating streaming micro-batches)
batch_data = [
    ("2024-01-01 10:00:00", "user_1", "purchase", 100.0),
    ("2024-01-01 10:00:05", "user_2", "purchase", 200.0),
    ("2024-01-01 10:00:10", "user_1", "purchase", 50.0),
    ("2024-01-01 10:00:15", "user_3", "purchase", 300.0),
    ("2024-01-01 10:00:20", "user_2", "purchase", 75.0),
]

df_batch = spark.createDataFrame(batch_data, ["event_time", "user_id", "event_type", "amount"])
df_batch = df_batch.withColumn("event_time", to_timestamp("event_time"))

# ============ foreachBatch() - PREFERRED APPROACH ============
"""
foreachBatch receives:
  - batch_df: A regular DataFrame (not streaming!) for the current micro-batch
  - batch_id: Unique ID for this batch (for idempotency)

You can do ANYTHING with batch_df that you'd do with a regular DataFrame:
  - Write to database (JDBC)
  - Write to multiple sinks
  - Apply complex transformations
  - Call external APIs
  - Perform upserts/merges
"""

# Pattern 1: Write to multiple sinks
def write_to_multiple_sinks(batch_df: DataFrame, batch_id: int):
    """Write each micro-batch to multiple destinations."""
    print(f"\n--- Processing Batch {batch_id} ({batch_df.count()} rows) ---")
    
    # Sink 1: Write raw data to data lake (Parquet)
    batch_df.write.mode("append").parquet("/shared/raw_events")
    
    # Sink 2: Write aggregated data to another location
    agg_df = batch_df.groupBy("user_id").agg(
        sum("amount").alias("total_amount"),
        count("*").alias("event_count")
    )
    agg_df.write.mode("overwrite").parquet("/shared/user_aggregates")
    
    # Sink 3: Write high-value transactions to alert system
    high_value = batch_df.filter(col("amount") > 150)
    if high_value.count() > 0:
        high_value.write.mode("append").json("/shared/alerts")
    
    print(f"  Batch {batch_id} written to 3 sinks")

# Simulate calling foreachBatch
write_to_multiple_sinks(df_batch, batch_id=0)

"""
# In streaming:
query = df_stream \
    .writeStream \
    .foreachBatch(write_to_multiple_sinks) \
    .option("checkpointLocation", "/shared/checkpoints/multi-sink") \
    .trigger(processingTime="30 seconds") \
    .start()
"""

# Pattern 2: Upsert/Merge to database
def upsert_to_database(batch_df: DataFrame, batch_id: int):
    """Upsert (insert or update) to a database table."""
    print(f"\n--- Upsert Batch {batch_id} ---")
    
    # In production, you'd use JDBC:
    # batch_df.write \
    #     .format("jdbc") \
    #     .option("url", "jdbc:postgresql://host:5432/db") \
    #     .option("dbtable", "events") \
    #     .option("user", "user") \
    #     .option("password", "pass") \
    #     .mode("append") \
    #     .save()
    
    # For Delta Lake (supports MERGE/UPSERT natively):
    # from delta.tables import DeltaTable
    # delta_table = DeltaTable.forPath(spark, "/shared/delta/events")
    # delta_table.alias("target").merge(
    #     batch_df.alias("source"),
    #     "target.event_id = source.event_id"
    # ).whenMatchedUpdateAll() \
    #  .whenNotMatchedInsertAll() \
    #  .execute()
    
    batch_df.show()
    print(f"  Batch {batch_id}: {batch_df.count()} rows upserted")

upsert_to_database(df_batch, batch_id=1)

# Pattern 3: Deduplication within batch
def deduplicate_and_write(batch_df: DataFrame, batch_id: int):
    """Deduplicate within each batch before writing."""
    print(f"\n--- Dedup Batch {batch_id} ---")
    
    # Remove duplicates within this batch
    deduped = batch_df.dropDuplicates(["user_id", "event_time"])
    
    # Write deduplicated data
    deduped.write.mode("append").parquet("/shared/deduped_events")
    print(f"  Batch {batch_id}: {batch_df.count()} -> {deduped.count()} after dedup")

deduplicate_and_write(df_batch, batch_id=2)

# Pattern 4: Idempotent writes using batch_id
def idempotent_write(batch_df: DataFrame, batch_id: int):
    """Use batch_id to ensure idempotent writes (safe for retries)."""
    print(f"\n--- Idempotent Write Batch {batch_id} ---")
    
    # Add batch_id to data for tracking
    batch_with_id = batch_df.withColumn("batch_id", lit(batch_id))
    
    # Write to a batch-specific path (idempotent - overwrite same batch)
    batch_with_id.write.mode("overwrite") \
        .parquet(f"/shared/batches/batch_{batch_id}")
    
    print(f"  Batch {batch_id} written idempotently")

idempotent_write(df_batch, batch_id=3)

# ============ foreach() - Per-Row Processing ============
"""
foreach uses a ForeachWriter with three methods:
  - open(partition_id, epoch_id): Called once per partition per batch
  - process(row): Called for each row
  - close(error): Called when partition processing is done

Less efficient than foreachBatch but useful for:
  - Per-record API calls
  - Per-record database inserts
  - Custom per-record logic
"""

# ForeachWriter example (conceptual - works in streaming)
"""
class DatabaseWriter:
    def open(self, partition_id, epoch_id):
        # Open database connection
        self.connection = get_db_connection()
        # Return True to process this partition, False to skip
        return True
    
    def process(self, row):
        # Write single row to database
        self.connection.execute(
            "INSERT INTO events (user_id, event_type, amount) VALUES (?, ?, ?)",
            (row.user_id, row.event_type, row.amount)
        )
    
    def close(self, error):
        # Close connection, handle errors
        if error:
            self.connection.rollback()
        else:
            self.connection.commit()
        self.connection.close()

# Usage:
query = df_stream \
    .writeStream \
    .foreach(DatabaseWriter()) \
    .outputMode("append") \
    .start()
"""

# ============ foreachBatch vs foreach COMPARISON ============
"""
┌─────────────────────┬──────────────────────────┬──────────────────────────┐
│ Aspect              │ foreachBatch             │ foreach                  │
├─────────────────────┼──────────────────────────┼──────────────────────────┤
│ Input               │ DataFrame + batch_id     │ Single Row               │
│ Efficiency          │ HIGH (batch operations)  │ LOW (row by row)         │
│ Use Case            │ DB writes, multi-sink    │ Per-record API calls     │
│ Parallelism         │ Full DataFrame parallel  │ Per-partition parallel   │
│ Connection Mgmt     │ Per batch               │ Per partition per batch  │
│ Flexibility         │ Any DataFrame operation  │ Limited to row logic     │
│ Recommended         │ YES (almost always)      │ Only when necessary      │
└─────────────────────┴──────────────────────────┴──────────────────────────┘

ALWAYS prefer foreachBatch unless you specifically need per-row processing.
"""

# ============ COMPLETE STREAMING PIPELINE WITH foreachBatch ============
"""
# Full production pipeline:

def process_orders(batch_df: DataFrame, batch_id: int):
    '''Process each micro-batch of orders.'''
    
    # 1. Parse and validate
    parsed = batch_df.select(
        from_json(col("value").cast("string"), order_schema).alias("order")
    ).select("order.*").filter(col("amount") > 0)
    
    # 2. Enrich with lookup data
    enriched = parsed.join(broadcast(dim_products), "product_id", "left")
    
    # 3. Write to data lake
    enriched.write.mode("append") \
        .partitionBy("date") \
        .parquet("/shared/data_lake/orders")
    
    # 4. Write aggregates to serving layer
    agg = enriched.groupBy("category").agg(sum("amount").alias("revenue"))
    agg.write.format("jdbc") \
        .option("url", "jdbc:postgresql://host/db") \
        .option("dbtable", "category_revenue") \
        .mode("overwrite").save()
    
    # 5. Send alerts for high-value orders
    alerts = enriched.filter(col("amount") > 10000)
    if alerts.count() > 0:
        alerts.select(to_json(struct("*")).alias("value")) \
            .write.format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "order-alerts") \
            .save()

# Start the pipeline
query = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .load() \
    .writeStream \
    .foreachBatch(process_orders) \
    .option("checkpointLocation", "/shared/checkpoints/orders-pipeline") \
    .trigger(processingTime="1 minute") \
    .start()
"""

print("\n=== foreachBatch Summary ===")
print("1. Most flexible sink option")
print("2. Receives regular DataFrame (all batch operations available)")
print("3. Use batch_id for idempotent writes")
print("4. Can write to multiple sinks in one function")
print("5. Preferred over foreach for performance")

# Write final demo
df_batch.write.mode("overwrite").parquet("/shared/foreach_batch_demo")

spark.stop()
