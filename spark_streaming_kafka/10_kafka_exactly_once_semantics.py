"""
Topic: Exactly-Once Semantics with Kafka + Spark
==================================================

End-to-end exactly-once processing guarantee.

Spark UI Behavior:
- No visible difference in Spark UI between at-least-once and exactly-once.
- Checkpoint commits are visible in streaming progress metrics.
- Failed batches are retried (visible as repeated batch IDs in logs).

Key Interview Points:
- Exactly-once is an END-TO-END guarantee (source + processing + sink).
- Kafka source: Replayable (can re-read from any offset) ✓
- Spark processing: Checkpointed (tracks offsets + state) ✓
- Sink: Must be IDEMPOTENT (writing same data twice = same result) ✓
- If any component breaks: Falls back to at-least-once (duplicates possible).
- Three delivery semantics: at-most-once, at-least-once, exactly-once.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("10_Exactly_Once_Semantics") \
    .master("local[*]") \
    .getOrCreate()

# ============ DELIVERY SEMANTICS ============
"""
┌─────────────────┬────────────────────────────────────────────────────────────┐
│ Semantic        │ Description                                                │
├─────────────────┼────────────────────────────────────────────────────────────┤
│ AT-MOST-ONCE    │ Messages may be LOST but never duplicated.                 │
│                 │ Process: Read → Commit offset → Process                    │
│                 │ If crash after commit but before process: DATA LOST         │
│                 │ Use when: Loss is acceptable (metrics, logs)               │
├─────────────────┼────────────────────────────────────────────────────────────┤
│ AT-LEAST-ONCE   │ Messages are never lost but may be DUPLICATED.             │
│                 │ Process: Read → Process → Commit offset                    │
│                 │ If crash after process but before commit: REPROCESSED       │
│                 │ Use when: Duplicates are acceptable or handled downstream  │
├─────────────────┼────────────────────────────────────────────────────────────┤
│ EXACTLY-ONCE    │ Messages are processed EXACTLY once. No loss, no dupes.    │
│                 │ Requires: Replayable source + Checkpoint + Idempotent sink │
│                 │ Use when: Financial transactions, billing, critical data   │
└─────────────────┴────────────────────────────────────────────────────────────┘
"""

# ============ HOW SPARK ACHIEVES EXACTLY-ONCE ============
"""
SPARK'S EXACTLY-ONCE MECHANISM:

1. PRE-PLANNING (before processing):
   - Read checkpoint: What was the last committed batch?
   - Determine offsets for next batch (write to offsets/ directory)
   
2. PROCESSING:
   - Read data from Kafka at those specific offsets
   - Apply transformations
   - Write to sink
   
3. POST-PROCESSING:
   - Write commit marker (commits/ directory)
   - This marks the batch as DONE
   
4. ON FAILURE:
   - If crash during step 2: Batch is NOT committed
   - On restart: Re-read same offsets, reprocess, rewrite
   - Sink must handle this duplicate write (idempotent!)
   
5. ON SUCCESS:
   - Commit marker written
   - Next batch starts from new offsets

FAILURE SCENARIOS:
┌────────────────────────────────┬─────────────────────────────────────────┐
│ Failure Point                  │ Recovery Behavior                       │
├────────────────────────────────┼─────────────────────────────────────────┤
│ Before processing              │ Restart from last committed offset      │
│ During processing              │ Restart from last committed offset      │
│ After sink write, before commit│ Reprocess batch (sink must be idempotent)│
│ After commit                   │ Move to next batch (all good)           │
└────────────────────────────────┴─────────────────────────────────────────┘
"""

# ============ IDEMPOTENT SINK PATTERNS ============
"""
PATTERN 1: File Sink (Built-in idempotent)
─────────────────────────────────────────────
Spark writes to temp directory, atomically renames on commit.
If batch is retried, temp files are overwritten (same result).

query = df.writeStream \
    .format("parquet") \
    .option("path", "/shared/output") \
    .option("checkpointLocation", "/shared/checkpoint") \
    .start()


PATTERN 2: Kafka Sink (Idempotent producer)
─────────────────────────────────────────────
Enable Kafka idempotent producer to prevent duplicates.

query = df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "output-topic") \
    .option("kafka.enable.idempotence", "true") \
    .option("kafka.acks", "all") \
    .option("checkpointLocation", "/shared/checkpoint") \
    .start()


PATTERN 3: Database Sink (Upsert/Merge)
─────────────────────────────────────────────
Use UPSERT (INSERT ON CONFLICT UPDATE) instead of INSERT.
Writing same record twice = same final state.

def upsert_to_db(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://host/db") \
        .option("dbtable", "events") \
        .mode("append") \
        .save()
    # Better: Use raw SQL with ON CONFLICT DO UPDATE

# Even better with Delta Lake:
def upsert_delta(batch_df, batch_id):
    from delta.tables import DeltaTable
    delta_table = DeltaTable.forPath(spark, "/shared/delta/events")
    delta_table.alias("t").merge(
        batch_df.alias("s"),
        "t.event_id = s.event_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()


PATTERN 4: Custom Idempotent Logic (batch_id tracking)
─────────────────────────────────────────────────────────
Track which batch_ids have been processed.

def idempotent_write(batch_df, batch_id):
    # Check if this batch was already processed
    if batch_already_processed(batch_id):
        return  # Skip (idempotent)
    
    # Process and write
    batch_df.write.mode("append").parquet("/shared/output")
    
    # Mark batch as processed
    mark_batch_processed(batch_id)
"""

# ============ KAFKA TRANSACTIONS (Exactly-Once Kafka-to-Kafka) ============
"""
For Kafka-to-Kafka pipelines, Kafka transactions provide exactly-once:

Read from input topic → Process → Write to output topic + Commit offset
All in ONE atomic transaction.

Spark doesn't use Kafka transactions directly (uses checkpoint instead).
But for non-Spark Kafka consumers, transactions are the mechanism.

Kafka Transaction Flow:
1. Begin transaction
2. Produce messages to output topic
3. Commit consumer offsets (as part of transaction)
4. Commit transaction
→ Either ALL succeed or NONE (atomic)
"""

# ============ COMMON PITFALLS ============
"""
PITFALL 1: Non-idempotent sink
  Problem: INSERT INTO table (duplicates on retry)
  Fix: Use UPSERT/MERGE or deduplicate downstream

PITFALL 2: Side effects in transformations
  Problem: Sending emails/notifications in map() - sent again on retry!
  Fix: Move side effects to sink (foreachBatch) with idempotency check

PITFALL 3: Checkpoint on unreliable storage
  Problem: Checkpoint on local disk, node dies, checkpoint lost
  Fix: Use HDFS/S3/GCS for checkpoint location

PITFALL 4: Changing query without handling checkpoint
  Problem: Change aggregation logic, old checkpoint incompatible
  Fix: Delete checkpoint (accept reprocessing) or version checkpoint paths

PITFALL 5: Clock skew in exactly-once
  Problem: Processing time used for dedup, clocks differ across nodes
  Fix: Use event time (from Kafka message) not processing time

PITFALL 6: External system not idempotent
  Problem: Calling external API that charges money on each call
  Fix: Add dedup layer (check if already processed before calling)
"""

# ============ EXACTLY-ONCE CONFIGURATION CHECKLIST ============
"""
✓ Source: Kafka with specific offsets (replayable)
✓ Processing: Checkpoint enabled with reliable storage
✓ Sink: Idempotent write mechanism
✓ kafka.enable.idempotence = true (for Kafka sink)
✓ kafka.acks = all (for Kafka sink)
✓ Unique checkpointLocation per query
✓ No side effects in transformations
✓ Proper error handling in foreachBatch
"""

print("=== Exactly-Once Semantics Summary ===")
print("""
End-to-end exactly-once requires ALL THREE:
1. Replayable Source (Kafka ✓)
2. Checkpointed Processing (Spark ✓)
3. Idempotent Sink (your responsibility!)

Without idempotent sink → at-least-once (duplicates on retry)
Without checkpoint → at-most-once or reprocess everything
Without replayable source → at-most-once (data loss on failure)
""")

spark.stop()
