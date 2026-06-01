"""
Topic: Concurrency Control, Multi-Writer, and Timeline
========================================================

How Hudi handles concurrent writes and maintains consistency.

Key Interview Points:
- Hudi Timeline: Ordered log of all operations (commits, compactions, cleans).
- Optimistic Concurrency Control (OCC): Default for single-writer.
- Multi-writer support via lock providers (ZooKeeper, DynamoDB, etc.).
- MVCC: Readers never blocked by writers (snapshot isolation).
- Conflict resolution: Last writer wins or fail-on-conflict.
- Timeline is the source of truth for table state.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("08_Hudi_Concurrency") \
    .master("local[*]") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

# ============ HUDI TIMELINE ============
"""
The Timeline is Hudi's transaction log (like a WAL in databases).
Every operation is recorded as an "instant" on the timeline.

Timeline Structure:
.hoodie/
├── 20240101100000.commit           ← Completed commit
├── 20240101100000.commit.requested ← Commit requested
├── 20240101100000.commit.inflight  ← Commit in progress
├── 20240102090000.commit
├── 20240102090000.deltacommit      ← MoR log write
├── 20240103080000.compaction.requested
├── 20240103080000.compaction.inflight
├── 20240103080000.compaction.commit ← Completed compaction
├── 20240103090000.clean.requested
├── 20240103090000.clean.commit      ← Completed cleaning
└── 20240103100000.rollback          ← Rolled back commit

INSTANT STATES:
  .requested → .inflight → .commit (success)
                         → .rollback (failure)

INSTANT TYPES:
- commit: CoW write operation
- deltacommit: MoR write operation (log append)
- compaction: MoR compaction
- clean: File cleaning
- rollback: Rolled back operation
- savepoint: Marked point for disaster recovery
- replace: Clustering or insert_overwrite

TIMELINE GUARANTEES:
1. Atomic: Each commit is all-or-nothing
2. Ordered: Instants are strictly ordered by timestamp
3. Consistent: Readers see consistent snapshots
4. Durable: Timeline persisted to storage (HDFS/S3)
"""

# ============ CONCURRENCY CONTROL ============
"""
SINGLE WRITER (Default - Optimistic Concurrency):
- Only ONE writer at a time
- No locking needed
- If two writers conflict: Second one fails, must retry
- Sufficient for most batch ETL pipelines

MULTI-WRITER (requires lock provider):
- Multiple writers can write concurrently
- Lock provider coordinates access
- Conflict detection at file-group level
- Use for: Multiple pipelines writing to same table

LOCK PROVIDERS:
┌─────────────────────────┬─────────────────────────────────────────────┐
│ Provider                │ Use Case                                    │
├─────────────────────────┼─────────────────────────────────────────────┤
│ ZooKeeper               │ On-premise, Hadoop clusters                 │
│ DynamoDB                │ AWS (serverless, highly available)           │
│ HiveMetastore           │ When Hive metastore is available            │
│ FileSystem              │ Simple, for testing (not production)        │
│ InProcess               │ Single JVM (testing only)                   │
└─────────────────────────┴─────────────────────────────────────────────┘

CONFIGURATION (Multi-writer with DynamoDB):
hoodie.write.concurrency.mode = optimistic_concurrency_control
hoodie.cleaner.policy.failed.writes = LAZY
hoodie.write.lock.provider = org.apache.hudi.aws.transaction.lock.DynamoDBBasedLockProvider
hoodie.write.lock.dynamodb.table = hudi-locks
hoodie.write.lock.dynamodb.region = us-east-1
hoodie.write.lock.dynamodb.partition_key = tablename

CONFIGURATION (Multi-writer with ZooKeeper):
hoodie.write.concurrency.mode = optimistic_concurrency_control
hoodie.write.lock.provider = org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider
hoodie.write.lock.zookeeper.url = zk-host:2181
hoodie.write.lock.zookeeper.port = 2181
hoodie.write.lock.zookeeper.lock_key = /hudi/locks/my_table
hoodie.write.lock.zookeeper.base_path = /hudi/locks
"""

# ============ CONFLICT RESOLUTION ============
"""
When two writers modify the same file group:

SCENARIO:
  Writer A: Reads file_1, modifies record R1, writes file_1_v2
  Writer B: Reads file_1, modifies record R2, writes file_1_v3
  
  Both started from file_1_v1. Who wins?

RESOLUTION STRATEGIES:

1. FAIL ON CONFLICT (default for OCC):
   - Second writer to commit FAILS
   - Must retry the entire operation
   - Guarantees no data loss
   
2. LAST WRITER WINS:
   - Second writer's changes overwrite first
   - May lose first writer's changes
   - Simpler but less safe

3. FILE-GROUP LEVEL CONFLICT:
   - Conflict only if SAME file group is modified
   - Different file groups = no conflict (parallel OK)
   - This is Hudi's default granularity

NON-CONFLICTING CONCURRENT WRITES:
  Writer A: Writes to partition date=2024-01-01
  Writer B: Writes to partition date=2024-01-02
  → NO CONFLICT (different file groups)
  → Both succeed without coordination!
"""

# ============ MVCC (Multi-Version Concurrency Control) ============
"""
Readers and writers NEVER block each other:

Writer: Creating commit_003 (writing new files)
Reader: Reading snapshot at commit_002 (sees consistent old state)

How:
1. Writer creates new file versions (doesn't modify existing)
2. Reader reads files as of their snapshot time
3. After commit, new readers see new files
4. Old files cleaned up later (by cleaner)

This means:
- Long-running queries are never interrupted by writes
- Writes don't wait for readers to finish
- Each reader sees a consistent snapshot
- No dirty reads, no phantom reads
"""

# ============ SAVEPOINTS AND ROLLBACK ============
"""
SAVEPOINT: Mark a commit as "protected" (won't be cleaned).
Use for: Disaster recovery, known-good state.

# Create savepoint
spark-submit --class org.apache.hudi.utilities.HoodieSavepointCreator \\
    --instant-time 20240101100000 \\
    --base-path /hudi/my_table

ROLLBACK: Undo a commit (revert to previous state).
Use for: Bad data ingested, failed pipeline.

# Rollback last commit
spark-submit --class org.apache.hudi.utilities.HoodieRollback \\
    --instant-time 20240103120000 \\
    --base-path /hudi/my_table

# Programmatic rollback:
# from pyspark.sql import SparkSession
# spark.sql("CALL rollback_to_instant(table => 'my_table', instant_time => '20240101100000')")

RESTORE: Restore table to a savepoint (deletes all commits after savepoint).
Use for: Major disaster recovery.
"""

# ============ WRITE CONFLICT SCENARIOS ============
"""
SCENARIO 1: Streaming + Batch on same table
─────────────────────────────────────────────
Problem: Streaming job writes every minute, batch job runs daily.
Solution: Multi-writer with lock provider.
  - Streaming: Continuous small writes (deltacommit)
  - Batch: Large upsert (commit)
  - Lock ensures they don't corrupt each other

SCENARIO 2: Multiple Kafka consumers writing same table
─────────────────────────────────────────────────────────
Problem: Multiple Spark jobs consuming different Kafka topics, writing same Hudi table.
Solution: 
  Option A: Single writer (merge topics before writing)
  Option B: Multi-writer with DynamoDB lock (if topics write different partitions)

SCENARIO 3: Compaction + Ingestion
───────────────────────────────────
Problem: Compaction runs while streaming ingestion is active.
Solution: Hudi handles this natively!
  - Compaction and ingestion can run concurrently on MoR tables
  - They operate on different file slices
  - No lock needed for this specific case
"""

print("=== Concurrency Control Summary ===")
print("""
1. Timeline: Ordered log of all operations (source of truth)
2. Single writer: Default, no locking needed
3. Multi-writer: Requires lock provider (ZK, DynamoDB)
4. MVCC: Readers never blocked by writers
5. Conflict resolution: File-group level, fail-on-conflict default
6. Savepoints: Protected commits for disaster recovery
7. Rollback: Undo bad commits
""")

spark.stop()
