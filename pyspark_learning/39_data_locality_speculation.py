"""
Topic: Data Locality, Speculation, and Task Scheduling
=======================================================

How Spark schedules tasks close to data and handles slow tasks.

Spark UI Behavior:
- Data Locality: In Spark UI -> Stages -> Tasks -> "Locality Level" column
  Shows: PROCESS_LOCAL, NODE_LOCAL, RACK_LOCAL, ANY
- Speculation: In Spark UI -> Stages -> Tasks -> "Speculated" column
  Shows which tasks were launched speculatively.
- Speculative tasks appear as duplicate task attempts.

Key Interview Points:
- Data locality: Spark tries to schedule tasks where data resides.
- Locality levels: PROCESS_LOCAL > NODE_LOCAL > RACK_LOCAL > ANY
- Spark waits briefly for better locality before falling back.
- Speculation: Launches duplicate of slow tasks on other nodes.
- First to finish wins, other is killed.
- Useful for heterogeneous clusters or noisy neighbors.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("39_Data_Locality_Speculation") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.speculation", "false") \
    .config("spark.locality.wait", "3s") \
    .getOrCreate()

# ============ DATA LOCALITY LEVELS ============
"""
Locality Levels (best to worst):

1. PROCESS_LOCAL:
   - Data is in the same JVM (cached in executor memory)
   - Fastest: No network, no disk I/O
   - Example: Reading from cached DataFrame

2. NODE_LOCAL:
   - Data is on the same node (local disk)
   - Fast: No network, but disk I/O needed
   - Example: HDFS block on same machine

3. NO_PREF:
   - Data is equally accessible from anywhere
   - Example: Reading from S3/cloud storage

4. RACK_LOCAL:
   - Data is on a different node but same rack
   - Moderate: Network within rack (fast switch)
   - Example: HDFS block on same rack

5. ANY:
   - Data is on a different rack
   - Slowest: Cross-rack network transfer
   - Example: HDFS block on remote rack

Spark's scheduling strategy:
- First tries PROCESS_LOCAL
- Waits spark.locality.wait (default 3s) for better locality
- Falls back to NODE_LOCAL, waits again
- Falls back to RACK_LOCAL, waits again
- Finally accepts ANY locality
"""

# ============ LOCALITY WAIT CONFIGURATION ============
"""
spark.locality.wait = 3s (default)
  How long to wait for a data-local task slot before giving up.

spark.locality.wait.process = spark.locality.wait
  Wait time for PROCESS_LOCAL

spark.locality.wait.node = spark.locality.wait
  Wait time for NODE_LOCAL

spark.locality.wait.rack = spark.locality.wait
  Wait time for RACK_LOCAL

Tuning:
- Increase wait: Better locality but tasks may wait idle
- Decrease wait: Tasks start faster but may not be local
- For cloud storage (S3/GCS): Set to 0 (no locality benefit)
- For HDFS: Keep default or increase slightly
"""

# ============ SPECULATION ============
"""
Speculative Execution:
- Launches a COPY of a slow task on another executor
- First copy to finish wins, other is killed
- Helps with: Slow nodes, GC pauses, disk issues, noisy neighbors

Configuration:
  spark.speculation = true/false (default: false)
  spark.speculation.multiplier = 1.5
    Task is "slow" if it takes 1.5x the median task duration
  spark.speculation.quantile = 0.75
    Only start speculating after 75% of tasks in the stage complete
  spark.speculation.minTaskRuntime = 100ms
    Don't speculate on tasks that haven't run at least this long

When to enable:
✓ Heterogeneous clusters (different hardware)
✓ Shared clusters (noisy neighbors)
✓ Long-running stages where one slow task blocks everything
✓ Cloud environments with variable performance

When NOT to enable:
✗ Non-idempotent operations (writing without proper commit)
✗ Homogeneous, dedicated clusters (speculation wastes resources)
✗ Short tasks (overhead of launching speculative task > benefit)
✗ When skew is the real problem (fix skew instead!)

IMPORTANT: Speculation treats the SYMPTOM (slow task), not the CAUSE.
If the cause is data skew, fix the skew instead of enabling speculation!
"""

# ============ TASK SCHEDULING INTERNALS ============
"""
Task Scheduling Flow:

1. DAGScheduler creates stages from the DAG
2. For each stage, creates TaskSet (one task per partition)
3. TaskScheduler assigns tasks to executors based on:
   a. Data locality (prefer local data)
   b. Executor availability (free cores)
   c. Fairness (FIFO or Fair scheduler)

4. Task execution:
   - Executor receives task + serialized closure
   - Deserializes and runs the task
   - Returns result to driver (small) or writes to shuffle (large)

5. If task fails:
   - Retried up to spark.task.maxFailures (default 4) times
   - If all retries fail, stage fails
   - If stage fails, job fails (unless stage retry is enabled)

Scheduler modes:
- FIFO (default): First job submitted gets all resources
- FAIR: Resources shared between jobs (good for multi-user)
  Set with: spark.scheduler.mode = FAIR
"""

# ============ PRACTICAL DEMO ============

data = [(i, f"name_{i}", i * 1000) for i in range(1, 21)]
df = spark.createDataFrame(data, ["id", "name", "salary"])

# Cache to demonstrate PROCESS_LOCAL
df.cache()
df.count()  # Trigger caching

# Subsequent operations on cached data will be PROCESS_LOCAL
print("=== Operations on cached data (PROCESS_LOCAL) ===")
df.filter(col("salary") > 10000).show()

# Check locality in explain
print("\n=== Current Speculation Setting ===")
print(f"spark.speculation = {spark.conf.get('spark.speculation')}")
print(f"spark.locality.wait = {spark.conf.get('spark.locality.wait')}")

# ============ TASK FAILURE HANDLING ============
"""
spark.task.maxFailures = 4 (default)
  Maximum number of times a task can fail before the stage fails.

spark.stage.maxConsecutiveAttempts = 4
  Maximum consecutive stage attempts.

Failure causes:
1. OOM: Task runs out of memory
2. FetchFailedException: Can't read shuffle data (executor died)
3. TaskKilledException: Task killed (speculation, stage cancellation)
4. User code exception: Bug in UDF or transformation

Recovery:
- Task failure: Retry on same or different executor
- Executor failure: Tasks rescheduled, shuffle data recomputed
- Driver failure: Job fails (unless checkpointed in streaming)
"""

from pyspark.sql.functions import col
df.unpersist()

# Write
df.write.mode("overwrite").parquet("/shared/locality_demo")

spark.stop()
