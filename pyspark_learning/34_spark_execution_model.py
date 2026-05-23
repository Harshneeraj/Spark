"""
Topic: Spark Execution Model - Jobs, Stages, Tasks
====================================================

Understanding how Spark breaks down work is essential for debugging.

Spark UI Behavior:
- Application -> Jobs -> Stages -> Tasks
- Each ACTION creates a new JOB.
- Each SHUFFLE BOUNDARY creates a new STAGE within a job.
- Each PARTITION creates a TASK within a stage.
- Spark UI hierarchy: Application > Jobs > Stages > Tasks

Key Interview Points:
- Job: Triggered by an action (show, count, write, collect).
- Stage: Group of tasks that can run in parallel without shuffle.
  Stage boundary = shuffle (wide transformation).
- Task: Smallest unit of work. One task per partition per stage.
- DAG (Directed Acyclic Graph): Represents the computation plan.
- DAGScheduler: Splits job into stages at shuffle boundaries.
- TaskScheduler: Assigns tasks to executors.
- Total tasks = number of stages × partitions per stage.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg

spark = SparkSession.builder \
    .appName("34_Spark_Execution_Model") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

data = [(i, f"name_{i}", i % 5, i * 1000) for i in range(1, 21)]
df = spark.createDataFrame(data, ["id", "name", "dept_id", "salary"])

# ============ EXAMPLE 1: Simple Action (1 Job, 1 Stage) ============
"""
df.show()

Spark UI:
- Job 0
  └── Stage 0 (1 stage, no shuffle)
      └── Tasks: N (= number of input partitions)

Why 1 stage? No wide transformation, just read + show.
"""
print("=== Example 1: show() -> 1 Job, 1 Stage ===")
df.show()

# ============ EXAMPLE 2: GroupBy + Show (1 Job, 2 Stages) ============
"""
df.groupBy("dept_id").count().show()

Spark UI:
- Job 1
  ├── Stage 1: Read + partial aggregation (map-side)
  │   └── Tasks: N (input partitions)
  │   └── Shuffle Write: data written for next stage
  │
  └── Stage 2: Shuffle Read + final aggregation
      └── Tasks: 4 (= spark.sql.shuffle.partitions)
      └── Shuffle Read: data from Stage 1

Why 2 stages? groupBy causes shuffle (wide transformation).
Stage boundary is at the shuffle (Exchange in plan).
"""
print("=== Example 2: groupBy().count() -> 1 Job, 2 Stages ===")
df.groupBy("dept_id").count().show()

# ============ EXAMPLE 3: Join + GroupBy + Show (1 Job, 3+ Stages) ============
"""
df1.join(df2, "key").groupBy("col").count().show()

Spark UI:
- Job 2
  ├── Stage 3: Read df1 + shuffle by join key
  ├── Stage 4: Read df2 + shuffle by join key
  ├── Stage 5: Join (merge shuffled data)
  └── Stage 6: GroupBy shuffle + final aggregation

Multiple shuffle boundaries = multiple stages.
"""

df_dept = spark.createDataFrame(
    [(0, "HR"), (1, "Eng"), (2, "Mkt"), (3, "Fin"), (4, "Ops")],
    ["dept_id", "dept_name"]
)

print("=== Example 3: Join + GroupBy -> 1 Job, Multiple Stages ===")
df.join(df_dept, "dept_id") \
    .groupBy("dept_name") \
    .agg(avg("salary")) \
    .show()

# ============ EXECUTION FLOW DIAGRAM ============
"""
User Code (DataFrame API / SQL)
        │
        ▼
┌─────────────────────┐
│  Catalyst Optimizer  │  Logical Plan → Optimized Logical Plan → Physical Plan
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│    DAG Scheduler     │  Physical Plan → DAG → Stages (split at shuffles)
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│   Task Scheduler     │  Stages → Tasks (1 per partition) → Assign to executors
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│     Executors        │  Execute tasks, return results
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│      Driver          │  Collect results, trigger next stage
└─────────────────────┘


STAGE BOUNDARIES (where new stages start):
- groupBy / reduceByKey (shuffle for aggregation)
- join (shuffle both sides for sort-merge join)
- repartition (explicit shuffle)
- distinct (shuffle for dedup)
- orderBy / sort (range shuffle for global sort)
- coalesce does NOT create a new stage (narrow)

TASK EXECUTION:
- Tasks within a stage run in PARALLEL
- Tasks across stages run SEQUENTIALLY (stage N+1 waits for stage N)
- Within a stage, all narrow transformations are PIPELINED
  (filter + map + project all happen in one pass, no intermediate materialization)
"""

# ============ PIPELINING WITHIN A STAGE ============
"""
Within a single stage, Spark PIPELINES operations:

df.filter(col("salary") > 5000)  # Narrow
  .withColumn("bonus", col("salary") * 0.1)  # Narrow
  .select("name", "bonus")  # Narrow

All three operations happen in ONE pass over the data.
No intermediate DataFrame is materialized.
This is called "pipelining" or "operator fusion".

Each record flows through: filter → withColumn → select
before the next record is processed.
"""

print("\n=== Pipelined operations (all in 1 stage) ===")
df_pipelined = df.filter(col("salary") > 5000) \
    .withColumn("bonus", col("salary") * 0.1) \
    .select("name", "salary", "bonus")
df_pipelined.explain()
df_pipelined.show()

# ============ TASK METRICS (what to look for in Spark UI) ============
"""
In Spark UI -> Stages -> Click on a stage -> Task metrics:

Key metrics per task:
- Duration: How long the task took
- GC Time: Time spent in garbage collection
- Input Size: Data read from source
- Shuffle Read: Data read from previous stage's shuffle
- Shuffle Write: Data written for next stage
- Spill (Memory): Data spilled from memory
- Spill (Disk): Data spilled to disk

RED FLAGS:
- One task much slower than others → DATA SKEW
- High GC time → Memory pressure, increase memory or partitions
- Spill to disk → Not enough memory per task
- Many tasks with 0 input → Too many partitions for data size
"""

# ============ COUNTING JOBS/STAGES/TASKS ============

print("\n=== Predicting Spark UI behavior ===")
print("""
Operation                          | Jobs | Stages | Tasks per Stage
-----------------------------------|------|--------|----------------
df.show()                          | 1    | 1      | input_partitions
df.count()                         | 1    | 1      | input_partitions
df.groupBy().count().show()        | 1    | 2      | input_parts, shuffle_parts
df.join(df2, key).show()           | 1    | 3      | inp1, inp2, shuffle_parts
df.orderBy(col).show()             | 1    | 2      | input_parts, shuffle_parts
df.distinct().show()               | 1    | 2      | input_parts, shuffle_parts
df.repartition(N).show()           | 1    | 2      | input_parts, N
df.coalesce(N).show()              | 1    | 1      | input_parts (narrow!)
df.write.parquet(path)             | 1    | 1+     | depends on prior transforms
""")

# Write
df_pipelined.write.mode("overwrite").parquet("/shared/execution_model_demo")

spark.stop()
