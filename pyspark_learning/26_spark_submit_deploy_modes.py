"""
Topic: spark-submit and Deploy Modes
======================================

How to submit Spark applications to a cluster.

Spark UI Behavior:
- Client mode: Spark UI runs on the machine where spark-submit is executed.
  Driver logs visible in terminal.
- Cluster mode: Spark UI runs on the cluster node where driver is allocated.
  Driver logs in YARN/cluster logs (not terminal).
- Application shows up in Resource Manager UI (YARN) or Spark Master UI (Standalone).

Key Interview Points:
- Client mode: Driver runs on the machine that submits. Good for interactive/debugging.
- Cluster mode: Driver runs on a cluster node. Good for production (fault-tolerant).
- Deploy mode affects where the driver runs, NOT where executors run.
- In YARN cluster mode, if driver node dies, YARN can restart it.
- In client mode, if submitting machine dies, the job dies.
"""

# This file is documentation-only (can't actually submit from within a script)

"""
============ SPARK-SUBMIT SYNTAX ============

spark-submit \\
    --master <master-url> \\
    --deploy-mode <client|cluster> \\
    --driver-memory <memory> \\
    --executor-memory <memory> \\
    --executor-cores <cores> \\
    --num-executors <count> \\
    --conf <key>=<value> \\
    --jars <additional-jars> \\
    --py-files <python-files> \\
    <application.py> \\
    [application-arguments]


============ MASTER URL OPTIONS ============

┌────────────────────────┬─────────────────────────────────────────────┐
│ Master URL             │ Description                                 │
├────────────────────────┼─────────────────────────────────────────────┤
│ local                  │ 1 thread locally                            │
│ local[N]               │ N threads locally                           │
│ local[*]               │ All available cores locally                 │
│ spark://host:7077      │ Standalone cluster                          │
│ yarn                   │ YARN cluster (Hadoop)                       │
│ k8s://host:port        │ Kubernetes cluster                          │
│ mesos://host:5050      │ Mesos cluster                               │
└────────────────────────┴─────────────────────────────────────────────┘


============ DEPLOY MODES ============

CLIENT MODE (--deploy-mode client):
┌─────────────────────────────────────────────────────────────────┐
│  Submitting Machine          │  Cluster                         │
│  ┌─────────────────────┐    │  ┌──────────────────────┐       │
│  │  DRIVER             │    │  │  Executor 1          │       │
│  │  - SparkContext     │◄───┼──│  - Tasks             │       │
│  │  - DAG Scheduler    │    │  └──────────────────────┘       │
│  │  - Task Scheduler   │    │  ┌──────────────────────┐       │
│  │  - Spark UI (4040)  │◄───┼──│  Executor 2          │       │
│  └─────────────────────┘    │  │  - Tasks             │       │
│                              │  └──────────────────────┘       │
└─────────────────────────────────────────────────────────────────┘

Use when: Interactive sessions, debugging, notebooks, development.
Pros: See logs directly, easy debugging.
Cons: If submitting machine dies, job dies. Network latency to cluster.


CLUSTER MODE (--deploy-mode cluster):
┌─────────────────────────────────────────────────────────────────┐
│  Cluster                                                         │
│  ┌─────────────────────┐  ┌──────────────────────┐             │
│  │  DRIVER (on cluster)│  │  Executor 1          │             │
│  │  - SparkContext     │◄─│  - Tasks             │             │
│  │  - DAG Scheduler    │  └──────────────────────┘             │
│  │  - Task Scheduler   │  ┌──────────────────────┐             │
│  │  - Spark UI         │◄─│  Executor 2          │             │
│  └─────────────────────┘  │  - Tasks             │             │
│                            └──────────────────────┘             │
└─────────────────────────────────────────────────────────────────┘

Use when: Production jobs, scheduled pipelines, long-running jobs.
Pros: Fault-tolerant (YARN can restart driver), no dependency on submitting machine.
Cons: Harder to debug (logs in cluster), can't use interactive mode.


============ EXAMPLE SPARK-SUBMIT COMMANDS ============

# Local development
spark-submit \\
    --master local[*] \\
    --driver-memory 2g \\
    my_app.py

# YARN client mode (interactive/debugging)
spark-submit \\
    --master yarn \\
    --deploy-mode client \\
    --driver-memory 4g \\
    --executor-memory 8g \\
    --executor-cores 4 \\
    --num-executors 10 \\
    --conf spark.sql.shuffle.partitions=200 \\
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \\
    --conf spark.sql.adaptive.enabled=true \\
    my_app.py

# YARN cluster mode (production)
spark-submit \\
    --master yarn \\
    --deploy-mode cluster \\
    --driver-memory 4g \\
    --driver-cores 2 \\
    --executor-memory 16g \\
    --executor-cores 5 \\
    --num-executors 20 \\
    --conf spark.dynamicAllocation.enabled=true \\
    --conf spark.dynamicAllocation.minExecutors=5 \\
    --conf spark.dynamicAllocation.maxExecutors=50 \\
    --conf spark.sql.shuffle.partitions=400 \\
    --conf spark.sql.adaptive.enabled=true \\
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \\
    --conf spark.memory.fraction=0.7 \\
    --py-files utils.zip \\
    --jars external-lib.jar \\
    my_app.py --input /data/input --output /data/output

# Kubernetes
spark-submit \\
    --master k8s://https://k8s-master:6443 \\
    --deploy-mode cluster \\
    --conf spark.kubernetes.container.image=my-spark-image:latest \\
    --conf spark.kubernetes.namespace=spark-jobs \\
    my_app.py


============ DYNAMIC ALLOCATION ============

Dynamic allocation automatically adds/removes executors based on workload.

Configs:
  spark.dynamicAllocation.enabled = true
  spark.dynamicAllocation.minExecutors = 2
  spark.dynamicAllocation.maxExecutors = 100
  spark.dynamicAllocation.initialExecutors = 5
  spark.dynamicAllocation.executorIdleTimeout = 60s  (remove idle executor after 60s)
  spark.dynamicAllocation.schedulerBacklogTimeout = 1s  (add executor if tasks pending)

Benefits:
- Cost savings (release resources when idle)
- Auto-scaling for varying workloads
- No need to guess executor count

Requirements:
- External shuffle service must be enabled (spark.shuffle.service.enabled=true)
  OR decommission enabled (Spark 3.1+)


============ COMMON INTERVIEW QUESTIONS ============

Q: Difference between client and cluster mode?
A: Client mode runs driver on submitting machine (good for debugging).
   Cluster mode runs driver on cluster node (good for production, fault-tolerant).

Q: What happens if driver dies in client mode?
A: Entire application fails. All executors are terminated.

Q: What happens if an executor dies?
A: Spark reschedules its tasks on other executors. Data is recomputed from lineage.

Q: How does dynamic allocation work?
A: Spark adds executors when tasks are pending, removes when idle.
   Requires external shuffle service to preserve shuffle data of removed executors.

Q: How to pass additional Python files?
A: Use --py-files (zip, egg, or .py files). They're added to PYTHONPATH.

Q: How to set configs?
A: Priority order (highest to lowest):
   1. SparkConf in code (spark.conf.set)
   2. spark-submit --conf flags
   3. spark-defaults.conf file
   4. Default values
"""

# Minimal runnable example
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("26_Deploy_Modes_Demo") \
    .master("local[*]") \
    .getOrCreate()

# Show current deploy info
print(f"App Name: {spark.sparkContext.appName}")
print(f"Master: {spark.sparkContext.master}")
print(f"Deploy Mode: local (running within script)")
print(f"Spark Version: {spark.version}")

data = [(1, "demo", 100)]
df = spark.createDataFrame(data, ["id", "name", "value"])
df.write.mode("overwrite").parquet("/shared/deploy_mode_demo")

spark.stop()
