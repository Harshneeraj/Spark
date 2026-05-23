"""
Topic: Serialization, Tungsten, and Whole-Stage Code Generation
================================================================

Spark's internal optimizations for CPU and memory efficiency.

Spark UI Behavior:
- Whole-stage codegen: In SQL tab, stages marked with "*" are code-generated.
  Example: "*HashAggregate" means codegen is active.
- Without codegen: "HashAggregate" (no asterisk).
- Codegen fuses multiple operators into a single Java function (fewer virtual calls).
- In Spark UI -> SQL -> query plan, look for WholeStageCodegen nodes.

Key Interview Points:
- Tungsten: Spark's execution engine for CPU/memory efficiency.
  - Off-heap memory management (avoids GC)
  - Cache-aware computation (CPU cache friendly)
  - Whole-stage code generation (fuses operators)
  - Binary processing (operates on serialized data)
- Kryo vs Java serialization: Kryo is 10x faster, more compact.
- Arrow: Columnar format for efficient Python <-> JVM transfer (Pandas UDFs).
- Whole-stage codegen: Compiles query plan into optimized Java bytecode.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count

spark = SparkSession.builder \
    .appName("25_Serialization_Tungsten") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.sql.codegen.wholeStage", "true") \
    .getOrCreate()

data = [(i, f"name_{i}", i % 5, i * 1000 + 30000) for i in range(1, 51)]
df = spark.createDataFrame(data, ["id", "name", "dept_id", "salary"])

# ============ WHOLE-STAGE CODE GENERATION ============
"""
What it does:
- Fuses multiple physical operators into a single Java function.
- Eliminates virtual function calls between operators.
- Generates specialized code for the specific query.

Without codegen (traditional Volcano model):
  Scan -> Filter -> Project -> Aggregate
  Each operator calls next() on the previous one (virtual dispatch overhead)

With codegen:
  Single generated function that does: scan + filter + project + aggregate
  No virtual calls, tight loop, CPU cache friendly.

In explain() output:
  *(1) HashAggregate  <- The * means codegen is active!
  +- *(1) Project
     +- *(1) Filter
        +- *(1) Scan
  
  All operators with same number (1) are fused into one codegen unit.
"""

print("=== Whole-Stage Codegen in Action ===")
df_result = df.filter(col("salary") > 40000) \
    .groupBy("dept_id") \
    .agg(sum("salary").alias("total"), count("*").alias("cnt"))

# Look for * (asterisk) in the plan - indicates codegen
df_result.explain(mode="formatted")
df_result.show()

# View generated code (for debugging)
# print(df_result.queryExecution.debug.codegen())

# ============ CODEGEN vs NO CODEGEN ============

# Disable codegen to compare
spark.conf.set("spark.sql.codegen.wholeStage", "false")
print("\n=== WITHOUT Codegen ===")
df.filter(col("salary") > 40000).groupBy("dept_id").sum("salary").explain()
# No * prefix on operators

# Re-enable
spark.conf.set("spark.sql.codegen.wholeStage", "true")
print("\n=== WITH Codegen ===")
df.filter(col("salary") > 40000).groupBy("dept_id").sum("salary").explain()
# * prefix on operators (fused)

# ============ TUNGSTEN MEMORY FORMAT ============
"""
Tungsten stores data in a compact binary format:
- No Java object overhead (16 bytes per object header saved)
- No boxing of primitives
- Data stored contiguously in memory (cache-friendly)
- Can operate directly on serialized binary data

Traditional Java objects:
  String "hello" = 16 (header) + 12 (char array header) + 10 (5 chars * 2 bytes) = 38+ bytes

Tungsten format:
  String "hello" = 4 (length) + 5 (UTF-8 bytes) = 9 bytes

This is why DataFrames are much more memory-efficient than RDDs of Java objects.
"""

# ============ KRYO SERIALIZATION ============
"""
When to use Kryo:
- RDD operations (map, flatMap, etc.)
- Shuffle data serialization
- Caching with MEMORY_ONLY_SER
- Broadcast variables

Kryo vs Java Serialization:
┌──────────────┬──────────────────┬─────────────────┐
│ Aspect       │ Java Serializer  │ Kryo Serializer │
├──────────────┼──────────────────┼─────────────────┤
│ Speed        │ Slow             │ 10x faster      │
│ Size         │ Large            │ Compact          │
│ Setup        │ None             │ Register classes │
│ Compatibility│ All classes      │ Most classes     │
└──────────────┴──────────────────┴─────────────────┘

Configuration:
  spark.serializer = org.apache.spark.serializer.KryoSerializer
  spark.kryo.registrationRequired = false  (true for strict mode)
  spark.kryoserializer.buffer.max = 64m
"""

print("\n=== Serializer Configuration ===")
print(f"Serializer: {spark.conf.get('spark.serializer')}")

# ============ APACHE ARROW (Python <-> JVM) ============
"""
Arrow enables efficient data transfer between Python and JVM:
- Used in: toPandas(), createDataFrame(pandas_df), Pandas UDFs
- Columnar format (same as Spark's internal format)
- Zero-copy reads possible
- 10-100x faster than row-by-row serialization

Enable with:
  spark.sql.execution.arrow.pyspark.enabled = true

Without Arrow (toPandas()):
  JVM -> serialize each row -> Python -> deserialize -> build pandas DataFrame
  
With Arrow (toPandas()):
  JVM -> Arrow columnar batch -> Python -> zero-copy to pandas
"""

import pandas as pd

print("\n=== Arrow-based toPandas() ===")
print(f"Arrow enabled: {spark.conf.get('spark.sql.execution.arrow.pyspark.enabled')}")

# This uses Arrow for fast transfer
pandas_df = df.toPandas()
print(f"Converted to Pandas: {len(pandas_df)} rows")
print(pandas_df.head())

# And back to Spark (also uses Arrow)
df_back = spark.createDataFrame(pandas_df)
df_back.show(5)

# ============ PERFORMANCE TIPS ============
"""
1. Always enable Kryo serializer for RDD workloads
2. Keep whole-stage codegen enabled (default)
3. Enable Arrow for Python interop
4. Use DataFrames over RDDs (Tungsten optimization)
5. Avoid UDFs when possible (break codegen pipeline)
6. Use built-in functions (participate in codegen)
"""

# Write
df_result.write.mode("overwrite").parquet("/shared/tungsten_demo")

spark.stop()
