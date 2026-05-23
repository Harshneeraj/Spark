"""
Topic: Predicate Pushdown and Partition Pruning
================================================

Two critical optimizations that reduce the amount of data read.

Spark UI Behavior:
- With predicate pushdown: Fewer bytes read from source (visible in stage input size).
- With partition pruning: Entire partitions/files are skipped (not even opened).
- In Spark UI -> Stages -> Input size will be much smaller with these optimizations.
- In explain() plan: Look for "PushedFilters" and "PartitionFilters".

Key Interview Points:
- Predicate Pushdown: Filter pushed to data source (Parquet, JDBC, etc.)
  Only matching rows are read from disk. Works with Parquet, ORC, JDBC.
- Partition Pruning: Entire directory partitions are skipped.
  Only relevant partition directories are scanned.
- Column Pruning: Only needed columns are read (columnar formats like Parquet).
- These happen automatically via Catalyst optimizer.
- Not all data sources support pushdown (CSV doesn't, Parquet does).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("27_Predicate_Pushdown_Partition_Pruning") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Create sample data
data = [
    (1, "Alice", "Engineering", "US", 90000),
    (2, "Bob", "Marketing", "UK", 45000),
    (3, "Charlie", "Engineering", "US", 65000),
    (4, "Diana", "HR", "IN", 55000),
    (5, "Eve", "Marketing", "US", 70000),
    (6, "Frank", "Engineering", "UK", 80000),
    (7, "Grace", "HR", "IN", 60000),
    (8, "Henry", "Marketing", "US", 52000)
]

df = spark.createDataFrame(data, ["id", "name", "department", "country", "salary"])

# ============ WRITE PARTITIONED DATA ============
# Create partitioned Parquet (directory structure by department and country)
df.write.mode("overwrite") \
    .partitionBy("department", "country") \
    .parquet("/shared/partitioned_employees")

# ============ PARTITION PRUNING ============
"""
When reading partitioned data and filtering on partition column,
Spark skips entire directories that don't match.

Directory structure:
/shared/partitioned_employees/
├── department=Engineering/
│   ├── country=UK/
│   │   └── part-00000.parquet
│   └── country=US/
│       └── part-00000.parquet
├── department=HR/
│   └── country=IN/
│       └── part-00000.parquet
└── department=Marketing/
    ├── country=UK/
    │   └── part-00000.parquet
    └── country=US/
        └── part-00000.parquet
"""

print("=== Partition Pruning Demo ===")
df_partitioned = spark.read.parquet("/shared/partitioned_employees")

# Filter on partition column -> PARTITION PRUNING
# Only reads department=Engineering directory!
df_eng = df_partitioned.filter(col("department") == "Engineering")
print("Plan with partition filter (look for PartitionFilters):")
df_eng.explain()
df_eng.show()

# Multiple partition filters
df_eng_us = df_partitioned.filter(
    (col("department") == "Engineering") & (col("country") == "US")
)
print("\nPlan with multiple partition filters:")
df_eng_us.explain()
df_eng_us.show()

# ============ PREDICATE PUSHDOWN ============
"""
Predicate pushdown pushes filter conditions to the data source level.
For Parquet: Uses min/max statistics in row group metadata to skip row groups.
For JDBC: Adds WHERE clause to the SQL query sent to database.
"""

# Write non-partitioned Parquet for pushdown demo
df.write.mode("overwrite").parquet("/shared/employees_flat")

print("\n=== Predicate Pushdown Demo ===")
df_flat = spark.read.parquet("/shared/employees_flat")

# Filter on non-partition column -> PREDICATE PUSHDOWN
# Parquet reader uses column statistics to skip row groups
df_high_salary = df_flat.filter(col("salary") > 70000)
print("Plan with predicate pushdown (look for PushedFilters):")
df_high_salary.explain()
df_high_salary.show()

# Multiple predicates
df_filtered = df_flat.filter(
    (col("salary") > 50000) & (col("name").isNotNull())
)
print("\nPlan with multiple pushed predicates:")
df_filtered.explain()
df_filtered.show()

# ============ COLUMN PRUNING ============
"""
Column pruning: Only reads columns that are needed.
Parquet is columnar -> can skip entire columns without reading them.
CSV is row-based -> must read entire row even if you need 1 column.
"""

print("\n=== Column Pruning Demo ===")
# Only reads 'name' and 'salary' columns from Parquet
df_pruned = df_flat.select("name", "salary")
print("Plan with column pruning (look for ReadSchema):")
df_pruned.explain()
df_pruned.show()

# ============ WHAT GETS PUSHED DOWN ============
"""
Pushdown-compatible predicates (Parquet):
✓ =, !=, <, >, <=, >=
✓ IS NULL, IS NOT NULL
✓ IN (list)
✓ AND, OR combinations of above
✓ BETWEEN

NOT pushed down:
✗ UDF-based filters (black box to optimizer)
✗ Complex expressions (LIKE with wildcards)
✗ Filters on computed columns
✗ Filters after joins (pushed to before join, but not to source)
"""

# Example: UDF breaks pushdown
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

@udf(returnType=BooleanType())
def custom_filter(salary):
    return salary > 70000

print("\n=== UDF breaks predicate pushdown ===")
df_udf_filter = df_flat.filter(custom_filter(col("salary")))
df_udf_filter.explain()
# Notice: No PushedFilters! UDF is opaque to optimizer.

# Compare with built-in filter
print("\n=== Built-in filter has pushdown ===")
df_builtin_filter = df_flat.filter(col("salary") > 70000)
df_builtin_filter.explain()
# Notice: PushedFilters: [salary > 70000]

# ============ JDBC PREDICATE PUSHDOWN ============
"""
For JDBC sources, predicates are pushed as SQL WHERE clauses:

df = spark.read.jdbc(url, table, properties=props)
df.filter(col("age") > 30)

Without pushdown: SELECT * FROM table (reads ALL data, filters in Spark)
With pushdown: SELECT * FROM table WHERE age > 30 (database filters)

This is HUGE for performance with large database tables!
"""

# ============ BEST PRACTICES ============
"""
1. Always partition large tables by frequently-filtered columns.
2. Use Parquet/ORC for predicate pushdown support.
3. Avoid UDFs in filter conditions (breaks pushdown).
4. Put most selective filters first (Catalyst reorders anyway, but good practice).
5. For JDBC: Use predicates option to push complex filters.
6. Check explain() plan to verify pushdown is happening.
"""

# Write final result
df_eng.write.mode("overwrite").parquet("/shared/predicate_pushdown_demo")

spark.stop()
