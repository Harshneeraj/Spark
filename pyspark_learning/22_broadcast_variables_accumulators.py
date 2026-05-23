"""
Topic: Broadcast Variables and Accumulators
=============================================

Shared variables for distributed computing.

Spark UI Behavior:
- Broadcast variable: Visible in Spark UI -> Environment tab.
  Sent once to each executor (not per task).
  No extra job triggered for broadcast creation.
  
- Accumulators: Visible in Spark UI -> Stages -> Accumulators section.
  Updated per task, aggregated at driver.
  No extra job - updates happen within existing tasks.

Key Interview Points:
- Broadcast variables: Read-only, sent once to each executor, cached in memory.
  Use for: lookup tables, configuration, ML model parameters.
  Different from broadcast() join hint!
  
- Accumulators: Write-only from executors, read at driver.
  Use for: counters, debugging metrics, error counting.
  WARNING: Accumulators in transformations may be updated multiple times
  (if task is retried). Only reliable in actions.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, DoubleType

spark = SparkSession.builder \
    .appName("22_Broadcast_Variables_Accumulators") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

sc = spark.sparkContext

# ============ BROADCAST VARIABLES ============
"""
Broadcast variables allow you to keep a read-only variable cached on each
executor rather than shipping a copy with every task.

Without broadcast: Variable is serialized with EVERY task (N tasks = N copies sent)
With broadcast: Variable is sent ONCE to each executor (E executors = E copies)
"""

# Example: Country code lookup (imagine this is a large dictionary)
country_codes = {
    "US": "United States",
    "UK": "United Kingdom",
    "IN": "India",
    "DE": "Germany",
    "JP": "Japan",
    "FR": "France"
}

# Broadcast the lookup dictionary
broadcast_countries = sc.broadcast(country_codes)

# Sample data
data = [
    (1, "Alice", "US", 90000),
    (2, "Bob", "UK", 45000),
    (3, "Charlie", "IN", 65000),
    (4, "Diana", "DE", 55000),
    (5, "Eve", "JP", 70000),
    (6, "Frank", "FR", 80000)
]

df = spark.createDataFrame(data, ["id", "name", "country_code", "salary"])

# Use broadcast variable in a UDF
@udf(returnType=StringType())
def get_country_name(code):
    """Lookup country name from broadcast variable"""
    lookup = broadcast_countries.value  # Access broadcast value
    return lookup.get(code, "Unknown")

print("=== Using Broadcast Variable in UDF ===")
df.withColumn("country_name", get_country_name(col("country_code"))).show()

# Example 2: Broadcast tax rates
tax_rates = {"US": 0.30, "UK": 0.25, "IN": 0.20, "DE": 0.28, "JP": 0.23, "FR": 0.27}
broadcast_tax = sc.broadcast(tax_rates)

@udf(returnType=DoubleType())
def calculate_tax(country, salary):
    """Calculate tax using broadcast tax rates"""
    rates = broadcast_tax.value
    rate = rates.get(country, 0.0)
    return float(salary) * rate

print("=== Tax Calculation with Broadcast ===")
df.withColumn("tax", calculate_tax(col("country_code"), col("salary"))).show()

# ============ ACCUMULATORS ============
"""
Accumulators are variables that are only "added" to through an associative
and commutative operation. Used for counters and sums.

Rules:
- Executors can only ADD to accumulators (write-only from executor side)
- Only the DRIVER can READ the accumulator value
- Accumulators in transformations may count wrong (task retries!)
- Accumulators in ACTIONS are guaranteed accurate
"""

# Create accumulators
high_salary_count = sc.accumulator(0)
total_salary_acc = sc.accumulator(0)
error_count = sc.accumulator(0)

# Use accumulators in an action (foreach) - RELIABLE
def process_row(row):
    global high_salary_count, total_salary_acc
    total_salary_acc += row["salary"]
    if row["salary"] > 60000:
        high_salary_count += 1

# foreach is an ACTION - accumulator values are reliable
df.foreach(process_row)

print(f"\n=== Accumulator Results ===")
print(f"High salary count (>60000): {high_salary_count.value}")
print(f"Total salary sum: {total_salary_acc.value}")

# Example: Counting bad records
data_with_errors = [
    (1, "Alice", "US", 90000),
    (2, "Bob", None, 45000),      # Missing country
    (3, "Charlie", "IN", -1),     # Invalid salary
    (4, "Diana", "DE", 55000),
    (5, None, "JP", 70000),       # Missing name
]

df_dirty = spark.createDataFrame(data_with_errors, ["id", "name", "country_code", "salary"])

null_name_count = sc.accumulator(0)
null_country_count = sc.accumulator(0)
invalid_salary_count = sc.accumulator(0)

def count_errors(row):
    if row["name"] is None:
        null_name_count.add(1)
    if row["country_code"] is None:
        null_country_count.add(1)
    if row["salary"] is not None and row["salary"] < 0:
        invalid_salary_count.add(1)

df_dirty.foreach(count_errors)

print(f"\n=== Data Quality Metrics (Accumulators) ===")
print(f"Null names: {null_name_count.value}")
print(f"Null countries: {null_country_count.value}")
print(f"Invalid salaries: {invalid_salary_count.value}")

# ============ ACCUMULATOR WARNING ============
"""
WARNING: Accumulators in TRANSFORMATIONS (map, filter) are NOT reliable!

Why? If a task fails and is retried, the accumulator is updated AGAIN.
This leads to double-counting.

SAFE: Use accumulators in actions (foreach, foreachPartition, count after filter)
UNSAFE: Using accumulators inside map/filter transformations

Example of UNSAFE usage:
  rdd.map(lambda x: (accumulator.add(1), x))  # May double-count!
"""

# ============ DESTROY BROADCAST ============
# Free memory when done
broadcast_countries.destroy()
broadcast_tax.destroy()

# Write
df.withColumn("country_name", get_country_name(col("country_code"))) \
    .write.mode("overwrite").parquet("/shared/broadcast_demo")

spark.stop()
