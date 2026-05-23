"""
Topic: Important Spark SQL Functions Reference
================================================

Commonly used functions that appear in interviews and daily work.

Spark UI Behavior:
- All these functions are NARROW transformations (no shuffle).
- They execute within the same stage as the read.
- Built-in functions participate in whole-stage codegen (fast!).
- show() -> 1 job, 1 stage.

Key Interview Points:
- Always prefer built-in functions over UDFs (10-100x faster).
- Built-in functions are optimized by Catalyst and participate in codegen.
- String, Date, Math, Collection functions are most commonly asked.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    # String functions
    col, lit, concat, concat_ws, substring, length, trim, ltrim, rtrim,
    upper, lower, initcap, regexp_replace, regexp_extract, split,
    lpad, rpad, reverse, translate, instr, locate,
    
    # Date functions
    current_date, current_timestamp, date_format, to_date, to_timestamp,
    datediff, months_between, add_months, date_add, date_sub,
    year, month, dayofmonth, dayofweek, hour, minute, second,
    last_day, next_day, trunc, date_trunc,
    
    # Math functions
    abs, ceil, floor, round, sqrt, pow, log, exp,
    
    # Null functions
    coalesce, isnull, isnan, when, ifnull,
    
    # Collection functions
    array, array_contains, explode, posexplode, size,
    map_keys, map_values, create_map,
    
    # Aggregate functions
    count, sum, avg, min, max, collect_list, collect_set,
    
    # Other
    monotonically_increasing_id, spark_partition_id, input_file_name,
    struct, named_struct, hash, md5, sha2
)

spark = SparkSession.builder \
    .appName("37_SQL_Functions_Reference") \
    .master("local[*]") \
    .getOrCreate()

data = [
    (1, "  Alice Smith  ", "alice@email.com", "1990-05-15", 90000.567),
    (2, "Bob Jones", "bob@company.org", "1985-12-20", 45000.123),
    (3, "Charlie Brown", "charlie@email.com", "1992-03-10", 65000.789),
    (4, "Diana Prince", "diana@company.org", "1988-07-25", 55000.456),
    (5, "Eve Wilson", "eve@email.com", "1995-11-30", 70000.234)
]

df = spark.createDataFrame(data, ["id", "name", "email", "birth_date", "salary"])

# ============ STRING FUNCTIONS ============
print("=== STRING FUNCTIONS ===")

df.select(
    col("name"),
    trim(col("name")).alias("trimmed"),
    upper(col("name")).alias("upper"),
    lower(col("name")).alias("lower"),
    initcap(col("name")).alias("initcap"),
    length(trim(col("name"))).alias("len"),
    reverse(trim(col("name"))).alias("reversed")
).show(truncate=False)

# Substring and split
df.select(
    col("email"),
    substring(col("email"), 1, 5).alias("first_5"),
    split(col("email"), "@").alias("parts"),
    split(col("email"), "@")[0].alias("username"),
    split(col("email"), "@")[1].alias("domain")
).show(truncate=False)

# Regex
df.select(
    col("email"),
    regexp_extract(col("email"), r"@(.+)\.", 1).alias("domain_name"),
    regexp_replace(col("email"), r"@.+", "@masked.com").alias("masked_email")
).show(truncate=False)

# Concat
df.select(
    concat(lit("EMP_"), col("id").cast("string")).alias("emp_code"),
    concat_ws(" | ", col("name"), col("email")).alias("combined")
).show(truncate=False)

# Padding
df.select(
    col("id"),
    lpad(col("id").cast("string"), 5, "0").alias("padded_id")
).show()

# ============ DATE FUNCTIONS ============
print("\n=== DATE FUNCTIONS ===")

df_dates = df.withColumn("birth", to_date(col("birth_date"), "yyyy-MM-dd"))

df_dates.select(
    col("name"),
    col("birth"),
    year(col("birth")).alias("year"),
    month(col("birth")).alias("month"),
    dayofmonth(col("birth")).alias("day"),
    dayofweek(col("birth")).alias("dow"),
    datediff(current_date(), col("birth")).alias("days_alive"),
    months_between(current_date(), col("birth")).cast("int").alias("months_alive"),
    add_months(col("birth"), 6).alias("birth_plus_6m"),
    date_format(col("birth"), "dd-MMM-yyyy").alias("formatted"),
    last_day(col("birth")).alias("month_end"),
    trunc(col("birth"), "year").alias("year_start")
).show(truncate=False)

# ============ MATH FUNCTIONS ============
print("\n=== MATH FUNCTIONS ===")

df.select(
    col("salary"),
    round(col("salary"), 2).alias("rounded_2"),
    round(col("salary"), 0).alias("rounded_0"),
    ceil(col("salary")).alias("ceil"),
    floor(col("salary")).alias("floor"),
    abs(col("salary") - 60000).alias("abs_diff"),
    sqrt(col("salary")).alias("sqrt"),
    pow(col("salary") / 10000, 2).alias("squared")
).show()

# ============ CONDITIONAL FUNCTIONS ============
print("\n=== CONDITIONAL (when/otherwise) ===")

df.select(
    col("name"),
    col("salary"),
    when(col("salary") > 70000, "High")
    .when(col("salary") > 50000, "Medium")
    .otherwise("Low").alias("band"),
    coalesce(col("name"), lit("Unknown")).alias("safe_name")
).show()

# ============ COLLECTION FUNCTIONS ============
print("\n=== COLLECTION FUNCTIONS ===")

df_arrays = spark.createDataFrame([
    (1, [1, 2, 3, 4, 5]),
    (2, [10, 20, 30]),
    (3, [100])
], ["id", "numbers"])

df_arrays.select(
    col("id"),
    col("numbers"),
    size(col("numbers")).alias("array_size"),
    array_contains(col("numbers"), 3).alias("has_3"),
    col("numbers")[0].alias("first_element")
).show()

# Explode: Convert array to rows
print("=== explode() - array to rows ===")
df_arrays.select(col("id"), explode(col("numbers")).alias("number")).show()

# ============ STRUCT AND MAP ============
print("\n=== STRUCT and MAP ===")

df.select(
    struct(col("name"), col("email")).alias("person_struct"),
    create_map(lit("name"), col("name"), lit("email"), col("email")).alias("person_map")
).show(truncate=False)

# ============ UTILITY FUNCTIONS ============
print("\n=== UTILITY FUNCTIONS ===")

df.select(
    col("name"),
    monotonically_increasing_id().alias("unique_id"),
    spark_partition_id().alias("partition"),
    hash(col("name")).alias("hash_val"),
    md5(col("name")).alias("md5_val")
).show(truncate=False)

# Write
df.withColumn("birth", to_date(col("birth_date"))) \
    .write.mode("overwrite").parquet("/shared/functions_demo")

spark.stop()
