"""
Topic: Common PySpark Interview Scenarios and Solutions
========================================================

Real-world problems frequently asked in interviews with complete solutions.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, row_number, rank, dense_rank, lead, lag,
    sum, count, avg, max, min, collect_list,
    when, lit, concat, explode, split, trim,
    datediff, current_date, to_date, date_format,
    monotonically_increasing_id, broadcast
)
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("40_Interview_Scenarios") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# ============ SCENARIO 1: Second Highest Salary per Department ============
print("=" * 60)
print("SCENARIO 1: Second Highest Salary per Department")
print("=" * 60)

emp_data = [
    (1, "Alice", "Engineering", 90000),
    (2, "Bob", "Engineering", 85000),
    (3, "Charlie", "Engineering", 80000),
    (4, "Diana", "Marketing", 70000),
    (5, "Eve", "Marketing", 65000),
    (6, "Frank", "HR", 60000),
    (7, "Grace", "HR", 55000),
]

df_emp = spark.createDataFrame(emp_data, ["id", "name", "department", "salary"])

# Solution: Window function with dense_rank
window = Window.partitionBy("department").orderBy(col("salary").desc())

df_emp.withColumn("rank", dense_rank().over(window)) \
    .filter(col("rank") == 2) \
    .drop("rank") \
    .show()

# ============ SCENARIO 2: Running Total / Cumulative Sum ============
print("=" * 60)
print("SCENARIO 2: Running Total of Sales")
print("=" * 60)

sales_data = [
    ("2024-01-01", 100),
    ("2024-01-02", 200),
    ("2024-01-03", 150),
    ("2024-01-04", 300),
    ("2024-01-05", 250),
]

df_sales = spark.createDataFrame(sales_data, ["date", "amount"])

window_running = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_sales.withColumn("running_total", sum("amount").over(window_running)).show()

# ============ SCENARIO 3: Find Consecutive Days of Activity ============
print("=" * 60)
print("SCENARIO 3: Find Users with 3+ Consecutive Login Days")
print("=" * 60)

login_data = [
    ("user_1", "2024-01-01"),
    ("user_1", "2024-01-02"),
    ("user_1", "2024-01-03"),  # 3 consecutive!
    ("user_1", "2024-01-05"),
    ("user_2", "2024-01-01"),
    ("user_2", "2024-01-03"),  # Gap
    ("user_2", "2024-01-04"),
]

df_logins = spark.createDataFrame(login_data, ["user_id", "login_date"])
df_logins = df_logins.withColumn("login_date", to_date("login_date"))

# Technique: Subtract row_number from date to find groups
window_user = Window.partitionBy("user_id").orderBy("login_date")

df_consecutive = df_logins \
    .withColumn("rn", row_number().over(window_user)) \
    .withColumn("group_date", date_add(col("login_date"), -col("rn"))) \
    .groupBy("user_id", "group_date") \
    .agg(count("*").alias("consecutive_days")) \
    .filter(col("consecutive_days") >= 3)

from pyspark.sql.functions import date_add

df_consecutive.show()

# ============ SCENARIO 4: Pivot - Monthly Revenue by Product ============
print("=" * 60)
print("SCENARIO 4: Monthly Revenue Pivot")
print("=" * 60)

revenue_data = [
    ("Product_A", "Jan", 1000),
    ("Product_A", "Feb", 1200),
    ("Product_A", "Mar", 1100),
    ("Product_B", "Jan", 800),
    ("Product_B", "Feb", 900),
    ("Product_B", "Mar", 950),
]

df_revenue = spark.createDataFrame(revenue_data, ["product", "month", "revenue"])

df_revenue.groupBy("product") \
    .pivot("month", ["Jan", "Feb", "Mar"]) \
    .sum("revenue") \
    .show()

# ============ SCENARIO 5: SCD Type 2 (Slowly Changing Dimension) ============
print("=" * 60)
print("SCENARIO 5: SCD Type 2 - Track Historical Changes")
print("=" * 60)

# Current dimension table
current_data = [
    (1, "Alice", "Engineering", "2020-01-01", "9999-12-31", True),
    (2, "Bob", "Marketing", "2021-01-01", "9999-12-31", True),
]

# New incoming data (Alice changed department)
new_data = [
    (1, "Alice", "Management"),  # Changed!
    (2, "Bob", "Marketing"),     # No change
    (3, "Charlie", "Engineering"),  # New!
]

df_current = spark.createDataFrame(current_data, 
    ["id", "name", "department", "start_date", "end_date", "is_current"])
df_new = spark.createDataFrame(new_data, ["id", "name", "department"])

# Find changes
df_joined = df_current.filter(col("is_current") == True) \
    .join(df_new, "id", "full")

# Identify changed records
df_changed = df_joined.filter(
    (df_current["department"] != df_new["department"]) |
    df_current["id"].isNull()  # New records
)

print("Changed/New records:")
df_changed.show()

# In production: Close old records (set end_date, is_current=False)
# and insert new records (new start_date, is_current=True)

# ============ SCENARIO 6: Sessionization (Group Events into Sessions) ============
print("=" * 60)
print("SCENARIO 6: Sessionization (30-min gap = new session)")
print("=" * 60)

from pyspark.sql.functions import unix_timestamp, from_unixtime

events_data = [
    ("user_1", "2024-01-01 10:00:00"),
    ("user_1", "2024-01-01 10:05:00"),
    ("user_1", "2024-01-01 10:20:00"),  # Same session
    ("user_1", "2024-01-01 11:00:00"),  # New session (40 min gap)
    ("user_1", "2024-01-01 11:10:00"),
    ("user_2", "2024-01-01 09:00:00"),
    ("user_2", "2024-01-01 09:15:00"),
]

df_events = spark.createDataFrame(events_data, ["user_id", "event_time"])
df_events = df_events.withColumn("event_time", to_timestamp("event_time"))

# Calculate time gap from previous event
window_session = Window.partitionBy("user_id").orderBy("event_time")

df_sessions = df_events \
    .withColumn("prev_time", lag("event_time").over(window_session)) \
    .withColumn("gap_minutes", 
        (unix_timestamp("event_time") - unix_timestamp("prev_time")) / 60) \
    .withColumn("new_session", 
        when(col("gap_minutes") > 30, 1).when(col("prev_time").isNull(), 1).otherwise(0)) \
    .withColumn("session_id", 
        sum("new_session").over(window_session))

df_sessions.select("user_id", "event_time", "gap_minutes", "session_id").show(truncate=False)

# ============ SCENARIO 7: Explode and Flatten Nested Data ============
print("=" * 60)
print("SCENARIO 7: Flatten Nested/Array Data")
print("=" * 60)

nested_data = [
    (1, "Alice", "python,java,scala"),
    (2, "Bob", "java,go"),
    (3, "Charlie", "python,rust,c++"),
]

df_nested = spark.createDataFrame(nested_data, ["id", "name", "skills_csv"])

# Explode CSV string into rows
df_exploded = df_nested \
    .withColumn("skill", explode(split(col("skills_csv"), ","))) \
    .withColumn("skill", trim(col("skill"))) \
    .drop("skills_csv")

df_exploded.show()

# Count skills across all employees
df_exploded.groupBy("skill").count().orderBy(col("count").desc()).show()

# ============ SCENARIO 8: Gap and Island Problem ============
print("=" * 60)
print("SCENARIO 8: Find Missing IDs (Gaps)")
print("=" * 60)

id_data = [(1,), (2,), (3,), (5,), (6,), (9,), (10,)]
df_ids = spark.createDataFrame(id_data, ["id"])

# Find gaps using lead
window_id = Window.orderBy("id")
df_gaps = df_ids \
    .withColumn("next_id", lead("id").over(window_id)) \
    .filter(col("next_id") - col("id") > 1) \
    .select(
        (col("id") + 1).alias("gap_start"),
        (col("next_id") - 1).alias("gap_end")
    )

print("Missing ID ranges:")
df_gaps.show()

# ============ SCENARIO 9: Top N per Group (Efficient) ============
print("=" * 60)
print("SCENARIO 9: Top 2 Products per Category by Revenue")
print("=" * 60)

products_data = [
    ("Electronics", "Phone", 1000),
    ("Electronics", "Laptop", 1500),
    ("Electronics", "Tablet", 800),
    ("Clothing", "Shirt", 50),
    ("Clothing", "Pants", 80),
    ("Clothing", "Jacket", 200),
    ("Food", "Pizza", 15),
    ("Food", "Burger", 12),
    ("Food", "Sushi", 25),
]

df_products = spark.createDataFrame(products_data, ["category", "product", "price"])

window_cat = Window.partitionBy("category").orderBy(col("price").desc())

df_products.withColumn("rank", row_number().over(window_cat)) \
    .filter(col("rank") <= 2) \
    .drop("rank") \
    .show()

# ============ SCENARIO 10: Year-over-Year Growth ============
print("=" * 60)
print("SCENARIO 10: Year-over-Year Revenue Growth")
print("=" * 60)

yearly_data = [
    ("Product_A", 2021, 100000),
    ("Product_A", 2022, 120000),
    ("Product_A", 2023, 150000),
    ("Product_B", 2021, 80000),
    ("Product_B", 2022, 75000),
    ("Product_B", 2023, 90000),
]

df_yearly = spark.createDataFrame(yearly_data, ["product", "year", "revenue"])

window_yoy = Window.partitionBy("product").orderBy("year")

df_yearly.withColumn("prev_revenue", lag("revenue").over(window_yoy)) \
    .withColumn("yoy_growth_pct", 
        when(col("prev_revenue").isNotNull(),
             round((col("revenue") - col("prev_revenue")) / col("prev_revenue") * 100, 2))
    ).show()

# Write
df_exploded.write.mode("overwrite").parquet("/shared/interview_scenarios")

spark.stop()
