"""
Topic: Pivot and Unpivot (Melt) Operations
============================================

Reshaping DataFrames between wide and long formats.

Spark UI Behavior:
- pivot() is a WIDE transformation (causes shuffle for groupBy + pivot).
- show() on pivoted df -> 1 job -> 2 stages (read + shuffle/aggregate).
- Pivot with explicit values list is faster (no extra job to discover values).
- Pivot without values list triggers an extra job to collect distinct values!

Key Interview Points:
- pivot() converts rows to columns (long to wide format).
- unpivot/melt converts columns to rows (wide to long format).
- Always provide explicit values list to pivot() for performance.
- Without values list: Spark runs an extra job to find distinct values.
- Pivot is essentially groupBy + pivot column + aggregation.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, first, expr

spark = SparkSession.builder \
    .appName("29_Pivot_Unpivot") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Sales data (long format)
sales_data = [
    ("Alice", "Q1", "Electronics", 5000),
    ("Alice", "Q2", "Electronics", 6000),
    ("Alice", "Q1", "Clothing", 3000),
    ("Alice", "Q2", "Clothing", 3500),
    ("Bob", "Q1", "Electronics", 4000),
    ("Bob", "Q2", "Electronics", 4500),
    ("Bob", "Q1", "Clothing", 2000),
    ("Bob", "Q2", "Clothing", 2500),
    ("Charlie", "Q1", "Electronics", 7000),
    ("Charlie", "Q2", "Electronics", 7500),
    ("Charlie", "Q1", "Clothing", 4000),
    ("Charlie", "Q2", "Clothing", 4200)
]

df_sales = spark.createDataFrame(sales_data, ["salesperson", "quarter", "category", "revenue"])

print("=== Original Long Format ===")
df_sales.show()

# ============ PIVOT ============

# Pivot: Convert quarters from rows to columns
# Spark UI: 2 stages (groupBy shuffle + aggregate)
print("=== Pivot by Quarter (with explicit values - FAST) ===")
df_pivoted = df_sales.groupBy("salesperson", "category") \
    .pivot("quarter", ["Q1", "Q2"]) \
    .sum("revenue")
df_pivoted.show()

# Without explicit values (SLOWER - extra job to find distinct values!)
print("=== Pivot without explicit values (extra job!) ===")
df_pivoted_slow = df_sales.groupBy("salesperson", "category") \
    .pivot("quarter") \
    .sum("revenue")
df_pivoted_slow.show()

# Pivot by category
print("=== Pivot by Category ===")
df_cat_pivot = df_sales.groupBy("salesperson", "quarter") \
    .pivot("category", ["Electronics", "Clothing"]) \
    .sum("revenue")
df_cat_pivot.show()

# Pivot with multiple aggregations
print("=== Pivot with avg aggregation ===")
df_sales.groupBy("salesperson") \
    .pivot("quarter", ["Q1", "Q2"]) \
    .agg(avg("revenue").alias("avg_rev")) \
    .show()

# ============ UNPIVOT (Melt) - Wide to Long ============

# Method 1: Using stack() expression (most common)
print("=== Unpivot (stack) - Wide to Long ===")
df_wide = df_pivoted  # Use our pivoted result

# stack(N, col1_name, col1, col2_name, col2, ...)
# N = number of column pairs to unpivot
df_unpivoted = df_wide.select(
    "salesperson", "category",
    expr("stack(2, 'Q1', Q1, 'Q2', Q2) as (quarter, revenue)")
).filter(col("revenue").isNotNull())

df_unpivoted.show()

# Method 2: Using unpivot() (Spark 3.4+)
# df_wide.unpivot(["salesperson", "category"], ["Q1", "Q2"], "quarter", "revenue")

# Method 3: Union approach (works in all versions)
print("=== Unpivot using Union ===")
df_q1 = df_wide.select("salesperson", "category", lit("Q1").alias("quarter"), col("Q1").alias("revenue"))
df_q2 = df_wide.select("salesperson", "category", lit("Q2").alias("quarter"), col("Q2").alias("revenue"))

from pyspark.sql.functions import lit
df_union_unpivot = df_q1.union(df_q2).filter(col("revenue").isNotNull())
df_union_unpivot.orderBy("salesperson", "category", "quarter").show()

# ============ PRACTICAL EXAMPLE: Monthly Report ============

monthly_data = [
    ("Product_A", 100, 120, 150, 130),
    ("Product_B", 200, 180, 220, 250),
    ("Product_C", 50, 60, 55, 70)
]

df_monthly = spark.createDataFrame(monthly_data, 
    ["product", "jan_sales", "feb_sales", "mar_sales", "apr_sales"])

print("=== Monthly Report (Wide) ===")
df_monthly.show()

# Unpivot to long format for analysis
print("=== Monthly Report (Long - for analysis) ===")
df_monthly_long = df_monthly.select(
    "product",
    expr("""
        stack(4, 
            'January', jan_sales, 
            'February', feb_sales, 
            'March', mar_sales, 
            'April', apr_sales
        ) as (month, sales)
    """)
)
df_monthly_long.show()

# Now easy to do aggregations on the long format
print("=== Total sales per product ===")
df_monthly_long.groupBy("product").sum("sales").show()

print("=== Average sales per month ===")
df_monthly_long.groupBy("month").avg("sales").show()

# Write
df_pivoted.write.mode("overwrite").parquet("/shared/pivot_result")

spark.stop()
