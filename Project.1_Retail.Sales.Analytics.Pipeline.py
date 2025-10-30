# 🎯 Project Objective (Interview-Ready)

# Simulate a real data engineering workflow you can confidently discuss in interviews:

# Generate large-scale synthetic retail data (orders, customers, products) with Faker.

# Build an ETL pipeline in PySpark: Bronze → Silver → Gold layers.

# Apply joins, aggregations, window analytics, partitioning, caching, broadcast joins.

# Save (optionally) to Parquet with partitioning for analytics-style access.

# Run Spark SQL queries on the final datasets.


# 🧰 Tools & Environment

# PySpark 3.3.4 (already installed)

# Java 11 (Temurin) (already installed)

# Python 3.12 (you’re using this; working for you with 3.3.4)

# Libraries: faker, pyspark, pandas (optional)

# Storage format: Parquet (compact, columnar, fast)

# Optional: Delta Lake (skip for space; can add later)

# 🗂️ Data Model (Simple Star Schema)

# Fact table: orders_fact
# Columns: order_id, order_ts, customer_id, product_id, quantity, unit_price, region, channel

# Dims: dim_customers (id, age, gender, country), dim_products (id, category, subcategory, name)

# 🧭 Methodology (Step‑by‑Step)

# Generate synthetic customers, products, orders with Faker (parameterized scale).

# Ingest into Spark → Bronze (raw DataFrames).

# Silver: clean types, derive columns (e.g., total_amount, order_date, yyyymm).

# Gold: business aggregates (monthly revenue, top categories per region, top customers).

# Optimize: broadcast joins, partitioning, caching, shuffle tuning.

# (Optional) Save to Parquet partitioned by yyyymm and region.

# Register SQL views and run BI-style queries.

# ⚙️ Run Strategy (No Files Created Automatically)

# Each code block below is complete.

# There’s a DRY_RUN switch: when True, we skip all writes (so nothing is created on disk).

# When you’re ready to save outputs, set DRY_RUN = False and adjust paths.

# 🧪 PRIME.CODING — Block 1: Config & Spark Session
# 🔍 📘 Why?

# We centralize config (scale, paths, switches) and create a Spark session tuned for local development and 
# predictable behavior.

# 🧱 👨🏫 What You Did

# Defined knobs: how many customers/products/orders to generate.

# Added DRY_RUN safety to avoid disk writes.

# Created a SparkSession with a reasonable shuffle.partitions.

# --- Project 1: Config + Spark Session ---
from pyspark.sql import SparkSession
import os
import sys 
# import findspark # <-- REMOVE OR COMMENT OUT THIS LINE

# =========================================================
# FORCE SPARK_HOME to point to the correct installation path
# This is the definitive fix for the 'JavaPackage' error.
# =========================================================
# The path should be up to the 'pyspark' folder within site-packages
os.environ['SPARK_HOME'] = r'D:\Uncodemy\Pyspark\pyspark_env\Lib\site-packages\pyspark' 

# Set the Python interpreter for PySpark (keep this)
os.environ['PYSPARK_PYTHON'] = r'D:\Uncodemy\Pyspark\pyspark_env\Scripts\python.exe'

# Ensure the Spark libraries are on the path
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python/lib/py4j-0.10.9.5-src.zip'))
# =========================================================


# =========Project Config=========
CUSTOMER_COUNT = 10_000 # number of synthetic customers to generate
PRODUCT_COUNT  = 3_000         # number of synthetic products to generate
ORDERS_COUNT   = 200_000       # number of synthetic orders to generate (increase to stress test)
RANDOM_SEED    = 42            # reproducible fake data
DRY_RUN        = True          # True = no files written; False = writes Parquet outputs
# ===== Create Spark Session =====
spark = SparkSession.builder \
    .appName("RetailSalesAnalyticsAnalyticsProject") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()
# ... rest of your code ...


# Print banner (sanity)
print(f"Spark version: {spark.version}, DRY_RUN={DRY_RUN}")  


# 🔁 Full Syntax Breakdown

# SparkSession.builder...getOrCreate() → constructs (or reuses) the Spark driver session.

# .master("local[*]") → run locally using all CPU cores (*).

# .config("spark.sql.shuffle.partitions","8") → sets number of shuffle output partitions.

# 🧠 Analogy

# Think of SparkSession as turning on the factory power grid and setting conveyor belt speed.

# 🪞 Visual Imagination

# A control room dashboard: switches for parallelism and safety locks (DRY_RUN).

# 🧾 Summary Table
# Setting	Meaning
# CUSTOMER_COUNT	Size of dim_customers
# PRODUCT_COUNT	Size of dim_products
# ORDERS_COUNT	Rows in orders_fact
# DRY_RUN	Prevents disk writes 


# Block 2: Generate Dimensions

# 🔍 📘 Why?

# We need lookup tables (dimensions) to join with orders and support analytics (customer/segment, product/category).

# 🧱 👨🏫 What You Did

# Used Faker to build customers and products deterministically.

# Created Spark DataFrames for both dims.

from faker import Faker
import random
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import os   
os.environ['PYSPARK_PYTHON'] = r'D:\Uncodemy\Pyspark\pyspark_env\Scripts\python.exe'

# Set deterministic seed for reproducibility
random.seed(RANDOM_SEED)
fake = Faker()
Faker.seed(RANDOM_SEED)

# ----- Customers -----
# Create Python list of tuples for customers
customers_py = []
genders = ["Male", "Female", "Other"]
for cid in range(1, CUSTOMER_COUNT + 1):
    # Each tuple: (customer_id, name, age, gender, country, region)
    name    = fake.name()
    age     = random.randint(18, 75)
    gender  = random.choice(genders)
    country = fake.country()
    # Derive a coarse region from country text (very rough)
    region  = ("North America" if "United States" in country or "Canada" in country else
               "Europe" if country in ["France","Germany","Italy","Spain","United Kingdom"] else
               "Asia" if country in ["India","China","Japan","Singapore"] else
               "Other")
    customers_py.append((cid, name, age, gender, country, region))
    
    
schema_customers = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("customer_name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("country", StringType(), True),
    StructField("region", StringType(), True),
])

dim_customers = spark.createDataFrame(customers_py, schema_customers)

# ----- Products -----
categories    = ["Electronics", "Clothing", "Home & Kitchen", "Sports", "Toys"]
subcategories = {
    "Electronics": ["Phone", "Laptop", "Headphones", "Camera"],
    "Clothing": ["Shirt", "Jeans", "Jacket", "Shoes"],
    "Home & Kitchen": ["Blender", "Cookware", "Lamp", "Desk"],
    "Sports": ["Ball", "Gloves", "Shoes", "Watch"],
    "Toys": ["Doll", "Blocks", "Car", "Puzzle"]
}


products_py = []
for pid in range(1, PRODUCT_COUNT + 1):
    cat = random.choice(categories)
    sub = random.choice(subcategories[cat])
    name = f"{cat}-{sub}-{pid:04d}"
    price = round(random.uniform(5.0, 1500.0), 2)   # base price and unit
    products_py.append((pid, cat, sub, name, price))
    
schema_products = "product_id INT, category STRING, subcategory STRING, product_name STRING, base_price DOUBLE"
dim_products = spark.createDataFrame(products_py, schema_products)

print("Dims ready:", dim_customers.count(), dim_products.count())

# 🔁 Full Syntax Breakdown

# StructType([...]) / StructField → define schema (columns, types, nullability).

# spark.createDataFrame(list, schema) → creates a distributed DataFrame from Python list.

# round(random.uniform(...), 2) → create realistic prices.

# 🧠 Analogy

# Dimensions are your catalogs: a customer directory and a product catalog.

# 🪞 Visual Imagination

# Two neatly labeled shelves: Customers and Products; each product tags with category/subcategory.

# 🧾 Summary Table
# DataFrame	Rows	Key Columns
# dim_customers	CUSTOMER_COUNT	customer_id, region
# dim_products	PRODUCT_COUNT	product_id, category, base_price



# Block 3: Generate Orders (Fact), ETL Bronze→Silver→Gold


# 📘 Why?

# Orders are the event stream we analyze. We simulate realistic orders with timestamps, quantities, and channels.
# We then clean & enrich to usable analytics tables.

# 🧱 👨🏫 What You Did

# Built orders as Python tuples, then created a Spark DataFrame (orders_bronze).

# Derived Silver columns: total_amount, order_date, yyyymm.

# Joined with dimensions (broadcast small dims).

# Produced Gold aggregates: monthly revenue by region/category, top customers, etc.

# Added optimizations: caching, controlling partitions.


# --- Generate Orders (Fact) ---
from datetime import datetime, timedelta
import math


# Helper to random timestamp across N days
def random_ts(days_back: int = 180) -> datetime:
    # pick a random day offset within past 'days_back' days
    offset = random.randint(0, days_back)
    base = datetime.now() - timedelta(days=offset)
     # random time within the day
    return base.replace(hour=random.randint(0,23), minute=random.randint(0,59), second=random.randint(0,59), microsecond=0)

channels = ["Online", "Store", "Marketplace"]

orders_py = []
for oid in range(1, ORDERS_COUNT + 1):
    cust_id = random.randint(1, CUSTOMER_COUNT)
    prod_id = random.randint(1, PRODUCT_COUNT)
    ts      = random_ts(240)                    # past ~8 months
    qty     = random.randint(1, 5)
    ch      = random.choice(channels)
    # region will be derived from customer later; keep placeholder to demonstrate enrichment
    orders_py.append((f"ORD-{oid:09d}", ts, cust_id, prod_id, qty, ch))
    
schema_orders = """
  order_id STRING,
  order_ts TIMESTAMP,
  customer_id INT,
  product_id INT,
  quantity INT,
  channel STRING
"""

orders_bronze = spark.createDataFrame(orders_py, schema_orders)

# --- Silver: Clean & Enrich ---
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast, col, lit

# join with customers to get region/country (broadcast small dim)
orders_enriched = orders_bronze.join(broadcast(dim_customers.select("customer_id","region","country")), "customer_id", "left") \
    .join(broadcast(dim_products.select("product_id","category","subcategory","base_price")), "product_id", "left")
    


# derive metrics
orders_silver = orders_enriched \
    .withColumn("unit_price", F.col("base_price")) \
    .withColumn("total_amount", F.col("unit_price") * F.col("quantity")) \
    .withColumn("order_date", F.to_date("order_ts")) \
    .withColumn("yyyymm", F.date_format("order_ts", "yyyyMM"))
    
    

# (Optional) tune partitions for downstream shuffles
orders_silver = orders_silver.repartition(8, F.col("yyyymm"), F.col("region"))


# Cache reused dataset
orders_silver.cache()
_ = orders_silver.count()  # materialize cache

# --- Gold: Business Aggregates ---

# 1) Monthly revenue by region & category
gold_monthly_region_category = orders_silver.groupBy("yyyymm","region","category") \
    .agg(F.sum("total_amount").alias("revenue"), F.sum("quantity").alias("units")) \
    .orderBy("yyyymm","region","category")
    
# 2) Top customers by total spending (per region)
cust_spend = orders_silver.groupBy("region","customer_id") \
    .agg(F.sum("total_amount").alias("total_spent"))
    
win = Window.partitionBy("region").orderBy(F.desc("total_spent"))
gold_top_customers = cust_spend.withColumn("rank", F.dense_rank().over(win)) \
    .filter(F.col("rank") <= 10)
    
# 3) Rolling 3-month moving revenue overall
# First compute monthly totals
monthly_total = orders_silver.groupBy("yyyymm").agg(F.sum("total_amount").alias("revenue"))
win_rows = Window.orderBy("yyyymm").rowsBetween(-2, 0)
gold_moving_rev = monthly_total.withColumn("rev_ma_3m", F.avg("revenue").over(win_rows))


# --- Optional: Persist results (guarded by DRY_RUN) ---
if not DRY_RUN:
    OUTPUT_BASE = r"D:\Uncodemy\Pyspark\Project 1\retail_pipleline"
    base = OUTPUT_BASE
    orders_silver.write.mode("overwrite").partitionBy("yyyymm", "region").parquet(f"{base}/silver_orders")
    gold_monthly_region_category.write.mode("overwrite").parquet(f"{base}/gold_monthly_region_category")
    gold_top_customers.write.mode("overwrite").parquet(f"{base}/gold_top_customers")
    gold_moving_rev.write.mode("overwrite").parquet(f"{base}/gold_moving_rev")
    
    
# Register SQL views for ad-hoc analysis
orders_silver.createOrReplaceTempView("orders_silver")
gold_monthly_region_category.createOrReplaceTempView("gold_monthly_region_category")
gold_top_customers.createOrReplaceTempView("gold_top_customers")
gold_moving_rev.createOrReplaceTempView("gold_moving_rev")

print("Silver/Gold ready. Example counts:",
    orders_silver.count(), gold_monthly_region_category.count(), gold_top_customers.count(), gold_moving_rev.count())


# 🔁 Full Syntax Breakdown

# .join(broadcast(df2), "key", "left") → broadcast join: Spark ships small dim to all executors (no shuffle on big fact).

# .withColumn("total_amount", col("unit_price") * col("quantity")) → column derivation.

# F.date_format("order_ts","yyyyMM") → partitionable month key.

# .repartition(8, col("yyyymm"), col("region")) → control partition count + colocate by keys.

# Window.partitionBy(...).orderBy(...); dense_rank() → top‑N per region.

# rowsBetween(-2,0) → 3-row moving window = current + 2 prev.

# 🧠 Analogies

# Bronze → Silver → Gold is like raw → cleaned → curated sections in a grocery warehouse.

# Broadcast join is like handing each worker a tiny catalog, so they don’t run across the warehouse to check product names.

# 🪞 Visual Imagination

# Picture conveyor belts (partitions) sorted by yyyymm and region, with workers stamping totals and ranks as boxes pass by.

# 🧾 Summary Table
# Layer	DF	What it Contains
# Bronze	orders_bronze	Raw orders (ids, ts, qty, product, customer, channel)
# Silver	orders_silver	Enriched with region/category + derived totals & month
# Gold	gold_*	Aggregates: monthly rev, top customers, moving averages
    

# Block 4: BI Queries (Spark SQL)

# 🔍 📘 Why?

# Analysts + interviews love SQL. Show you can expose DataFrames as SQL and answer questions fast.

# 🧱 👨🏫 What You Did

# Registered views and wrote three BI-style queries.   

# 1) Top 5 categories per region in the last full month available
sql_top5 = spark.sql("""
WITH last_month AS (
  SELECT max(yyyymm) AS ym FROM gold_monthly_region_category
)
SELECT g.region, g.category, g.revenue
FROM gold_monthly_region_category g, last_month m
WHERE g.yyyyMM = m.ym
QUALIFY row_number() OVER (PARTITION BY g.region ORDER BY g.revenue DESC) <= 5
""")
# Note: If your Spark version doesn't support QUALIFY, emulate via Window in DataFrame API.


# 2) Revenue trend with 3-month moving average
sql_trend = spark.sql("""
SELECT yyyymm, revenue, rev_ma_3m
FROM gold_moving_rev
ORDER BY yyyymm
""")


# 3) Who are the top customers by region?
sql_top_customers = spark.sql("""
SELECT region, customer_id, total_spent, rank
FROM gold_top_customers
ORDER BY region, rank
""")

# Show samples (safe; does not write files)
sql_trend.show(10, truncate=False)
sql_top_customers.show(20, truncate=False)


# 🔁 Full Syntax Breakdown

# WITH last_month AS (...) → common table expression (CTE).

# QUALIFY (BigQuery/Snowflake style). If not supported in your Spark SQL, do ranking in DataFrame and filter rank <= 5.

# ORDER BY organizes final output for readability.

# 🧠 Analogy

# SQL view is a window into your DataFrame — same warehouse, different reporting screens.

# 🪞 Visual Imagination

# Multiple dashboards lighting up: trend, top categories, top customers.

# 🧾 Summary Table
# Query	Insight
# sql_top5	Top 5 categories per region (last month)
# sql_trend	Monthly revenue + moving average
# sql_top_customers	Top spenders by region

# 🧠 Practice Exercises (with Answers)

# Exercise: Add a return rate metric: simulate ~5% of orders as returns and reduce revenue accordingly.
# Answer idea: Add is_return = (rand() < 0.05) and compute net_revenue = sum(total_amount * (1 - is_return)).
# from pyspark.sql.functions import rand
# is_return = (rand() < 0.05).cast("int")
# net_revenue = sum(total_amount * (1 - is_return))

# Exercise: Compute channel mix (Online/Store/Marketplace) monthly percentages.
# Answer idea: groupBy(yyyymm, channel).agg(sum(total_amount)); then window per yyyymm to divide by monthly total.

# Exercise: Detect price outliers per product using z-score.
# Answer idea: Window by product_id to compute mean/std; flag abs(price-mean) > 3*std.

# Exercise: Skew handling — if region='Other' dominates, salt by adding salt = (rand()*5).cast("int") before a heavy groupBy.

# 🎤 Interview Talking Points (Ready-to-Use)

# I designed a Bronze → Silver → Gold ETL in PySpark using Faker to simulate millions of rows.

# I optimized joins via broadcast of small dimensions; set spark.sql.shuffle.partitions; and used partitionBy(yyyymm, region) for output.

# I cached hot DataFrames and coalesced partitions before writes to save disk I/O.

# I exposed DataFrames as SQL views and produced monthly revenue, moving averages, and top customers per region.

# The pipeline is scalable (parameters for data size) and idempotent (overwrite mode + deterministic seeding).