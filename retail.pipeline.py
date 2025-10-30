from pyspark.sql import SparkSession, functions as F, Window
from faker import Faker
import random
from datetime import datetime, timedelta

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

# ==== CONFIG ====
CUSTOMER_COUNT, PRODUCT_COUNT, ORDERS_COUNT = 1000, 300, 5000
RANDOM_SEED, DRY_RUN = 42, True
OUTPUT_BASE = "out/retail_pipeline"
random.seed(RANDOM_SEED)
Faker.seed(RANDOM_SEED)
fake = Faker()

# ==== SPARK SESSION ====
spark = SparkSession.builder \
    .appName("RetailSalesPipeline") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()
print(f"Spark {spark.version} started. DRY_RUN={DRY_RUN}")

# ==== DIM CUSTOMERS ====
genders = ["Male", "Female", "Other"]
customers_py = [(cid, fake.name(), random.randint(18, 75), random.choice(genders),
                 (country := fake.country()),
                 "North America" if "United States" in country or "Canada" in country else
                 "Europe" if country in ["France","Germany","Italy","Spain","United Kingdom"] else
                 "Asia" if country in ["India","China","Japan","Singapore"] else "Other")
                for cid in range(1, CUSTOMER_COUNT + 1)]
schema_customers = "customer_id INT, name STRING, age INT, gender STRING, country STRING, region STRING"
dim_customers = spark.createDataFrame(customers_py, schema_customers)

# ==== DIM PRODUCTS ====
categories = ["Electronics", "Clothing", "Home & Kitchen", "Sports", "Toys"]
subcategories = {
    "Electronics": ["Phone","Laptop","Headphones","Camera"],
    "Clothing": ["Shirt","Jeans","Jacket","Shoes"],
    "Home & Kitchen": ["Blender","Cookware","Lamp","Desk"],
    "Sports": ["Ball","Gloves","Shoes","Watch"],
    "Toys": ["Doll","Blocks","Car","Puzzle"]
}
products_py = [(pid, cat, sub, f"{cat}-{sub}-{pid:04d}", round(random.uniform(5.0, 1500.0), 2))
               for pid in range(1, PRODUCT_COUNT + 1)
               for cat in [random.choice(categories)]
               for sub in [random.choice(subcategories[cat])]]
schema_products = "product_id INT, category STRING, subcategory STRING, name STRING, base_price DOUBLE"
dim_products = spark.createDataFrame(products_py, schema_products)

# ==== FACT ORDERS (BRONZE) ====
def rand_ts(days_back=240):
    offset = random.randint(0, days_back)
    dt = datetime.now() - timedelta(days=offset)
    return dt.replace(hour=random.randint(0, 23), minute=random.randint(0, 59), second=random.randint(0, 59), microsecond=0)
channels = ["Online", "Store", "Marketplace"]
orders_py = [(f"ORD-{oid:09d}", rand_ts(), random.randint(1, CUSTOMER_COUNT),
              random.randint(1, PRODUCT_COUNT), random.randint(1, 5),
              random.choice(channels))
             for oid in range(1, ORDERS_COUNT + 1)]
schema_orders = "order_id STRING, order_ts TIMESTAMP, customer_id INT, product_id INT, quantity INT, channel STRING"
orders_bronze = spark.createDataFrame(orders_py, schema_orders)

# ==== SILVER ====
orders_silver = orders_bronze \
    .join(F.broadcast(dim_customers), "customer_id") \
    .join(F.broadcast(dim_products), "product_id") \
    .withColumn("total_amount", F.col("base_price") * F.col("quantity")) \
    .withColumn("order_date", F.to_date("order_ts")) \
    .withColumn("yyyymm", F.date_format("order_ts", "yyyyMM")) \
    .repartition(8, "yyyymm", "region").cache()
orders_silver.count()

# ==== GOLD ====
gold_monthly_region_category = orders_silver.groupBy("yyyymm", "region", "category") \
    .agg(F.sum("total_amount").alias("revenue"), F.sum("quantity").alias("units"))
cust_spend = orders_silver.groupBy("region", "customer_id") \
    .agg(F.sum("total_amount").alias("total_spent"))
win = Window.partitionBy("region").orderBy(F.desc("total_spent"))
gold_top_customers = cust_spend.withColumn("rank", F.dense_rank().over(win)).filter(F.col("rank") <= 5)
monthly_total = orders_silver.groupBy("yyyymm").agg(F.sum("total_amount").alias("revenue"))
win_rows = Window.orderBy("yyyymm").rowsBetween(-2, 0)
gold_moving_rev = monthly_total.withColumn("rev_ma_3m", F.avg("revenue").over(win_rows))

# ==== SAVE ====
if not DRY_RUN:
    orders_silver.write.mode("overwrite").partitionBy("yyyymm", "region").parquet(f"{OUTPUT_BASE}/silver_orders")
    gold_monthly_region_category.write.mode("overwrite").parquet(f"{OUTPUT_BASE}/gold_monthly_region_category")
    gold_top_customers.write.mode("overwrite").parquet(f"{OUTPUT_BASE}/gold_top_customers")
    gold_moving_rev.write.mode("overwrite").parquet(f"{OUTPUT_BASE}/gold_moving_rev")

# ==== SHOW ====
print("Silver sample:")
orders_silver.show(5, truncate=False)
print("Gold monthly revenue sample:")
gold_monthly_region_category.show(5, truncate=False)
print("Top customers sample:")
gold_top_customers.show(5, truncate=False)
print("Moving revenue trend sample:")
gold_moving_rev.show(5, truncate=False)

spark.stop()



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



# The explanation will be in **three layers** so you can adapt to the situation:

# 1. **High-level summary** (for quick answers)
# 2. **Detailed step-by-step walkthrough** (for deeper technical rounds)
# 3. **Prime.Coding coding reasoning** (why we wrote it this way)

# ---

# ## **1️⃣ High-Level Summary (30-second answer)**

# This PySpark pipeline is a **retail analytics simulation** using synthetic data from Faker.
# It builds a **Bronze → Silver → Gold** data architecture:

# * **Bronze:** Raw orders
# * **Silver:** Enriched orders (customers + products + calculated amounts)
# * **Gold:** Aggregated business metrics (monthly revenue by category & region, top customers, moving averages)
#   The script uses partitioning, caching, and broadcasting for efficiency, and outputs in Parquet format for optimized storage.

# ---

# ## **2️⃣ Step-by-Step Detailed Walkthrough**

# ### **Step 0 — Config & Spark Session**

# * Set constants like `CUSTOMER_COUNT`, `PRODUCT_COUNT`, and `ORDERS_COUNT` for dataset size.
# * Fix `RANDOM_SEED` for reproducibility (important in data science demos).
# * `SparkSession` is configured with:

#   * `local[*]` to run locally using all CPU cores
#   * `spark.sql.shuffle.partitions = 8` to control shuffle partitions

# ---

# ### **Step 1 — Dimension Tables (Customers & Products)**

# * **Customers:**

#   * Generated with `Faker` — includes name, age, gender, country.
#   * Country is mapped to **region** (`North America`, `Europe`, `Asia`, or `Other`).
# * **Products:**

#   * Categories & subcategories predefined.
#   * Random base price between **$5 and $1500**.

# ---

# ### **Step 2 — Fact Table (Bronze Orders)**

# * Orders generated randomly:

#   * `order_id` as `"ORD-000000001"`
#   * Random timestamp from the last **240 days**
#   * `customer_id`, `product_id`, `quantity`, and `channel` (Online, Store, Marketplace)
# * Stored in **Bronze layer** without transformations.

# ---

# ### **Step 3 — Silver Layer (Enrichment & Cleansing)**

# * Join orders with **customers** & **products** using **broadcast joins** (faster for small dimension tables).
# * Add computed columns:

#   * `total_amount` = `base_price * quantity`
#   * `order_date` (YYYY-MM-DD)
#   * `yyyymm` for monthly grouping
# * Repartitioned on `(yyyymm, region)` to optimize future aggregations.
# * Cached to avoid recomputing during Gold steps.

# ---

# ### **Step 4 — Gold Layer (Aggregations & Business KPIs)**

# 1. **Monthly Revenue by Region & Category**

#    * `.groupBy("yyyymm", "region", "category")` → sum revenue & units.
# 2. **Top Customers per Region**

#    * Rank customers by `total_spent` using a **Window function** with `dense_rank()`.
# 3. **3-Month Moving Average Revenue**

#    * Use `.rowsBetween(-2, 0)` over a month-ordered window.

# ---

# ### **Step 5 — Output & Display**

# * If `DRY_RUN = False` → Save all Silver & Gold tables as **Parquet** (partitioned for efficiency).
# * Otherwise, just `.show()` sample rows for quick verification.

# ---

# ## **3️⃣ Prime.Coding Reasoning**

# ### **Why Bronze → Silver → Gold?**

# * **Bronze** keeps *raw history* untouched.
# * **Silver** applies enrichment/cleaning — reusable in multiple reports.
# * **Gold** stores final, ready-to-consume analytics outputs.

# ### **Why Broadcast Joins?**

# * Small dimension tables fit in memory → avoids expensive shuffles.

# ### **Why Partition by `(yyyymm, region)`?**

# * Many queries are time & region based → partition pruning = faster reads.

# ### **Why Use Window Functions for Ranking & Moving Average?**

# * Efficient way to compute metrics across groups without self-joins.

# ---

# ✅ **Key takeaway for interview**
# This pipeline shows:

# * **ETL in PySpark**
# * **Data modeling for analytics**
# * **Performance optimizations** (broadcast join, partitioning, caching)
# * **Business metrics computation** (revenue trends, top customers)




# ## **Mock Interview — PySpark Retail Pipeline**

# ### **🟢 Level 1 — Basics (Warm-up)**

# **Q1.** What is the difference between the Bronze, Silver, and Gold layers in this pipeline?
# **A1.**

# * **Bronze:** Raw ingested data without transformation (orders as generated).
# * **Silver:** Enriched & cleansed data (joins with customers/products, derived columns).
# * **Gold:** Aggregated & business-ready data (monthly revenue, top customers, moving averages).

# **Q2.** Why do we store output in Parquet format instead of CSV?
# **A2.**

# * Parquet is **columnar** → faster reads for specific columns.
# * Compressed → smaller disk space.
# * Supports **schema evolution** and better integration with Spark optimizations.

# **Q3.** What role does Faker play in the script?
# **A3.** It generates synthetic datasets for testing and learning without needing real customer data.

# ---

# ### **🟡 Level 2 — Technical Concepts**

# **Q4.** Why did we use **broadcast joins** for customers and products?
# **A4.** Customers and products tables are small, so broadcasting them avoids shuffles by sending them to each executor’s memory, speeding up the join.

# **Q5.** What’s the purpose of `repartition("yyyymm", "region")` in the Silver layer?
# **A5.**

# * Groups data physically by month and region.
# * Enables **partition pruning**, reducing read time for queries filtering by these fields.

# **Q6.** How does the `dense_rank()` window function work in ranking top customers?
# **A6.** It assigns consecutive ranks without gaps (unlike `rank()`) within each region, based on total spend.

# ---

# ### **🔴 Level 3 — Performance & Architecture**

# **Q7.** Suppose the dataset grows to 500M rows. What optimizations would you apply?
# **A7.**

# * Increase shuffle partitions proportionally to cluster size.
# * Ensure proper **predicate pushdown** with Parquet.
# * Tune **executor memory and cores** in Spark configuration.
# * Consider **bucketing** on customer_id for repeated joins.

# **Q8.** Why is `cache()` used in the Silver layer?
# **A8.** Silver data is reused multiple times (for monthly, ranking, moving average). Caching avoids recomputation from Bronze.

# **Q9.** How does partitioning differ from bucketing in Spark?
# **A9.**

# * **Partitioning:** Splits data by column values into separate files/folders. Best for columns used in filters.
# * **Bucketing:** Divides data into fixed number of buckets using a hash function. Best for join optimizations.

# ---


