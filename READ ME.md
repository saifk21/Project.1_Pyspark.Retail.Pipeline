PySpark Retail Analytics ETL Pipeline

A comprehensive, end-to-end ETL pipeline built with PySpark to simulate and analyze retail sales data. This project demonstrates best practices in data engineering, from raw data generation to optimized, analytics-ready "Gold" tables.

It uses the faker library to generate realistic synthetic data and processes it through a scalable Bronze → Silver → Gold medallion architecture.

Project Overview

This pipeline simulates a common data engineering workflow:

Generate synthetic raw data for customers, products, and orders.

Ingest raw orders into a "Bronze" layer (raw, untouched data).

Enrich & Clean the data, moving it to a "Silver" layer. This involves joining datasets, calculating new metrics, and standardizing formats.

Aggregate & Transform the enriched data into "Gold" layers, which represent final, aggregated, business-ready metrics for analytics and reporting.

Pipeline Architecture

The pipeline follows the Bronze → Silver → Gold (Medallion) architecture, ensuring data quality, traceability, and reusability.

graph TD
    subgraph Generation
        A[Faker: dim_customers]
        B[Faker: dim_products]
        C[Faker: fact_orders]
    end

    subgraph Bronze Layer (Raw Storage)
        C -- Ingest --> D(orders_bronze)
    end

    subgraph Silver Layer (Enriched & Cleansed)
        D -- Join --> E[orders_silver]
        A -- Broadcast Join --> E
        B -- Broadcast Join --> E
    end

    subgraph Gold Layer (Aggregated Business KPIs)
        E -- Aggregate --> F[gold_monthly_region_category]
        E -- Aggregate + Window --> G[gold_top_customers]
        E -- Aggregate + Window --> H[gold_moving_rev]
    end

    subgraph Output
        F -- Write Parquet --> I[(Parquet Files)]
        G -- Write Parquet --> I
        H -- Write Parquet --> I
    end


Bronze Layer (orders_bronze): Raw, untouched, append-only order data. This is the "single source of truth."

Silver Layer (orders_silver): The Bronze data is cleaned, validated, and enriched by joining with customer and product dimensions. New calculations (like total_amount) are added. This layer is the source for most ad-hoc analytics.

Gold Layer (gold_* tables): Pre-aggregated tables that directly serve business intelligence (BI) dashboards or reports. Examples include monthly revenue summaries, top customer rankings, and moving averages.

Key Features

ETL Architecture: Implements the robust Bronze/Silver/Gold medallion architecture.

Synthetic Data Generation: Uses faker and random libraries for realistic, reproducible data simulation.

Performance Optimizations:

Broadcast Joins: Efficiently joins the large fact table with smaller dimension tables (dim_customers, dim_products).

Caching: Caches the hot orders_silver DataFrame in memory to avoid re-computation for each Gold table.

Partitioning: Writes output Parquet files partitioned by yyyymm and region to dramatically speed up read queries (via partition pruning).

Advanced Analytics:

Window Functions: Uses dense_rank() to calculate Top-N customers per region without expensive self-joins.

Moving Averages: Calculates a 3-month rolling revenue average using a rowsBetween window.

Configuration-Driven: Easily scalable by changing constants (CUSTOMER_COUNT, DRY_RUN, etc.) at the top of the script.

Reproducibility: Uses RANDOM_SEED to ensure the same "random" data is generated every time, making debugging and demos consistent.

How to Run

Prerequisites

Python 3.8+

Java 8 or 11 (Required by Spark)

Apache Spark (Binary Installation): This script assumes you have a full Apache Spark distribution downloaded and unzipped on your machine (e.g., spark-3.4.1-bin-hadoop3). You can download it from the official Spark website. pip install pyspark is not enough on its own.




 Set Environment Variables

This is the most important step. You must tell your system where to find your Spark installation.

On Windows (PowerShell):

# Point to your unzipped Spark folder
$env:SPARK_HOME = "C:\path\to\your\spark-3.x.x-bin-hadoop3"
# Point to the python.exe inside your virtual environment
$env:PYSPARK_PYTHON = ".\pyspark_env\Scripts\python.exe"


On Mac/Linux (Bash/Zsh):

# Point to your unzipped Spark folder
export SPARK_HOME="/path/to/your/spark-3.x.x-bin-hadoop3"
# Point to the python executable inside your virtual environment
export PYSPARK_PYTHON="./pyspark_env/bin/python"


 Run the Script

The script is set to DRY_RUN = True by default, which prints samples to the console without writing any files.

# This will run in "dry" mode
python Project.1_Retail.Sales.Analytics.Pipeline.py
or retail.pipeline.py


To generate the actual Parquet files in the /out directory, open pyspark_sales_pipeline.py and change the config variable:

# Set this to False to write Parquet files
DRY_RUN = False


Data Schema Deep Dive

Dimension Tables (Generated)

dim_customers
| Column | Type | Description |
|---|---|---|
| customer_id | INT | Unique Customer ID |
| name | STRING | Customer Name (Fake) |
| age | INT | Customer Age (18-75) |
| gender | STRING | Male, Female, or Other |
| country | STRING | Customer Country (Fake) |
| region | STRING | Mapped Region (NA, Europe, Asia, Other) |

dim_products
| Column | Type | Description |
|---|---|---|
| product_id | INT | Unique Product ID |
| category | STRING | Main Category (Electronics, Clothing, etc.) |
| subcategory | STRING | Sub-category (Phone, Shirt, etc.) |
| name | STRING | Product Name |
| base_price | DOUBLE | Product Price (5.0 - 1500.0) |

Bronze Layer

orders_bronze
| Column | Type | Description |
|---|---|---|
| order_id | STRING | Unique Order ID (e.g., ORD-000000001) |
| order_ts | TIMESTAMP | Full timestamp of the order |
| customer_id | INT | Foreign key to dim_customers |
| product_id | INT | Foreign key to dim_products |
| quantity | INT | Number of units purchased (1-5) |
| channel | STRING | Online, Store, or Marketplace |

Silver Layer

orders_silver (All columns from Bronze + Dims + New)
| Column | Type | Description |
|---|---|---|
| customer_id | INT | (Joined) |
| product_id | INT | (Joined) |
| order_id | STRING | (from Bronze) |
| order_ts | TIMESTAMP | (from Bronze) |
| quantity | INT | (from Bronze) |
| channel | STRING | (from Bronze) |
| name | STRING | (from dim_customers) |
| age | INT | (from dim_customers) |
| ... | ... | (all other customer/product fields) |
| total_amount | DOUBLE | base_price * quantity |
| order_date | DATE | to_date(order_ts) |
| yyyymm | STRING | date_format(order_ts, "yyyyMM") |

Gold Layer (Analytics Tables)

gold_monthly_region_category
| Column | Type | Description |
|---|---|---|
| yyyymm | STRING | Month identifier |
| region | STRING | Customer region |
| category | STRING | Product category |
| revenue | DOUBLE | Sum of total_amount for the group |
| units | LONG | Sum of quantity for the group |

gold_top_customers
| Column | Type | Description |
|---|---|---|
| region | STRING | Customer region |
| customer_id | INT | Customer ID |
| total_spent | DOUBLE | Total revenue from this customer |
| rank | INT | Dense rank (1-5) of customer by total_spent |

gold_moving_rev
| Column | Type | Description |
|---|---|---|
| yyyymm | STRING | Month identifier |
| revenue | DOUBLE | Total revenue for the month |
| rev_ma_3m | DOUBLE | 3-Month Moving Average of revenue |

Core Concepts & Optimizations Explained

Why Bronze → Silver → Gold?

This architecture decouples raw data from analytics.

Bronze is our "data lake." It's cheap storage and a permanent backup. If we find a bug in our Silver logic, we can rebuild the Silver layer from Bronze without re-ingesting.

Silver is our "data warehouse." It's clean, structured, and trustworthy. Data scientists and analysts can use this layer for ad-hoc queries.

Gold is our "data mart." It's highly aggregated and serves specific business needs (like a "Monthly Revenue" dashboard), making BI queries extremely fast.

The Power of Broadcast Joins

orders_bronze \
    .join(F.broadcast(dim_customers), "customer_id") \
    .join(F.broadcast(dim_products), "product_id")


Problem: Joining a large table (orders_bronze) with another large table is a "shuffle" operation, which is the most expensive operation in Spark. It sends matching data across the network to different worker nodes.
Solution: Our dimension tables (dim_customers, dim_products) are small. By marking them with F.broadcast(), we tell Spark: "Instead of shuffling the big table, send a full copy of this small table to every single worker node." The join then happens locally on each node, eliminating the network bottleneck.

Why Cache orders_silver?

orders_silver = ... .repartition(8, "yyyymm", "region").cache()
orders_silver.count() # Action to trigger the cache


Problem: We use orders_silver three separate times to build our three Gold tables. By default, Spark's lazy execution would re-calculate orders_silver from scratch three times.
Solution: .cache() tells Spark to "keep this DataFrame in memory after you compute it the first time." The .count() is an action that forces the computation. Now, all three Gold table aggregations read from the lightning-fast in-memory cache instead of re-running the entire Silver ETL.

Why Partition by yyyymm and region?

orders_silver.write.mode("overwrite") \
    .partitionBy("yyyymm", "region").parquet(...)


Problem: An analyst wants to see "all sales in the 'Europe' region for June 2024." Without partitioning, Spark would have to read every single row of the entire dataset just to find the few that match.
Solution: Partitioning saves the data in a folder structure like .../yyyymm=202406/region=Europe/data.parquet. When the analyst queries, Spark's "partition pruning" feature knows to only read the data inside that one specific folder, skipping 99% of the data and returning an answer in seconds instead of minutes.

How to Talk About This Project (Interview Points)

"I designed a full Bronze, Silver, and Gold ETL pipeline in PySpark to simulate and analyze a retail dataset, generating synthetic data with Faker."

"I optimized the core transformation logic by broadcasting the small customer and product dimension tables, which eliminated the network shuffle and dramatically improved the join performance on the large order fact table."

"To support multiple downstream aggregations, I cached the Silver-layer DataFrame after its initial computation, preventing Spark from redundantly re-computing the enrichment logic for each Gold table."

"For the data output, I wrote the Silver layer as partitioned Parquet files based on yyyymm and region. This enables partition pruning, making any future queries that filter by date or region extremely fast."

"I used Window functions to efficiently calculate complex, business-facing metrics like Top-5 customers per region and a 3-month moving revenue average, all within a single Spark transformation."

"The entire pipeline is reproducible due to RANDOM_SEED and idempotent because it uses mode("overwrite"), ensuring a clean, consistent state every time it runs."

Future Enhancements & Ideas

This project is a strong foundation. Here are some great next steps:

[ ] Add a Returns Metric: Simulate a ~5% return rate (is_return = (rand() < 0.05)) and add a net_revenue calculation to the Gold tables.

[ ] Calculate Channel Mix: Add a Gold table to compute the monthly percentage of revenue coming from "Online," "Store," and "Marketplace".

[ ] Handle Data Skew: If one region (e.g., "Other") dominates, the partition for that region will be massive. Implement "salting" (e.g., adding a salt_key = (rand()*5).cast("int")) to the groupBy to break that large partition into smaller, parallelizable chunks.

[ ] Implement Logging & Error Handling: Wrap key transformations in try/except blocks and use Python's logging module.

[ ] Add Unit Tests: Use pytest to write unit tests for individual transformation functions