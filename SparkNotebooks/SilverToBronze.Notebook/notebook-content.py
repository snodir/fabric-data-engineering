# Fabric notebook source


# CELL ********************

import findspark
findspark.init()

# CELL ********************

# Steps to create a simple silver sales table for practice

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# CELL ********************

spark = SparkSession.builder \
    .appName("SilverToGold Transformation") \
    .getOrCreate()

# CELL ********************



# --- 1. Define the Schema using StringType for the Date column ---
schema = StructType([
    StructField("Product",       StringType(),  True),
    StructField("OrderID",       IntegerType(), True),
    StructField("Date",          StringType(),  True),  # Note: Defined as STRING
    StructField("Customer",      StringType(),  True),
    StructField("Quantity",      IntegerType(), True),
    StructField("Price",         DoubleType(),  True),
    StructField("standard_name", StringType(),  True)
])

# --- 2. Define the Data ---
data = [
    ("Widget C", 1003, "2025-01-01", "Delta Inc",  1, 120.00, "Widget Gamma"),
    ("Widget B", 1002, "2025-01-01", "Beta LLC",   2,  40.00, "Widget Beta"),
    ("Widget B", 1004, "2025-01-01", "Acme Corp",  3,  40.00, "Widget Beta"),
    ("Widget A", 1005, "2025-01-02", "Acme Corp",  5,  25.00, "Widget Alpha"),
    ("Widget B", 1006, "2025-01-02", "Omega Corp", 3,  40.00, "Widget Beta"),
    ("Widget A", 1001, "2025-01-01", "Acme Corp",  5,  25.00, "Widget Alpha"),
    ("Widget D", 1007, "2025-01-01", "Omega Co",   3,  90.00, "Widget Delta"),
    ("Widget A", 1008, "2025-01-02", "Zeta Group", 4,  54.29, "Widget Alpha")
]

# CELL ********************

# --- 3. Create the DataFrame ---
df_silver = spark.createDataFrame(data, schema=schema)

# --- 4. Fix the Data Type: Convert the String 'Date' to DateType ---
# The correct format is "yyyy-MM-dd" for the strings in the data variable.
df_silver = df_silver.withColumn(
    "Date",
    to_date(col("Date"), "yyyy-MM-dd")
)

print("--- Generated Silver Dataset ---")
df_silver.printSchema()
display(df_silver)

# --- 5. Save the DataFrame as a Delta Table ---
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("small_silver_sales")

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE gold_monthly_sales_summary (
# MAGIC     product        STRING,
# MAGIC     sales_month    DATE,
# MAGIC     total_sales    DOUBLE,
# MAGIC     total_quantity DOUBLE,
# MAGIC     average_price  DOUBLE
# MAGIC ) USING DELTA;

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO gold_monthly_sales_summary
# MAGIC     SELECT
# MAGIC         Product                      AS product,
# MAGIC         DATE_TRUNC('month', Date)    AS sales_month,
# MAGIC         SUM(Price * Quantity)        AS total_sales,
# MAGIC         SUM(Quantity)                AS total_quantity,
# MAGIC         AVG(Price)                   AS average_price
# MAGIC     FROM silver_sales
# MAGIC     GROUP BY
# MAGIC         Product,
# MAGIC         DATE_TRUNC('month', Date);

# CELL ********************

# MAGIC %%sql
# MAGIC ALTER TABLE gold_monthly_sales_summary ADD COLUMN net_sales DOUBLE;
# MAGIC 
# MAGIC UPDATE gold_monthly_sales_summary
# MAGIC SET net_sales = total_sales * 0.9;
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM gold_monthly_sales_summary;

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE gold_monthly_sales_summary_partitioned
# MAGIC PARTITIONED BY (Product)
# MAGIC AS SELECT * FROM gold_monthly_sales_summary;
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM gold_monthly_sales_summary_partitioned;
# MAGIC 
# MAGIC DESCRIBE gold_monthly_sales_summary_partitioned;

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT
# MAGIC     Product,
# MAGIC     SUM(net_sales) AS product_sales
# MAGIC FROM gold_monthly_sales_summary
# MAGIC WHERE sales_month >= '2025-01-01'
# MAGIC   AND sales_month <= '2025-03-31'
# MAGIC GROUP BY Product
# MAGIC ORDER BY product_sales DESC;
