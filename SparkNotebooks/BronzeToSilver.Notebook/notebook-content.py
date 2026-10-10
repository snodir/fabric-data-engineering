# Fabric notebook source


# CELL ********************

import findspark
findspark.init()

# CELL ********************

# Import the needed Python modules
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from datetime import datetime

# CELL ********************

spark = SparkSession.builder \
    .appName("BronzeToSilver Transformation") \
    .getOrCreate()

# CELL ********************

# Load the Bronze layer data in the notebook

df = spark.read.format("csv") \
    .option("header", True) \
    .load("Files/*.csv")

# CELL ********************

from pyspark.sql.functions import col, count, when, isnull

df.select([count(when(isnull(c), c)).alias(c) for c in df.columns]).show()

# CELL ********************

duplicates = df.groupBy("OrderID").count().filter("count > 1")

duplicates.show()

# CELL ********************

df_clean = df.dropDuplicates(["OrderID"])

df_clean.show()

# CELL ********************

avg_price = df_clean.agg({'Price': 'avg'}).collect()[0][0]
print(avg_price)

rounded_avg_price = round(avg_price, 2)
print(rounded_avg_price)

# CELL ********************

avg_qty = round(df_clean.agg({'Quantity': 'avg'}).collect()[0][0], 0)

rounded_avg_price = round(avg_price, 2)
print(rounded_avg_price)

# CELL ********************

df_clean = df_clean.fillna({'Price': rounded_avg_price})

df_clean.show()

# CELL ********************

avg_qty = round(df_clean.agg({'Quantity': 'avg'}).collect()[0][0], 0)

df_clean = df_clean.fillna({'Quantity': avg_qty})

df_clean.show()

# CELL ********************

from pyspark.sql.functions import to_date

df_clean = df_clean.withColumn('Date', to_date('Date', 'yyyy-MM-dd'))

display(df_clean)
print(df_clean.dtypes)

# CELL ********************

from pyspark.sql.functions import to_date, coalesce

df_clean = df_clean.withColumn(
    'Date',
    coalesce(
        to_date('Date', 'yyyy-MM-dd'),
        to_date('Date', 'dd-MM-yyyy')
    )
)

# CELL ********************

from pyspark.sql.functions import col, when, lit

default_date = to_date(lit('2025-01-01'), 'yyyy-MM-dd')

df_clean = df_clean.withColumn('Date',
            when(col('Date').isNull(), default_date)
            .otherwise(col('Date'))
)
display(df_clean)

# CELL ********************

df_valid = df_clean.filter(df_clean['quantity'] >= 0)

display(df_valid)

# CELL ********************

df_valid.select([count(when(isnull(c), c)).alias(c) for c in df.columns]).show()

# CELL ********************

df_valid = df_valid.fillna({'Customer': 'Unknown'})
display(df_valid)

# CELL ********************

# Save the Silver layer as a Delta table
df_valid.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("Files/silver_sales")

# CELL ********************

# Verify the saved files using Spark
display(spark.read.format("delta").load("Files/silver_sales"))
