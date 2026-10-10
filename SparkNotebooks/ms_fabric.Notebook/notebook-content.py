# Fabric notebook source


# CELL ********************

# import findspark
# findspark.init()

# CELL ********************

import os, sys, pyspark

os.environ["SPARK_HOME"] = os.path.dirname(pyspark.__file__)  # wheel's bundled Spark
os.environ["PYSPARK_PYTHON"] = sys.executable                  # workers use the venv Python
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# CELL ********************


print(pyspark.__version__)          # 4.2.0
print(pyspark.__file__)             # should be inside venv312_ibmde\...\site-packages
print(os.environ.get("SPARK_HOME")) # None or a stale path means nothing is overriding the wheel
print(sys.executable)               # confirms the venv interpreter

# CELL ********************

# Import the needed Python modules
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from datetime import datetime

# CELL ********************

# Create a SparkSession
spark = SparkSession.builder \
    .appName("CreateDataFrameWithSchema") \
    .getOrCreate()

# CELL ********************

print(spark.version)
print(spark.sparkContext.master)        # local[*] = single-JVM mode
print(spark.sparkContext.defaultParallelism)

# CELL ********************


# Create a DataFrame for the customer table schema
customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True)
])


# Sample data for customers
customers_data = [
    (101, "Alice Johnson", "alice.j@example.com"),
    (102, "Bob Williams", "bob.w@example.com"),
    (103, "Charlie Brown", "charlie.b@example.com")
]


df_customers = spark.createDataFrame(customers_data, customer_schema)


# Write the empty DataFrame as a Delta table
df_customers.write.format("delta").mode("overwrite").save("Tables/dbo/customer_table")


# Create a DataFrame for the orders table schema
orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", TimestampType(), True),
    StructField("total_amount", DoubleType(), True)
])


# Sample data for orders
orders_data = [
    (1001, 101, datetime.strptime('2023-11-15 10:30:00', '%Y-%m-%d %H:%M:%S'), 150.75),
    (1002, 102, datetime.strptime('2023-11-15 11:00:00', '%Y-%m-%d %H:%M:%S'), 89.99),
    (1003, 101, datetime.strptime('2023-11-16 14:00:00', '%Y-%m-%d %H:%M:%S'), 45.50)
]


df_orders = spark.createDataFrame(orders_data, orders_schema)


# Write the empty DataFrame as a Delta table
df_orders.write.format("delta").mode("overwrite").save("Tables/dbo/orders_table")


print("Delta tables 'customer_table' and 'orders_table' created and populated with sample data successfully.")

# CELL ********************


# Import the needed Python modules
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from datetime import datetime


# Create a DataFrame for the customer table schema
customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True)
])


# Additional sample data for customers
customers_data = [
    (104, "James Terrance", "james.j@example.com"),
    (105, "McDonald SweetRush", "mcdonald.w@example.com"),
    (106, "Folu Matthew", "folu.m@example.com")
]
df_new_customers = spark.createDataFrame(customers_data, customer_schema)
df_new_customers.write.format("delta").mode("append").save("Tables/dbo/customer_table")


# Create a DataFrame for the orders table schema
orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", TimestampType(), True),
    StructField("total_amount", DoubleType(), True)
])


# Additional sample data for orders
from pyspark.sql.functions import to_timestamp
orders_data = [
    (1001, 104, datetime.strptime('2023-11-18 10:30:00', '%Y-%m-%d %H:%M:%S'), 185.75),
    (1002, 105, datetime.strptime('2023-11-19 11:00:00', '%Y-%m-%d %H:%M:%S'), 890.99),
    (1003, 106, datetime.strptime('2023-11-26 14:00:00', '%Y-%m-%d %H:%M:%S'), 457.50)
]
df_new_orders = spark.createDataFrame(orders_data, orders_schema)
df_new_orders = df_new_orders.withColumn("order_date", to_timestamp("order_date"))
df_new_orders.write.format("delta").mode("append").save("Tables/dbo/orders_table")


# Display the contents of the tables to verify
display(spark.sql("SELECT * FROM customer_table"))
display(spark.sql("SELECT * FROM orders_table"))


print("Additional sample data inserted into the customers and orders tables respectively")

# CELL ********************

# Correct an email address for customer_id 102
spark.sql("UPDATE customer_table SET email = 'bob.williams@example.com' WHERE customer_id = 102")


print("Customer email updated.")


display(spark.sql("SELECT * FROM customer_table WHERE customer_id = 102"))

# CELL ********************

spark.sql("ALTER TABLE customer_table ADD COLUMNS (membership_status STRING);")
spark.sql("UPDATE customer_table SET membership_status = 'Bronze' WHERE membership_status IS NULL;")
spark.sql("SELECT * FROM customer_table")

display(spark.sql("DESCRIBE HISTORY orders_table"))
display(spark.sql("SELECT * FROM orders_table VERSION AS OF 0"))
display(spark.sql("SELECT * FROM orders_table"))
display(spark.sql("SELECT * FROM orders_table VERSION AS OF 1"))

# CELL ********************

