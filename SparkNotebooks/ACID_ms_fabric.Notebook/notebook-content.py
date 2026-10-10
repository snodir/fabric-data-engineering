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

# Create a SparkSession
spark = SparkSession.builder \
    .appName("CreateDataFrameWithSchema") \
    .getOrCreate()

# CELL ********************

import inspect
print(display)
print(inspect.getmodule(display))

# CELL ********************

!pip list

# CELL ********************

# Create a sample DataFrame with book inventory
data = [
    (101, "The Great Gatsby", 10),
    (102, "To Kill a Mockingbird", 5),
    (103, "1984", 15)
]
columns = ["book_id", "title", "quantity"]

df = spark.createDataFrame(data, columns)

df.createOrReplaceTempView("inventory_tmpvw")
# Write the DataFrame to a Delta table in your Lakehouse
# df.write.format("delta").mode("overwrite").saveAsTable("inventory")

# Display the table to confirm it was created
df.show()
# display(spark.sql("SELECT * FROM inventory_tmpvw"))

# CELL ********************

from pyspark.sql.functions import col, when

# Perform a successful update (durability demo)
print("Performing a successful sale of 'To Kill a Mockingbird'...")

# Read the latest version of the table
#inventory_table = spark.table("inventory")

# Decrease the quantity for book_id 102
updated_df = (
inventory_table
.withColumn("quantity",
when(col("book_id") == 102, col("quantity") - 1)
.otherwise(col("quantity")))
)

# Write the change as a single overwrite to the same table (creates a new table version)
updated_df.write.format("delta").mode("overwrite").saveAsTable("inventory")

print("Update successful! Latest table state:")
display(spark.table("inventory"))


# CELL ********************

# Use Time Travel to query the original version of the table
print("Using Time Travel to see the table before the last sale...")

df_version_0 = spark.read.format("delta").option("versionAsOf", 0).table("inventory")

display(df_version_0)

# CELL ********************

# Show the ordered commit history for the table
history_df = spark.sql("DESCRIBE HISTORY inventory")
display(history_df)

# CELL ********************

# MAGIC %%sql
# MAGIC -- Accounts: current balances
# MAGIC CREATE TABLE IF NOT EXISTS accounts (
# MAGIC   account_id INT,
# MAGIC   balance DECIMAL(18,2)
# MAGIC );
# MAGIC 
# MAGIC -- Ledger: immutable debit/credit journal
# MAGIC CREATE TABLE IF NOT EXISTS ledger (
# MAGIC   entry_id BIGINT,
# MAGIC   ts TIMESTAMP,
# MAGIC   from_acct INT,
# MAGIC   to_acct INT,
# MAGIC   amount DECIMAL(18,2),   -- positive amount = transfer magnitude
# MAGIC   note STRING
# MAGIC );
# MAGIC 
# MAGIC -- Seed data (idempotent-ish: upsert or ensure only once in your lab)
# MAGIC DELETE FROM accounts;
# MAGIC INSERT INTO accounts (account_id, balance) VALUES
# MAGIC (1001, 1000.00),
# MAGIC (2002,  250.00);
# MAGIC 
# MAGIC -- Clean slate for ledger
# MAGIC DELETE FROM ledger;

# CELL ********************

# MAGIC %%sql
# MAGIC -- Build the transfer deltas as a derived set:
# MAGIC -- 1001: -200.00, 2002: +200.00
# MAGIC MERGE INTO accounts AS t
# MAGIC USING (
# MAGIC   SELECT 1001 AS account_id, CAST(-200.00 AS DECIMAL(18,2)) AS delta UNION ALL
# MAGIC   SELECT 2002,  CAST( 200.00 AS DECIMAL(18,2))
# MAGIC ) AS s
# MAGIC ON t.account_id = s.account_id
# MAGIC WHEN MATCHED THEN UPDATE SET t.balance = t.balance + s.delta;

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM accounts ORDER BY account_id;
# MAGIC 
# MAGIC -- Invariant check: sum of deltas across all affected accounts should be 0
# MAGIC -- (Here we re-check current pair)
# MAGIC SELECT SUM(CASE WHEN account_id IN (1001,2002) THEN balance END)
# MAGIC FROM accounts; -- not strictly zero; instead, check two-row delta sum if you logged pre-state
