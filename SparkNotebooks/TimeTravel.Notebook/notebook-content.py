# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "289a7fd4-0b4f-40a9-8f81-8e7d499915fa",
# META       "default_lakehouse_name": "myLakehouse",
# META       "default_lakehouse_workspace_id": "4496235d-2e5d-4488-87c9-7a0c3b7d0ef7",
# META       "known_lakehouses": [
# META         {
# META           "id": "289a7fd4-0b4f-40a9-8f81-8e7d499915fa"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Create a sample DataFrame
data = [(101, "Standard", 5.99),
(102, "Express", 15.99),
(103, "Standard", 5.99),
(104, "Standard", 5.99),
(105, "Overnight", 25.99)
]
columns = ["OrderID", "ShippingMethod", "ShippingCost"]
df = spark.createDataFrame(data, columns)
# Write the DataFrame to a Delta table named 'orders'
df.write.mode("overwrite").format("delta").saveAsTable("orders")
print("Table 'orders' created successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC UPDATE orders
# MAGIC SET ShippingMethod = 'Express', ShippingCost = 15.99
# MAGIC WHERE ShippingMethod = 'Standard';
# MAGIC Verify that the error occurred. Run a SELECT query again:
# MAGIC 
# MAGIC SELECT * FROM orders;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Compare Version 0 (before error) with Version 1 (after error)
df_v0 = spark.read.format("delta").option("versionAsOf", 0).table("orders")
df_v1 = spark.read.format("delta").option("versionAsOf", 1).table("orders")
print("Version 0 - Correct Data:")
df_v0.show()
print("Version 1 - Erroneous Data:")
df_v1.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Simulate an accidental DELETE (optional scenario)
spark.sql("DELETE FROM orders WHERE ShippingMethod = 'Overnight'")
# Validate the missing row
print("After DELETE (rows missing):")
spark.sql("SELECT * FROM orders").show()
# Recover deleted row using Time Travel (Version 1 was before DELETE)
df_v1 = spark.read.format("delta").option("versionAsOf", 1).table("orders")
print("Recovered Data from Version 1:")
df_v1.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Restore data from Version 0 into a new table
df_restored = spark.read.format("delta").option("versionAsOf", 0).table("orders")
df_restored.write.format("delta").mode("overwrite").saveAsTable("orders_restored")
print("Restored table created as 'orders_restored'")
spark.sql("SELECT * FROM orders_restored").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Restore using timestamp (replace with actual timestamp from DESCRIBE HISTORY)
df_time = spark.read.format("delta").option("timestampAsOf", "2025-10-01T12:00:00.000Z").table("orders")
df_time.write.format("delta").mode("overwrite").saveAsTable("orders_restored_timestamp")
print("Restored table created using timestamp-based recovery")
spark.sql("SELECT * FROM orders_restored_timestamp").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load the table as it existed before the DELETE (for example, version 2)
df_before_delete = spark.read.format("delta").option("versionAsOf", 2).table("orders")
# Display the data before the DELETE occurred
df_before_delete.show()
# Restore the correct version to a new table
df_before_delete.write.format("delta").mode("overwrite").saveAsTable("orders_restored")
print("Restored table created successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load the table as it existed before a specific timestamp
df_recovered = spark.read.format("delta") \
.option("timestampAsOf", "2025-10-04T14:59:00Z") \
.table("orders")
# Display the data from that timestamp
df_recovered.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
