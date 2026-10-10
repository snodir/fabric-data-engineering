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

# Create a sample DataFrame with order data
data = [
    (101, "Product A", 5, "2023-10-01"),
    (102, "Product B", 2, "2023-10-01"),
    (103, "Product C", 1, "2023-10-02")
]
columns = ["order_id", "product_name", "quantity", "order_date"]

# Create a Spark DataFrame
df = spark.createDataFrame(data, columns)

# Save the DataFrame as a Delta table in your Lakehouse
df.write.format("delta").mode("overwrite").saveAsTable("orders")

print("The 'orders' table has been created successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO orders (order_id, product_name, quantity, order_date)
# MAGIC VALUES (104, 'Product D', 10, '2023-10-03');

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM orders;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC UPDATE orders
# MAGIC SET quantity = 3
# MAGIC WHERE order_id = 102;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql  
# MAGIC SELECT * FROM orders WHERE order_id = 102;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DELETE FROM orders
# MAGIC WHERE order_id = 103;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM orders;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC DESCRIBE HISTORY orders;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# View orders table as of version 2
df_v2 = spark.read.format("delta").option("versionAsOf", 2).table("orders")
df_v2.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC DESCRIBE HISTORY orders; 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC RESTORE TABLE orders TO VERSION AS OF 2;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
