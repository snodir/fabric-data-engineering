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

# Create the initial data
sales_data = [(1, "2023-10-01", 101, 2, 25.50),
(2, "2023-10-01", 102, 1, 50.00),
(3, "2023-10-02", 101, 1, 12.75)
]
columns = ["Transaction_ID", "Date", "Customer_ID", "Quantity", "Total_Amount"]
# Create a Spark DataFrame
df = spark.createDataFrame(sales_data, columns)
# Write the DataFrame to a Delta table in your Lakehouse
df.write.format("delta").mode("overwrite").save("Tables/dbo/Sales_Transactions")
print("Successfully created the Sales_Transactions table.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# New data with an extra column 'Promotion_Code'
new_sales_data = [
(4, "2023-10-03", 103, 3, 75.00, "FALL20"),
(5, "2023-10-03", 102, 1, 45.00, "SAVE10")
]
new_columns = ["Transaction_ID", "Date", "Customer_ID", "Quantity", "Total_Amount", "Promotion_Code"]
# Create a new DataFrame
new_df = spark.createDataFrame(new_sales_data, new_columns)
# Append the new data and allow the schema to merge
new_df.write.format("delta") \
.mode("append") \
.option("mergeSchema", "true") \
.save("Tables/Sales_Transactions")
print("Successfully appended new data with schema evolution.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM Sales_Transactions

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read the existing table
df = spark.read.format("delta").load("Tables/Sales_Transactions")
# Update the schema: cast Total_Amount to DECIMAL(10,2)
from pyspark.sql.functions import col
df_updated = df.withColumn("Total_Amount", col("Total_Amount").cast("decimal(10,2)"))
# Overwrite the table with the updated schema
df_updated.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("Tables/Sales_Transactions")
print("Schema updated: Total_Amount column now set to DECIMAL(10,2).")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check schema
df_updated.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE Sales_Transactions;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
