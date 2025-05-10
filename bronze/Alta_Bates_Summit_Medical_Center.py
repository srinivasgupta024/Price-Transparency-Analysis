# Databricks notebook source
# Define storage details
storage_account_name = "projhealthcaresa"
container_name = "bronze"
file_path = "Alta_Bates_Summit_Medical_Center"

# Construct the full path
parquet_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}/"



# COMMAND ----------

# parquet_path = "abfss://san-francisco-oakland-berkeley@projhealthsa.dfs.core.windows.net/Kaiser/"
df = spark.read.parquet(parquet_path)
display(df)

# COMMAND ----------

