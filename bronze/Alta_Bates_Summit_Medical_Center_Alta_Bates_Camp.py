# Databricks notebook source
# Define storage details
storage_account_name = "projhealthcaresa"
container_name = "bronze"
file_path = "Alta_Bates_Summit_Medical_Center_Alta_Bates_Camp"

# Construct the full path
parquet_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}/"



# COMMAND ----------

# parquet_path = "abfss://san-francisco-oakland-berkeley@projhealthsa.dfs.core.windows.net/Kaiser/"
df = spark.read.parquet(parquet_path)
display(df)

# COMMAND ----------

# Select the columns of interest
distinct_values_df = df.select("code_4_type").distinct()

# Show the distinct values
distinct_values_df.show()
  

# COMMAND ----------

