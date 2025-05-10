# Databricks notebook source
# HARD CODE YOUR STORAGE KEY HERE (be very careful!)
spark.conf.set(
    "fs.azure.account.key.projhealthcare.dfs.core.windows.net",
    "da0/FIoxp43uIM+x3NLKrS17b6XBds5V+AwU/o+we5+7ohHOUxaUk39NxnyjeuzfM2nqrZTlt+M8+ASt7Cp21A=="
)


# COMMAND ----------

# Define storage details
storage_account_name = "projhealthcare"
container_name = "bronze"
file_path = "John_Muir_Health_Walnut_Creek_Medical_Center"

# Construct the full path
parquet_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}/"
df = spark.read.parquet(parquet_path)
display(df)