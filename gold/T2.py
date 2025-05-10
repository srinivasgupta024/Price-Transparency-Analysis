# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.projhealthcare.dfs.core.windows.net",
    "da0/FIoxp43uIM+x3NLKrS17b6XBds5V+AwU/o+we5+7ohHOUxaUk39NxnyjeuzfM2nqrZTlt+M8+ASt7Cp21A=="
)

# COMMAND ----------

# Define storage details
storage_account_name = "projhealthcare"
container_name = "gold"
file_path = "dim_hospital"

# Construct the full path
parquet_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}/"

df = spark.read.format("delta").load(parquet_path)
display(df)


# COMMAND ----------

from pyspark.sql.functions import to_date, col, coalesce, date_format

df = df.withColumn(
    "last_updated_on",
    date_format(
        coalesce(
            to_date(col("last_updated_on"), "MM/dd/yyyy"),
            to_date(col("last_updated_on"), "M/d/yyyy"),        # add this!
            to_date(col("last_updated_on"), "MM-dd-yyyy"),
            to_date(col("last_updated_on"), "yyyy-MM-dd")
        ),
        "yyyy-MM-dd"
    )
)



# COMMAND ----------

display(df)

# COMMAND ----------

output_path = "abfss://gold@projhealthcare.dfs.core.windows.net/dim_hospital"
df.write.mode("overwrite").format("delta").save(output_path)

# COMMAND ----------

