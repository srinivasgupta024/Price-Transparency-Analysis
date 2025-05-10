# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.projhealthcare.dfs.core.windows.net",
    "da0/FIoxp43uIM+x3NLKrS17b6XBds5V+AwU/o+we5+7ohHOUxaUk39NxnyjeuzfM2nqrZTlt+M8+ASt7Cp21A=="
)

# COMMAND ----------

# Define storage details
storage_account_name = "projhealthcare"
container_name = "bronze"
file_path = "Scripps_Memorial_Hospital_Encinitas"

# Construct the full path
parquet_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}/"



# COMMAND ----------

# parquet_path = "abfss://san-francisco-oakland-berkeley@projhealthsa.dfs.core.windows.net/Kaiser/"
df = spark.read.parquet(parquet_path)
display(df)

# COMMAND ----------

from pyspark.sql.functions import col

# Filter rows where code_2_type is 'CPT Codes'
df_filtered = df.filter(col("code_2") == "10140")


# COMMAND ----------

display(df_filtered)

# COMMAND ----------

# List of columns to drop
columns_to_drop = ["code_1", "code_1_type", "code_2_type", "code_3", "code_3_type", "code_4", "code_4_type", "drug_unit_of_measurement", "drug_type_of_measurement","additional_generic_notes","modifiers"] 

# Drop the specified columns
df_filtered = df_filtered.drop(*columns_to_drop)

# Show the result
df_filtered.show()


# COMMAND ----------

# Rename 'code_2' to 'cpt_code'
df_renamed = df_filtered.withColumnRenamed("code_2", "cpt_code")

# Show the updated DataFrame
df_renamed.show()


# COMMAND ----------

output_path = "abfss://silver@projhealthcaresa.dfs.core.windows.net/Scripps_Memorial_Hospital_Encinitas"
df_renamed.write.mode("overwrite").parquet(output_path)

# COMMAND ----------

# # Get the number of columns in the DataFrame
# column_count = len(df_renamed.columns)

# # Display the column count
# print(f"Number of columns: {column_count}")


# COMMAND ----------

