# Databricks notebook source
# Define storage details
storage_account_name = "projhealthcaresa"
container_name = "bronze"
file_path = "Sharp_Memorial_Hospital"

# Construct the full path
parquet_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}/"



# COMMAND ----------

df = spark.read.parquet(parquet_path)
display(df)

# COMMAND ----------

from pyspark.sql.functions import col

# Filter rows where code_2_type is 'CPT Codes'
df_filtered = df.filter(col("code_3_type") == "CPT")


# COMMAND ----------

# List of columns to drop
columns_to_drop = ["code_1", "code_1_type", "code_2_type", "code_2", "code_3_type", "code_4", "code_4_type", "drug_unit_of_measurement", "drug_type_of_measurement","additional_generic_notes","modifiers"] 

# Drop the specified columns
df_filtered = df_filtered.drop(*columns_to_drop)

# Show the result
df_filtered.show()


# COMMAND ----------

# Rename 'code_2' to 'cpt_code'
df_renamed = df_filtered.withColumnRenamed("code_3", "cpt_code")

# Show the updated DataFrame
df_renamed.show()


# COMMAND ----------

display(df_renamed)

# COMMAND ----------

# Display the data types of each column
df_renamed.dtypes


# COMMAND ----------

output_path = "abfss://silver@projhealthcaresa.dfs.core.windows.net/Sharp_Memorial_Hospital"
df_renamed.write.mode("overwrite").parquet(output_path)

# COMMAND ----------

# # Get the number of columns in the DataFrame
# column_count = len(df_renamed.columns)

# # Display the column count
# print(f"Number of columns: {column_count}")


# COMMAND ----------

# Display the data types of each column
# df_renamed.dtypes


# COMMAND ----------

# df_renamed.columns

# COMMAND ----------

parquet_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/{file_path}/"
df = spark.read.parquet(parquet_path)
display(df)


# COMMAND ----------

len(df.columns)

# COMMAND ----------

