# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.projhealthcare.dfs.core.windows.net",
    "da0/FIoxp43uIM+x3NLKrS17b6XBds5V+AwU/o+we5+7ohHOUxaUk39NxnyjeuzfM2nqrZTlt+M8+ASt7Cp21A=="
)

# COMMAND ----------

# Define storage details
storage_account_name = "projhealthcare"
container_name = "bronze"
file_path = "Kaiser_Foundation_Hospital_Antioch"

# Construct the full path
parquet_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}/"
df = spark.read.parquet(parquet_path)
display(df)


# COMMAND ----------

from pyspark.sql.functions import col
df_filtered = df.filter(col("code_1_type") == "CPT")


# COMMAND ----------

# List of columns to drop based on the columns you don't want
columns_to_drop = [
    "code_1_type", "code_2_type", "code_2", "drug_unit", "drug_type", 
    "generic_notes", "modifiers","generi_notes","kaiser_medicaid_payer_notes","kaiser_medicare_payer_notes","kaiser_commercial_payer_notes"
]

# Drop the specified columns
df_filtered = df_filtered.drop(*columns_to_drop)

# COMMAND ----------

display(df_filtered)

# COMMAND ----------

# Get the number of columns in the DataFrame
column_count = len(df_filtered.columns)

# Display the column count
print(f"Number of columns: {column_count}")

# COMMAND ----------

output_path = f"abfss://silver@projhealthcare.dfs.core.windows.net/{file_path}/"
df_filtered.write.mode("overwrite").parquet(output_path)

# COMMAND ----------

from pyspark.sql.functions import lit, col

# Define storage details
# storage_account_name = "projhealthcare"
container_name = "silver"
# file_path = "Kaiser_Foundation_Hospital_Walnut_Creek"

# Construct the full path
parquet_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}/"

# Read the Parquet file
df = spark.read.parquet(parquet_path)

# Columns to drop
drop_cols = [
    'kaiser_medicare_negotiated_dollar',
    'kaiser_medicare_negotiated_percentage',
    'kaiser_medicare_negotiated_algorithm',
    'kaiser_medicare_estimated_amount',
    'kaiser_medicare_methodology',
    'kaiser_medicaid_negotiated_dollar',
    'kaiser_medicaid_negotiated_percentage',
    'kaiser_medicaid_negotiated_algorithm',
    'kaiser_medicaid_estimated_amount',
    'kaiser_medicaid_methodology'
]

# Drop unnecessary columns
df_cleaned = df.drop(*drop_cols)

# Add payer and plan columns
df_cleaned = df_cleaned.withColumn("payer_name", lit("All Payers")) \
                       .withColumn("plan_name", lit("All Plans"))

# Rename columns to match target schema
df_renamed = df_cleaned \
    .withColumnRenamed("last_updated", "last_updated_on") \
    .withColumnRenamed("code_1", "cpt_code") \
    .withColumnRenamed("gross_charge", "standard_charge_gross") \
    .withColumnRenamed("discounted_cash_charge", "standard_charge_discounted_cash") \
    .withColumnRenamed("kaiser_commercial_negotiated_dollar", "standard_charge_negotiated_dollar") \
    .withColumnRenamed("kaiser_commercial_negotiated_percentage", "standard_charge_negotiated_percentage") \
    .withColumnRenamed("kaiser_commercial_negotiated_algorithm", "standard_charge_negotiated_algorithm") \
    .withColumnRenamed("kaiser_commercial_estimated_amount", "estimated_amount") \
    .withColumnRenamed("kaiser_commercial_methodology", "standard_charge_methodology")

# Display final result
display(df_renamed)


# COMMAND ----------

df_renamed.columns

# COMMAND ----------

output_path = f"abfss://silver@projhealthcare.dfs.core.windows.net/{file_path}/"
df_renamed.write.mode("overwrite").parquet(output_path)

# COMMAND ----------

