# Databricks notebook source
# HARD CODE YOUR STORAGE KEY HERE (be very careful!)
spark.conf.set(
    "fs.azure.account.key.projhealthcare.dfs.core.windows.net",
    "da0/FIoxp43uIM+x3NLKrS17b6XBds5V+AwU/o+we5+7ohHOUxaUk39NxnyjeuzfM2nqrZTlt+M8+ASt7Cp21A=="
)


# COMMAND ----------

# Define storage details
storage_account_name = "projhealthcare"
container_name = "raw"
file_path = "John-Muir-Health-Concord-Medical-Center_standardcharges.csv"

# Construct the full path
parquet_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}/"



# COMMAND ----------

# Read CSV and skip first 2 rows
df = spark.read.option("header", "true") \
               .option("inferSchema", "true") \
               .option("skipRows", 2) \
               .csv(parquet_path)

# Display first 10 rows
display(df.limit(10))


# COMMAND ----------

df.columns

# COMMAND ----------

from pyspark.sql.functions import col

df_filtered = df.filter(col("code|2|type") == "CPT")


# COMMAND ----------

# List of columns to drop
cols_to_drop = [
    'code|1', 'code|1|type', 'code|2|type',
    'code|3', 'code|3|type', 'code|4', 'code|4|type',
    'drug_unit_of_measurement', 'drug_type_of_measurement',
    'modifiers',
    'additional_generic_notes', 'additional_generic_notes_with_LPP'
]

# Drop them from the DataFrame
df_cleaned = df_filtered.drop(*cols_to_drop)

# Display the result
display(df_cleaned)


# COMMAND ----------

output_path = "abfss://bronze@projhealthcare.dfs.core.windows.net/John_Muir_Medical_Center_Concord_Campus"
df_cleaned.write.mode("overwrite").parquet(output_path)

# COMMAND ----------



# COMMAND ----------

# Define storage details
storage_account_name = "projhealthcare"
container_name = "bronze"
file_path = "John_Muir_Medical_Center_Concord_Campus"

# Construct the full path
parquet_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}/"
df = spark.read.parquet(parquet_path)
display(df)

# COMMAND ----------

filtered_df = df.filter(df["plan_name"] == "MC BEN SCAN 101 [3649]")


# COMMAND ----------

display(filtered_df)

# COMMAND ----------

from pyspark.sql.functions import lit, col

# Add static columns
df_with_meta = df.withColumn("hospital_name", lit("John Muir Medical Center-Concord Campus")) \
                         .withColumn("hospital_location", lit("2540 East Street, Concord, CA 94520")) \
                         .withColumn("last_updated_on", lit("02/07/2025"))

# Rename columns
df_renamed = df_with_meta \
    .withColumnRenamed("description", "description") \
    .withColumnRenamed("code|2", "cpt_code") \
    .withColumnRenamed("setting", "setting") \
    .withColumnRenamed("standard_charge|gross", "standard_charge_gross") \
    .withColumnRenamed("standard_charge|discounted_cash", "standard_charge_discounted_cash") \
    .withColumnRenamed("standard_charge|negotiated_dollar", "standard_charge_negotiated_dollar") \
    .withColumnRenamed("standard_charge|negotiated_percentage", "standard_charge_negotiated_percentage") \
    .withColumnRenamed("standard_charge|negotiated_algorithm", "standard_charge_negotiated_algorithm") \
    .withColumnRenamed("estimated_amount", "estimated_amount") \
    .withColumnRenamed("standard_charge|methodology", "standard_charge_methodology") \
    .withColumnRenamed("standard_charge|min", "standard_charge_min") \
    .withColumnRenamed("standard_charge|max", "standard_charge_max") \
    .withColumnRenamed("payer_name", "payer_name") \
    .withColumnRenamed("plan_name", "plan_name")

# Reorder columns if needed (optional)
final_cols = [
    'hospital_name', 'hospital_location', 'last_updated_on', 'description', 'cpt_code', 'setting', 'payer_name', 'plan_name',
    'standard_charge_gross', 'standard_charge_discounted_cash',
    'standard_charge_negotiated_dollar', 'standard_charge_negotiated_percentage',
    'standard_charge_negotiated_algorithm', 'estimated_amount', 'standard_charge_methodology',
    'standard_charge_min', 'standard_charge_max'
]
df_final = df_renamed.select(*final_cols)

# Display the final DataFrame
display(df_final)


# COMMAND ----------

output_path = "abfss://silver@projhealthcare.dfs.core.windows.net/John_Muir_Medical_Center_Concord_Campus"
df_final.write.mode("overwrite").parquet(output_path)

# COMMAND ----------

 

# COMMAND ----------

# df = spark.read.option("header", "true").option("inferSchema", "true").csv(parquet_path)

# # Take only the first 2 rows
# df_first_two = df.limit(2)

# # Display
# display(df_first_two)


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

