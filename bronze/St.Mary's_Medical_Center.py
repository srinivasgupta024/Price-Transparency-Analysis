# Databricks notebook source
# HARD CODE YOUR STORAGE KEY HERE (be very careful!)
spark.conf.set(
    "fs.azure.account.key.projhealthcare.dfs.core.windows.net",
    "da0/FIoxp43uIM+x3NLKrS17b6XBds5V+AwU/o+we5+7ohHOUxaUk39NxnyjeuzfM2nqrZTlt+M8+ASt7Cp21A=="
)


# COMMAND ----------

# Define storage details
storage_account_name = "projhealthcare"
container_name = "demo"
file_path = "St.Mary's_Medical_Center"

# Construct the full path
parquet_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}/"
df = spark.read.parquet(parquet_path)
display(df)

# COMMAND ----------

df.columns

# COMMAND ----------

# ['hospital_name',
#  'hospital_location',
#  'last_updated_on',
#  'description',
#  'cpt_code',
#  'setting',
#  'payer_name',
#  'plan_name',
#  'standard_charge_gross',
#  'standard_charge_discounted_cash',
#  'standard_charge_negotiated_dollar',
#  'standard_charge_negotiated_percentage',
#  'standard_charge_negotiated_algorithm',
#  'estimated_amount',
#  'standard_charge_methodology',
#  'standard_charge_min',
#  'standard_charge_max']

# COMMAND ----------

from pyspark.sql.functions import lit, col

# Add static columns
df_with_meta = df.withColumn("hospital_name", lit("St. Mary's Medical Center - San Francisco")) \
                         .withColumn("hospital_location", lit("450 Stanyan St, San Francisco, CA 94117")) \
                         .withColumn("last_updated_on", lit("11-25-2024")) \
                         .withColumn("estimated_amount", lit(0)) \
                         .withColumn("standard_charge_methodology", lit("NA")) \
                         .withColumn("standard_charge_discounted_cash", lit(0)) 
                             


# COMMAND ----------

display(df_with_meta)   

# COMMAND ----------

df_with_meta.columns

# COMMAND ----------

from pyspark.sql.functions import col

# Drop 'Code Type' column
df = df_with_meta.drop("Code Type")

# Rename columns
df_renamed = df.select(
    col("hospital_name"),
    col("hospital_location"),
    col("last_updated_on"),
    col("Description").alias("description"),
    col("Code").alias("cpt_code"),
    col("Setting").alias("setting"),
    col("Payer Name").alias("payer_name"),
    col("Plan Name").alias("plan_name"),
    col("Gross Charge").alias("standard_charge_gross"),
    col("standard_charge_discounted_cash").alias("standard_charge_discounted_cash"),
    col("Standard Charge").alias("standard_charge_negotiated_dollar"),
    col("Charge %").alias("standard_charge_negotiated_percentage"),
    col("Methodology").alias("standard_charge_negotiated_algorithm"),
    col("estimated_amount").alias("estimated_amount"),
    col("standard_charge_methodology").alias("standard_charge_methodology"),
    col("Min Charge").alias("standard_charge_min"),
    col("Max Charge").alias("standard_charge_max")
)


# COMMAND ----------

display(df_renamed)

# COMMAND ----------

output_path = "abfss://silver@projhealthcare.dfs.core.windows.net/St.Mary's_Medical_Center"
df_renamed.write.mode("overwrite").parquet(output_path)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



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

output_path = "abfss://bronze@projhealthcare.dfs.core.windows.net/John_Muir_Health_Walnut_Creek_Medical_Center"
df_cleaned.write.mode("overwrite").parquet(output_path)

# COMMAND ----------

df = spark.read.option("header", "true").option("inferSchema", "true").csv(parquet_path)

# Take only the first 2 rows
df_first_two = df.limit(2)

# Display
display(df_first_two)


# COMMAND ----------



# COMMAND ----------

# Define storage details
storage_account_name = "projhealthcare"
container_name = "bronze"
file_path = "John_Muir_Health_Walnut_Creek_Medical_Center"

# Construct the full path
parquet_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}/"
df = spark.read.parquet(parquet_path)
display(df)

# COMMAND ----------

from pyspark.sql.functions import lit, col

# Add static columns
df_with_meta = df.withColumn("hospital_name", lit("John Muir Medical Center-Walnut Creek Campus")) \
                         .withColumn("hospital_location", lit("1601 Ygnacio Valley Road, Walnut Creek, CA 94598")) \
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

output_path = "abfss://silver@projhealthcare.dfs.core.windows.net/John_Muir_Health_Walnut_Creek_Medical_Center"
df.write.mode("overwrite").parquet(output_path)

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

