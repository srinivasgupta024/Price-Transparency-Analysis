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
file_path = "Kaiser_Foundation_Hospital_SouthSanFrancisco.csv"  # Change file extension to CSV

# Construct the full path
csv_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}"


# COMMAND ----------

from pyspark.sql.types import StructType, StructField

# Read the raw CSV file with no schema and handle quoted fields with commas
df_raw = spark.read.option("header", "false") \
                   .option("quote", "\"") \
                   .option("escape", "\"") \
                   .option("multiLine", "true") \
                   .option("inferSchema", "true") \
                   .csv(csv_path)

# Extract the third row as the header
new_header = df_raw.collect()[2]  # Row index 2 (third row)

# Convert the third row into a schema
schema = StructType([StructField(str(col), df_raw.schema[i].dataType, True) for i, col in enumerate(new_header)])

# Read the CSV file again with the defined schema, skipping the first three rows
df = spark.read.option("header", "false") \
              .schema(schema) \
              .option("quote", "\"") \
              .option("escape", "\"") \
              .option("multiLine", "true") \
              .csv(csv_path) \
              .rdd.zipWithIndex() \
              .filter(lambda x: x[1] > 2) \
              .map(lambda x: x[0]) \
              .toDF(schema)

# Show the first few rows of the dataframe
df.show(4, truncate=False)


# COMMAND ----------

display(df_raw)

# COMMAND ----------

from pyspark.sql.functions import lit

# Add new columns with constant values
df = (df.withColumn("hospital_name", lit("Kaiser Foundation Hospital - South San Francisco"))
        .withColumn("last_updated_on", lit("2025-02-01"))
        .withColumn("hospital_location", lit("1200 El Camino Real,South San Francisco,CA,94080")))

# Show the updated DataFrame
df.show(4, truncate=False)


# COMMAND ----------

column_rename_map = {
    "description": "description",
    "code|1": "code_1",
    "code|1|type": "code_1_type",
    "code|2": "code_2",
    "code|2|type": "code_2_type",
    "setting": "setting",
    "drug_unit_of_measurement": "drug_unit",
    "drug_type_of_measurement": "drug_type",
    "standard_charge|gross": "gross_charge",
    "standard_charge|discounted_cash": "discounted_cash_charge",
    "modifiers": "modifiers",
    "standard_charge|[KAISER FOUNDATION HEALTH PLAN, INC.]|[COMMERCIAL]|negotiated_dollar": "kaiser_commercial_negotiated_dollar",
    "standard_charge|[KAISER FOUNDATION HEALTH PLAN, INC.]|[COMMERCIAL]|negotiated_percentage": "kaiser_commercial_negotiated_percentage",
    "standard_charge|[KAISER FOUNDATION HEALTH PLAN, INC.]|[COMMERCIAL]|negotiated_algorithm": "kaiser_commercial_negotiated_algorithm",
    "estimated_amount|[KAISER FOUNDATION HEALTH PLAN, INC.]|[COMMERCIAL]": "kaiser_commercial_estimated_amount",
    "standard_charge|[KAISER FOUNDATION HEALTH PLAN, INC.]|[COMMERCIAL]|methodology": "kaiser_commercial_methodology",
    "additional_payer_notes|[KAISER FOUNDATION HEALTH PLAN, INC.]|[COMMERCIAL]": "kaiser_commercial_payer_notes",
    "standard_charge|[KAISER FOUNDATION HEALTH PLAN, INC.]|[MEDICARE]|negotiated_dollar": "kaiser_medicare_negotiated_dollar",
    "standard_charge|[KAISER FOUNDATION HEALTH PLAN, INC.]|[MEDICARE]|negotiated_percentage": "kaiser_medicare_negotiated_percentage",
    "standard_charge|[KAISER FOUNDATION HEALTH PLAN, INC.]|[MEDICARE]|negotiated_algorithm": "kaiser_medicare_negotiated_algorithm",
    "estimated_amount|[KAISER FOUNDATION HEALTH PLAN, INC.]|[MEDICARE]": "kaiser_medicare_estimated_amount",
    "standard_charge|[KAISER FOUNDATION HEALTH PLAN, INC.]|[MEDICARE]|methodology": "kaiser_medicare_methodology",
    "additional_payer_notes|[KAISER FOUNDATION HEALTH PLAN, INC.]|[MEDICARE]": "kaiser_medicare_payer_notes",
    "standard_charge|[KAISER FOUNDATION HEALTH PLAN, INC.]|[MEDICAID]|negotiated_dollar": "kaiser_medicaid_negotiated_dollar",
    "standard_charge|[KAISER FOUNDATION HEALTH PLAN, INC.]|[MEDICAID]|negotiated_percentage": "kaiser_medicaid_negotiated_percentage",
    "standard_charge|[KAISER FOUNDATION HEALTH PLAN, INC.]|[MEDICAID]|negotiated_algorithm": "kaiser_medicaid_negotiated_algorithm",
    "estimated_amount|[KAISER FOUNDATION HEALTH PLAN, INC.]|[MEDICAID]": "kaiser_medicaid_estimated_amount",
    "standard_charge|[KAISER FOUNDATION HEALTH PLAN, INC.]|[MEDICAID]|methodology": "kaiser_medicaid_methodology",
    "additional_payer_notes|[KAISER FOUNDATION HEALTH PLAN, INC.]|[MEDICAID]": "kaiser_medicaid_payer_notes",
    "standard_charge|min": "standard_charge_min",
    "standard_charge|max": "standard_charge_max",
    "additional_generic_notes": "generic_notes",
    "hospital_name": "hospital_name",
    "last_updated_on": "last_updated",
    "hospital_location": "hospital_location"
}

from pyspark.sql.functions import col

df = df.select([col(f"`{c}`").alias(column_rename_map.get(c, c)) for c in df.columns])



# COMMAND ----------

from pyspark.sql.functions import col

# Define the new column order
cols_to_move_first = ["hospital_name", "hospital_location", "last_updated"]
remaining_cols = [col for col in df.columns if col not in cols_to_move_first]

# Rearrange the DataFrame
df = df.select([col(c) for c in cols_to_move_first + remaining_cols])


# COMMAND ----------

output_path = "abfss://bronze@projhealthcare.dfs.core.windows.net/Kaiser_Foundation_Hospital_SouthSanFrancisco"
df.write.mode("overwrite").parquet(output_path)


# COMMAND ----------

parquet_path = "abfss://bronze@projhealthcare.dfs.core.windows.net/Kaiser_Foundation_Hospital_SouthSanFrancisco/"
df = spark.read.parquet(parquet_path)
display(df)

# COMMAND ----------

