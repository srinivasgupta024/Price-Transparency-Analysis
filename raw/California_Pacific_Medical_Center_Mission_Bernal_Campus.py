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
file_path = "940382780_chinese-hospital_standardcharges (1).csv"  # Change file extension to CSV

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

# COMMAND ----------

display(df_raw)

# COMMAND ----------

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

display(df)

# COMMAND ----------

# assume df is your DataFrame
distinct_vals = df.select("code|3|type").distinct()
distinct_vals.show()


# COMMAND ----------

df.columns

# COMMAND ----------

['hospital_name',
 'last_updated_on',
 'hospital_location',
 'description',
 'code_1',
 'code_1_type',
 'code_2',
 'code_2_type',
 'code_3',
 'code_3_type',
 'code_4',
 'code_4_type',
 'setting',
 'drug_unit_of_measurement',
 'drug_type_of_measurement',
 'standard_charge_gross',
 'standard_charge_discounted_cash',
 'payer_name',
 'plan_name',
 'modifiers',
 'standard_charge_negotiated_dollar',
 'standard_charge_negotiated_percentage',
 'standard_charge_negotiated_algorithm',
 'estimated_amount',
 'standard_charge_methodology',
 'standard_charge_min',
 'standard_charge_max',
 'additional_generic_notes']


# COMMAND ----------

from pyspark.sql.functions import col, lit
from pyspark.sql.types import DoubleType

# 1️⃣ Add hospital info + null min/max
df2 = (df
    .withColumn("hospital_name",        lit("Contra Costa County"))
    .withColumn("last_updated_on",      lit("6/25/2024"))
    .withColumn("hospital_location",    lit("2500 Alhambra Ave, Martinez, CA 94553"))
    .withColumn("standard_charge_min",  lit(None).cast(DoubleType()))
    .withColumn("standard_charge_max",  lit(None).cast(DoubleType()))
)

# 2️⃣ Rename any remaining "|" → "_"
#    (covering all your pipe-delimited names)
rename_map = {
    "code|1": "code_1",
    "code|1|type": "code_1_type",
    "code|2": "code_2",
    "code|2|type": "code_2_type",
    "code|3": "code_3",
    "code|3|type": "code_3_type",
    "code|4": "code_4",
    "code|4|type": "code_4_type",
    "standard_charge|gross":            "standard_charge_gross",
    "standard_charge|discounted_cash":   "standard_charge_discounted_cash",
    "standard_charge|negotiated_dollar": "standard_charge_negotiated_dollar",
    "standard_charge|negotiated_percentage": "standard_charge_negotiated_percentage",
    "standard_charge|negotiated_algorithm":  "standard_charge_negotiated_algorithm",
    "standard_charge|methodology":       "standard_charge_methodology"
}

# apply the renames in one go
renamed_df = df2.select(*[
    col(c).alias(rename_map[c]) if c in rename_map else col(c)
    for c in df2.columns
])

# 3️⃣ Reorder into the final 28‑column list
final_columns = [
    "hospital_name",
    "last_updated_on",
    "hospital_location",
    "description",
    "code_1",
    "code_1_type",
    "code_2",
    "code_2_type",
    "code_3",
    "code_3_type",
    "code_4",
    "code_4_type",
    "setting",
    "drug_unit_of_measurement",
    "drug_type_of_measurement",
    "standard_charge_gross",
    "standard_charge_discounted_cash",
    "payer_name",
    "plan_name",
    "modifiers",
    "standard_charge_negotiated_dollar",
    "standard_charge_negotiated_percentage",
    "standard_charge_negotiated_algorithm",
    "estimated_amount",
    "standard_charge_methodology",
    "standard_charge_min",
    "standard_charge_max",
    "additional_generic_notes"
]

final_df = renamed_df.select(*final_columns)


# COMMAND ----------

display(final_df)

# COMMAND ----------

output_path = "abfss://bronze@projhealthcaresa.dfs.core.windows.net/Contra_Costa_Regional_Medical_Center"
final_df.write.mode("overwrite").parquet(output_path)

# COMMAND ----------

# parquet_path = "abfss://bronze@projhealthcaresa.dfs.core.windows.net/Contra_Costa_Regional_Medical_Center/"
# df = spark.read.parquet(parquet_path)
# display(df)

# COMMAND ----------

