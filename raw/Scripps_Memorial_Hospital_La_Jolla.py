# Databricks notebook source
# Define storage details
storage_account_name = "projhealthcaresa"
container_name = "raw-data"
file_path = "Scripps_Memorial_Hospital_La_Jolla.csv"  # Change file extension to CSV

# Construct the full path
csv_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}"


# COMMAND ----------

# Read the CSV file with header
df = spark.read.option("header", "true").csv(csv_path)

# Convert to RDD and drop the first two rows
df_rdd = df.rdd.zipWithIndex().filter(lambda x: x[1] > 1).map(lambda x: x[0])

# Convert back to DataFrame using the original schema
df_cleaned = spark.createDataFrame(df_rdd, df.schema)

# Show the cleaned DataFrame
df_cleaned.show()


# COMMAND ----------

display(df_cleaned)

# COMMAND ----------

# Read CSV as RDD
rdd = spark.read.text(csv_path).rdd

# Take the first two lines
first_two_lines = rdd.take(5)

# Print the first two lines
for line in first_two_lines:
    print(line[0])  # Extracting the text content


# COMMAND ----------

df_cleaned.columns

# COMMAND ----------

correct_columns = [
    "description", "code_1", "code_1_type", "code_2", "code_2_type", 
    "code_3", "code_3_type", "code_4", "code_4_type", "setting", 
    "drug_unit_of_measurement", "drug_type_of_measurement", "standard_charge_gross", 
    "standard_charge_discounted_cash", "payer_name", "plan_name", "modifiers", 
    "standard_charge_negotiated_dollar", "standard_charge_negotiated_percentage", 
    "standard_charge_negotiated_algorithm", "estimated_amount", 
    "standard_charge_methodology", "standard_charge_min", "standard_charge_max", 
    "additional_generic_notes"
]

# Rename columns in the existing DataFrame
df_1 = df_cleaned.toDF(*correct_columns)

# Show the updated DataFrame
df_1.printSchema()
df_1.show(5)


# COMMAND ----------

display(df_1)

# COMMAND ----------

from pyspark.sql import functions as F

# Define the new values
hospital_name_value = "Scripps Memorial Hospital La Jolla"
last_updated_on_value = "2024-12-03"
hospital_location_value = "9888 Genesee Avenue, La Jolla, CA, 92037-1205"

# Add the new columns to the DataFrame
df_final = df_1.withColumn("hospital_name", F.lit(hospital_name_value)) \
       .withColumn("last_updated_on", F.lit(last_updated_on_value)) \
       .withColumn("hospital_location", F.lit(hospital_location_value))

# Reorder columns to place the new ones at the beginning
cols = ["hospital_name", "last_updated_on", "hospital_location"] + [col for col in df_1.columns if col not in ["hospital_name", "last_updated_on", "hospital_location"]]
df_final = df_final.select(cols)



# COMMAND ----------

display(df_final)

# COMMAND ----------

output_path = "abfss://bronze@projhealthcaresa.dfs.core.windows.net/Scripps_Memorial_Hospital_La_Jolla"


# COMMAND ----------

df_final.write.mode("overwrite").parquet(output_path)

# COMMAND ----------

# parquet_path = "abfss://bronze@projhealthcaresa.dfs.core.windows.net/Scripps_Memorial_Hospital_La_Jolla/"
# df = spark.read.parquet(parquet_path)
# display(df)

# COMMAND ----------

# df.columns

# COMMAND ----------

