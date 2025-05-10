# Databricks notebook source
# Define storage details
storage_account_name = "projhealthcaresa"
container_name = "raw-data"
file_path = "Sharp_Coronado_Hospital.csv"  # Change file extension to CSV

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

from pyspark.sql.types import *

# Step 1: Manually specify schema based on actual columns
schema = StructType([
    StructField("description", StringType(), True),
    StructField("code_1", StringType(), True),
    StructField("code_1_type", StringType(), True),
    StructField("code_2", StringType(), True),
    StructField("code_2_type", StringType(), True),
    StructField("code_3", StringType(), True),
    StructField("code_3_type", StringType(), True),
    StructField("modifiers", StringType(), True),
    StructField("setting", StringType(), True),
    StructField("drug_unit_of_measurement", StringType(), True),
    StructField("drug_type_of_measurement", StringType(), True),
    StructField("standard_charge_gross", DoubleType(), True),
    StructField("standard_charge_discounted_cash", DoubleType(), True),
    StructField("payer_name", StringType(), True),
    StructField("plan_name", StringType(), True),
    StructField("standard_charge_negotiated_dollar", DoubleType(), True),
    StructField("standard_charge_negotiated_percentage", DoubleType(), True),
    StructField("standard_charge_negotiated_algorithm", StringType(), True),
    StructField("estimated_amount", DoubleType(), True),
    StructField("standard_charge_methodology", StringType(), True),
    StructField("standard_charge_min", DoubleType(), True),
    StructField("standard_charge_max", DoubleType(), True),
])


df = spark.read \
    .option("header", "false") \
    .option("skipRows", 2) \
    .schema(schema) \
    .csv(csv_path)

df.show(truncate=False)


# COMMAND ----------

from pyspark.sql.functions import lit

df_with_meta = df \
    .withColumn("hospital_name", lit("Sharp Coronado Hospital")) \
    .withColumn("last_updated_on", lit("7/1/2024")) \
    .withColumn("hospital_location", lit("250 Prospect Place, Coronado, CA  92118"))

df_with_meta.show(truncate=False)


# COMMAND ----------

# Step 1: Define the desired front columns
front_cols = ["hospital_name", "last_updated_on", "hospital_location"]

# Step 2: Get the remaining columns (in current order, excluding the front)
remaining_cols = [col for col in df_with_meta.columns if col not in front_cols]

# Step 3: Reorder the DataFrame
df_reordered = df_with_meta.select(front_cols + remaining_cols)

# Step 4: Show the result
df_reordered.show(truncate=False)


# COMMAND ----------

display(df_reordered)

# COMMAND ----------

output_path = "abfss://bronze@projhealthcaresa.dfs.core.windows.net/Sharp_Coronado_Hospital/"
df_reordered.write.mode("overwrite").parquet(output_path)

# COMMAND ----------

