# Databricks notebook source
# Define storage details
storage_account_name = "projhealthcaresa"
container_name = "raw-data"
file_path = "AHMC_Seton_Medical_Center.csv"  # Change file extension to CSV

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

display(df)

# COMMAND ----------

