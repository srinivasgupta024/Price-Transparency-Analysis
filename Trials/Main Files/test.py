# Databricks notebook source
# HARD CODE YOUR STORAGE KEY HERE (be very careful!)
spark.conf.set(
    "fs.azure.account.key.projhealthcare.dfs.core.windows.net",
    "da0/FIoxp43uIM+x3NLKrS17b6XBds5V+AwU/o+we5+7ohHOUxaUk39NxnyjeuzfM2nqrZTlt+M8+ASt7Cp21A=="
)

# Set input/output paths
hospital = "Palomar_Health_Center-Poway.parquet"
silver_path = f"abfss://demo@projhealthcare.dfs.core.windows.net/{hospital}/"
gold_base_path = "abfss://gold@projhealthcare.dfs.core.windows.net"

# Load Parquet
df = spark.read.parquet(silver_path)

# Show sample
df.display()


# COMMAND ----------

from pyspark.sql.utils import AnalysisException
import os

# Hardcoded storage key
spark.conf.set(
    "fs.azure.account.key.projhealthcare.dfs.core.windows.net",
    "da0/FIoxp43uIM+x3NLKrS17b6XBds5V+AwU/o+we5+7ohHOUxaUk39NxnyjeuzfM2nqrZTlt+M8+ASt7Cp21A=="
)

# Define base path
silver_base_path = "abfss://silver@projhealthcare.dfs.core.windows.net/"

# List of hospital folders (manually defined or retrieved dynamically if needed)
hospital_list = [
    "Contra_Costa_Regional_Medical_Center",
    "Priscilla_Chan_Mark_Zuckerberg_San_Francisco_General_Hospital_Trauma_Center",
    "Scripps_Green_Hospital",
    "Scripps_Memorial_Hospital_Encinitas",
    "Scripps_Memorial_Hospital_La_Jolla",
    "Scripps_Mercy_Hospital_Chula_Vista",
    "Scripps_Mercy_Hospital_San_Diego",
    "Sharp_Chula_Vista_Medical_Center",
    "Sharp_Coronado_Hospital",
    "Sharp_Grossmont_Hospital",
    "Sharp_Memorial_Hospital"
]

# Iterate and print schema for each hospital
for hospital in hospital_list:
    path = os.path.join(silver_base_path, hospital)
    try:
        df = spark.read.parquet(path)
        print(f"\n=== Columns for {hospital} ===")
        print(df.columns)
        df.printSchema()
    except AnalysisException as e:
        print(f"\n[ERROR] Could not read Parquet for {hospital}: {str(e)}")


# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import StringType

# Define hospital folder
hospital = "Contra_Costa_Regional_Medical_Center"

# Define path
input_path = f"abfss://silver@projhealthcare.dfs.core.windows.net/{hospital}/"

# Step 1: Read the hospital file
df = spark.read.parquet(input_path)

# Step 2: Cast all columns to StringType
df_string = df.select([col(c).cast(StringType()).alias(c) for c in df.columns])

# Step 3: Overwrite the file at the same path
df_string.write.mode("overwrite").parquet(input_path)


# COMMAND ----------

