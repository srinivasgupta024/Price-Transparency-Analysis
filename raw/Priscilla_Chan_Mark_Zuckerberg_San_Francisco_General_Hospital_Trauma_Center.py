# Databricks notebook source
# Define storage details
storage_account_name = "projhealthcaresa"
container_name = "raw-data"
file_path = "Priscilla_Chan_Mark_Zuckerberg_San_Francisco_General_Hospital_Trauma_Center.csv"  # Change file extension to CSV

# Construct the full path
csv_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}"


# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# 1. Read raw file as an RDD of lines
raw_rdd = spark.sparkContext.textFile(csv_path)

# 2. Drop the first two physical lines
data_rdd = (
    raw_rdd
    .zipWithIndex()
    .filter(lambda li: li[1] >= 2)  # keep only lines with index ≥ 2
    .map(lambda li: li[0])          # unwrap back to just the line text
)

# 3. Use spark.read.csv on the RDD (not .load())
df = (
    spark.read
         .option("header",    "true")
         .option("sep",       ",")
         .option("multiLine", "true")
         .option("quote",     '"')
         .option("escape",    '"')
         .csv(data_rdd)                  # <— note .csv(data_rdd)
)

print(f"Loaded {len(df.columns)} columns:")
print(df.columns)
df.show(5, truncate=False)


# COMMAND ----------

raw_rdd.take(10)

# COMMAND ----------

display(df)

# COMMAND ----------

df.columns

# COMMAND ----------

from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType

# 1️⃣ Add the constant hospital columns
df2 = (
    df
    .withColumn("hospital_name",     lit("PRISCILLA CHAN & MARK ZUCKERBERG SAN FRANCISCO GENERAL HOSPITAL & TRAUMA CENTER"))
    .withColumn("last_updated_on",   lit("9/26/2024"))
    .withColumn("hospital_location", lit("1001 POTRERO AVE, SAN FRANCISCO, CALIFORNIA 94110-3518"))
)

# 2️⃣ Select and rename into your exact final schema
final_df = df2.select(
    # new hospital info
    col("hospital_name"),
    col("last_updated_on"),
    col("hospital_location"),

    # existing → renamed
    col("description"),
    col("code|1").alias("code_1"),
    col("code|1|type").alias("code_1_type"),
    col("code|2").alias("code_2"),
    col("code|2|type").alias("code_2_type"),
    col("code|3").alias("code_3"),
    col("code|3|type").alias("code_3_type"),

    # injected nulls for code_4
    lit(None).cast(StringType()).alias("code_4"),
    lit(None).cast(StringType()).alias("code_4_type"),

    # next existing fields
    col("setting"),
    col("drug_unit_of_measurement"),
    col("drug_type_of_measurement"),

    col("standard_charge|gross").alias("standard_charge_gross"),
    col("standard_charge|discounted_cash").alias("standard_charge_discounted_cash"),

    col("payer_name"),
    col("plan_name"),
    col("modifiers"),

    col("standard_charge|negotiated_dollar").alias("standard_charge_negotiated_dollar"),
    col("standard_charge|negotiated_percentage").alias("standard_charge_negotiated_percentage"),
    col("standard_charge|negotiated_algorithm").alias("standard_charge_negotiated_algorithm"),

    col("estimated_amount"),
    col("standard_charge|methodology").alias("standard_charge_methodology"),

    col("standard_charge|min").alias("standard_charge_min"),
    col("standard_charge|max").alias("standard_charge_max"),

    col("additional_generic_notes")
)

# now final_df has exactly 28 columns, in the order you specified
print(len(final_df.columns), "columns:")
print(final_df.columns)


# COMMAND ----------

display(final_df)

# COMMAND ----------

output_path = "abfss://bronze@projhealthcaresa.dfs.core.windows.net/Priscilla_Chan_Mark_Zuckerberg_San_Francisco_General_Hospital_Trauma_Center"
final_df.write.mode("overwrite").parquet(output_path)

# COMMAND ----------

