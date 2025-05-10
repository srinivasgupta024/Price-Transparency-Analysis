# Databricks notebook source
# 1️⃣ Define storage details & paths
storage_account = "projhealthcaresa"
container       = "warehouse"

fact_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/fact_charge_delta/"
hosp_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/dim_hospital_delta/"

# 2️⃣ Read them in as DataFrames
fact_df = spark.read.format("delta").load(fact_path)
hosp_df = spark.read.format("delta").load(hosp_path)

# 3️⃣ Build the hospital summary
from pyspark.sql.functions import count, avg

hospital_summary = (
    fact_df
      .join(hosp_df, "hospital_id")
      .groupBy("hospital_id", "hospital_name")
      .agg(
        count("*").alias("num_services"),
        avg("gross_charge").alias("avg_gross_charge"),
        avg("discounted_cash").alias("avg_cash_price"),
        avg("negotiated_dollar").alias("avg_negotiated_rate"),
        avg("estimated_amount").alias("avg_estimated_paid")
      )
)

# 4️⃣ Write back out to Gold as a Delta table
gold_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/hospital_summary_delta/"

hospital_summary.write \
  .format("delta") \
  .mode("overwrite") \
  .save(gold_path)


# COMMAND ----------

# 0️⃣ Common storage variables
storage_account     = "projhealthcaresa"
warehouse_container = "warehouse"
gold_container      = "gold"

fact_path  = f"abfss://{warehouse_container}@{storage_account}.dfs.core.windows.net/fact_charge_delta/"
gold_base   = f"abfss://{gold_container}@{storage_account}.dfs.core.windows.net/"

# 1️⃣ Load the fact once
fact_df = spark.read.format("delta").load(fact_path)


# COMMAND ----------

from pyspark.sql.functions import countDistinct, avg, stddev

# Load procedure dim
proc_df = spark.read.format("delta") \
    .load(f"abfss://{warehouse_container}@{storage_account}.dfs.core.windows.net/dim_procedure_delta/")

# Build summary
procedure_summary = (
  fact_df
    .join(proc_df, "procedure_id")
    .groupBy("procedure_id", "cpt_code", "description")
    .agg(
      countDistinct("hospital_id").alias("num_hospitals_offering"),
      avg("gross_charge").alias("avg_gross_charge"),
      avg("negotiated_dollar").alias("avg_negotiated_rate"),
      stddev("negotiated_dollar").alias("stddev_negotiated")
    )
)

# Persist to Gold
procedure_summary.write \
  .format("delta") \
  .mode("overwrite") \
  .save(gold_base + "procedure_summary_delta/")


# COMMAND ----------

from pyspark.sql.functions import countDistinct, avg

# Load payer & plan dims
payer_df = spark.read.format("delta") \
    .load(f"abfss://{warehouse_container}@{storage_account}.dfs.core.windows.net/dim_payer_delta/")
plan_df  = spark.read.format("delta") \
    .load(f"abfss://{warehouse_container}@{storage_account}.dfs.core.windows.net/dim_plan_delta/")

# Build summary
payer_summary = (
  fact_df
    .join(plan_df, "plan_id")
    .join(payer_df, "payer_id")
    .groupBy("payer_id", "payer_name", "plan_name")
    .agg(
      countDistinct("hospital_id").alias("num_hospitals"),
      avg("negotiated_dollar").alias("avg_negotiated_dollar"),
      avg("negotiated_pct").alias("avg_negotiated_pct")
    )
)

# Persist to Gold
payer_summary.write \
  .format("delta") \
  .mode("overwrite") \
  .save(gold_base + "payer_summary_delta/")


# COMMAND ----------

from pyspark.sql.functions import avg

# Re-load hospital & procedure dims if not in scope
hosp_df = spark.read.format("delta") \
    .load(f"abfss://{warehouse_container}@{storage_account}.dfs.core.windows.net/dim_hospital_delta/")
proc_df = spark.read.format("delta") \
    .load(f"abfss://{warehouse_container}@{storage_account}.dfs.core.windows.net/dim_procedure_delta/")

# Build cross-tab
hospital_procedure = (
  fact_df
    .join(hosp_df, "hospital_id")
    .join(proc_df, "procedure_id")
    .groupBy("hospital_id", "hospital_name", "procedure_id", "cpt_code")
    .agg(
      avg("gross_charge").alias("avg_gross_charge"),
      avg("negotiated_dollar").alias("avg_negotiated_rate")
    )
)

# Persist to Gold
hospital_procedure.write \
  .format("delta") \
  .mode("overwrite") \
  .save(gold_base + "hospital_procedure_delta/")


# COMMAND ----------

storage_account     = "projhealthcaresa"
warehouse_container = "warehouse"
gold_container      = "gold"
fact_df             = spark.read.format("delta").load(f"abfss://{warehouse_container}@{storage_account}.dfs.core.windows.net/fact_charge_delta/")
gold_base           = f"abfss://{gold_container}@{storage_account}.dfs.core.windows.net/"


# COMMAND ----------

from pyspark.sql.functions import countDistinct, avg

# Load setting dim
setting_df = spark.read.format("delta") \
    .load(f"abfss://{warehouse_container}@{storage_account}.dfs.core.windows.net/dim_setting_delta/")

# Build summary
setting_summary = (
  fact_df
    .join(setting_df, "setting_id")
    .groupBy("setting_id", "setting")
    .agg(
      countDistinct("hospital_id").alias("num_hospitals"),
      avg("gross_charge").alias("avg_gross_charge"),
      avg("discounted_cash").alias("avg_cash_price"),
      avg("negotiated_dollar").alias("avg_negotiated_rate")
    )
)

# Persist to Gold
setting_summary.write \
  .format("delta") \
  .mode("overwrite") \
  .save(gold_base + "setting_summary_delta/")


# COMMAND ----------

from pyspark.sql.functions import count, avg

# Load methodology dim
methodology_df = spark.read.format("delta") \
    .load(f"abfss://{warehouse_container}@{storage_account}.dfs.core.windows.net/dim_methodology_delta/")

# Build summary
methodology_summary = (
  fact_df
    .join(methodology_df, "methodology_id")
    .groupBy("methodology_id", "methodology")
    .agg(
      count("*").alias("num_records"),
      avg("negotiated_dollar").alias("avg_negotiated_rate"),
      avg("estimated_amount").alias("avg_estimated_paid")
    )
)

# Persist to Gold
methodology_summary.write \
  .format("delta") \
  .mode("overwrite") \
  .save(gold_base + "methodology_summary_delta/")


# COMMAND ----------

