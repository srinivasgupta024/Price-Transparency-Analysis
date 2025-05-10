# Databricks notebook source
from pyspark.sql.functions import (
    monotonically_increasing_id,
    row_number, col,
    count, avg, countDistinct, stddev
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ── 0. DEFINE STORAGE & PATHS ───────────────────────────────────────
storage_account_name = "projhealthcaresa"

# Silver: ingest one hospital at a time
silver_container = "silver"
file_path        = "Sharp_Coronado_Hospital"
silver_path      = (
    f"abfss://{silver_container}@{storage_account_name}"
    f".dfs.core.windows.net/{file_path}/"
)

# Warehouse (dims + fact)
warehouse_container = "warehouse"
warehouse_path      = (
    f"abfss://{warehouse_container}@{storage_account_name}"
    f".dfs.core.windows.net"
)

# Gold (summaries)
gold_container = "gold"
gold_path      = (
    f"abfss://{gold_container}@{storage_account_name}"
    f".dfs.core.windows.net"
)

# ── 1. LOAD SILVER LAYER ────────────────────────────────────────────
silver_df = spark.read.parquet(silver_path)

# ── 2. BUILD & OVERWRITE DIMENSIONS ─────────────────────────────────
# 2.1 Hospital
hospital_dim = (
    silver_df
      .select("hospital_name", "hospital_location", "last_updated_on")
      .distinct()
      .withColumn("hospital_id", monotonically_increasing_id())
)
hospital_dim.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{warehouse_path}/dim_hospital_delta")

# 2.2 Procedure
procedure_dim = (
    silver_df
      .select("cpt_code", "description")
      .distinct()
      .withColumn("procedure_id", monotonically_increasing_id())
)
procedure_dim.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{warehouse_path}/dim_procedure_delta")

# 2.3 Payer
payer_dim = (
    silver_df
      .select("payer_name")
      .distinct()
      .withColumn("payer_id", monotonically_increasing_id())
)
payer_dim.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{warehouse_path}/dim_payer_delta")

# 2.4 Plan
plan_dim = (
    silver_df
      .select("plan_name", "payer_name")
      .distinct()
      .join(payer_dim, on="payer_name", how="left")
      .withColumn("plan_id", monotonically_increasing_id())
      .select("plan_id", "payer_id", "plan_name")
)
plan_dim.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{warehouse_path}/dim_plan_delta")

# 2.5 Setting
setting_dim = (
    silver_df
      .select("setting")
      .distinct()
      .withColumn("setting_id", monotonically_increasing_id())
)
setting_dim.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{warehouse_path}/dim_setting_delta")

# 2.6 Methodology
methodology_dim = (
    silver_df
      .select(col("standard_charge_methodology").alias("methodology"))
      .distinct()
      .withColumn("methodology_id", monotonically_increasing_id())
)
methodology_dim.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{warehouse_path}/dim_methodology_delta")

# ── 3. BUILD & MERGE-ONLY-INSERT FACT TABLE ───────────────────────────
# 3.1 Join dims to get surrogate keys
fact_new = (
    silver_df
      .join(hospital_dim, ["hospital_name","hospital_location","last_updated_on"], "left")
      .join(procedure_dim, ["cpt_code","description"], "left")
      .join(payer_dim,   "payer_name",                                   "left")
      .join(plan_dim,    ["plan_name","payer_id"],                        "left")
      .join(setting_dim, "setting",                                       "left")
      .join(methodology_dim,
            silver_df.standard_charge_methodology == methodology_dim.methodology,
            "left")
      .select(
        "hospital_id",
        "procedure_id",
        "plan_id",
        "setting_id",
        "methodology_id",
        col("standard_charge_gross").alias("gross_charge"),
        col("standard_charge_discounted_cash").alias("discounted_cash"),
        col("standard_charge_min").alias("min_charge"),
        col("standard_charge_max").alias("max_charge"),
        col("standard_charge_negotiated_dollar").alias("negotiated_dollar"),
        col("standard_charge_negotiated_percentage").alias("negotiated_pct"),
        col("standard_charge_negotiated_algorithm").alias("negotiated_algorithm"),
        col("estimated_amount")
      )
)

# 3.2 Deduplicate on business key
merge_keys = ["hospital_id","procedure_id","plan_id","setting_id"]
window = Window.partitionBy(*merge_keys).orderBy(monotonically_increasing_id())
fact_dedup = (
    fact_new
      .withColumn("rn", row_number().over(window))
      .filter(col("rn") == 1)
      .drop("rn")
      .withColumn("fact_id", monotonically_increasing_id())
)

# 3.3 MERGE-ONLY-INSERT into Delta fact
fact_path  = f"{warehouse_path}/fact_charge_delta"
delta_fact = DeltaTable.forPath(spark, fact_path)
merge_cond = " AND ".join([f"t.{k}=s.{k}" for k in merge_keys])

delta_fact.alias("t") \
  .merge(fact_dedup.alias("s"), merge_cond) \
  .whenNotMatchedInsertAll() \
  .execute()

# ── 4. REFRESH GOLD SUMMARIES (FULL OVERWRITE) ────────────────────────
fact_df        = spark.read.format("delta").load(fact_path)
hosp_df        = spark.read.format("delta").load(f"{warehouse_path}/dim_hospital_delta")
proc_df        = spark.read.format("delta").load(f"{warehouse_path}/dim_procedure_delta")
payer_df       = spark.read.format("delta").load(f"{warehouse_path}/dim_payer_delta")
plan_df        = spark.read.format("delta").load(f"{warehouse_path}/dim_plan_delta")
setting_df     = spark.read.format("delta").load(f"{warehouse_path}/dim_setting_delta")
methodology_df = spark.read.format("delta").load(f"{warehouse_path}/dim_methodology_delta")

# 4.1 Hospital Summary
hospital_summary = (
    fact_df
      .join(hosp_df, "hospital_id")
      .groupBy("hospital_id","hospital_name")
      .agg(
        count("*").alias("num_services"),
        avg("gross_charge").alias("avg_gross_charge"),
        avg("discounted_cash").alias("avg_cash_price"),
        avg("negotiated_dollar").alias("avg_negotiated_rate"),
        avg("estimated_amount").alias("avg_estimated_paid")
      )
)
hospital_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/hospital_summary_delta")

# 4.2 Procedure Summary
procedure_summary = (
    fact_df
      .join(proc_df, "procedure_id")
      .groupBy("procedure_id","cpt_code","description")
      .agg(
        countDistinct("hospital_id").alias("num_hospitals_offering"),
        avg("gross_charge").alias("avg_gross_charge"),
        avg("negotiated_dollar").alias("avg_negotiated_rate"),
        stddev("negotiated_dollar").alias("stddev_negotiated")
      )
)
procedure_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/procedure_summary_delta")

# 4.3 Payer & Plan Summary
payer_summary = (
    fact_df
      .join(plan_df, "plan_id")
      .join(payer_df, "payer_id")
      .groupBy("payer_id","payer_name","plan_name")
      .agg(
        countDistinct("hospital_id").alias("num_hospitals"),
        avg("negotiated_dollar").alias("avg_negotiated_dollar"),
        avg("negotiated_pct").alias("avg_negotiated_pct")
      )
)
payer_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/payer_summary_delta")

# 4.4 Hospital × Procedure Cross-Tab
hospital_procedure = (
    fact_df
      .join(hosp_df, "hospital_id")
      .join(proc_df, "procedure_id")
      .groupBy("hospital_id","hospital_name","procedure_id","cpt_code")
      .agg(
        avg("gross_charge").alias("avg_gross_charge"),
        avg("negotiated_dollar").alias("avg_negotiated_rate")
      )
)
hospital_procedure.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/hospital_procedure_delta")

# 4.5 Setting Summary
setting_summary = (
    fact_df
      .join(setting_df, "setting_id")
      .groupBy("setting_id","setting")
      .agg(
        countDistinct("hospital_id").alias("num_hospitals"),
        avg("gross_charge").alias("avg_gross_charge"),
        avg("discounted_cash").alias("avg_cash_price"),
        avg("negotiated_dollar").alias("avg_negotiated_rate")
      )
)
setting_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/setting_summary_delta")

# 4.6 Methodology Summary
methodology_summary = (
    fact_df
      .join(methodology_df, "methodology_id")
      .groupBy("methodology_id","methodology")
      .agg(
        count("*").alias("num_records"),
        avg("negotiated_dollar").alias("avg_negotiated_rate"),
        avg("estimated_amount").alias("avg_estimated_paid")
      )
)
methodology_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/methodology_summary_delta")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_hospitals FROM warehouse.dim_hospital;
# MAGIC SELECT * 
# MAGIC   FROM warehouse.dim_hospital 
# MAGIC --  WHERE hospital_name = 'Your New Hospital Name';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM gold.hospital_summary
# MAGIC --  WHERE hospital_name = 'Your New Hospital Name';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- How many distinct hospitals are in your dim?
# MAGIC SELECT COUNT(*) AS hospital_count
# MAGIC   FROM warehouse.dim_hospital;
# MAGIC

# COMMAND ----------

