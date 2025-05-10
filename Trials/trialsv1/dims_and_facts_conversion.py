# Databricks notebook source
# Define storage details
storage_account_name = "projhealthcaresa"
container_name = "silver"
file_path = "Sharp_Coronado_Hospital"

# Construct the full path
parquet_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}/"



# COMMAND ----------

silver_df = spark.read.parquet(parquet_path)
display(silver_df)

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

# 1️⃣ Build your hospital dimension
hospital_dim = (
    silver_df
      .select("hospital_name", "hospital_location", "last_updated_on")
      .distinct()
      .withColumn("hospital_id", monotonically_increasing_id())
      .select("hospital_id", "hospital_name", "hospital_location", "last_updated_on")
)

# 2️⃣ Write out as Delta
output_path = "abfss://warehouse@projhealthcaresa.dfs.core.windows.net/dim_hospital_delta"

hospital_dim.write \
  .format("delta") \
  .mode("overwrite") \
  .save(output_path)


# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

# 3.1️⃣ Build procedure_dim
procedure_dim = (
  silver_df
    .select("cpt_code", "description")
    .distinct()
    .withColumn("procedure_id", monotonically_increasing_id())
    .select("procedure_id", "cpt_code", "description")
)

# Inspect a few rows
display(procedure_dim.limit(5))

# 3.2️⃣ Persist as Delta
procedure_output = "abfss://warehouse@projhealthcaresa.dfs.core.windows.net/dim_procedure_delta"

procedure_dim.write \
  .format("delta") \
  .mode("overwrite") \
  .save(procedure_output)


# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

# Extract unique payers
payer_dim = (
  silver_df
    .select("payer_name")
    .distinct()
    .withColumn("payer_id", monotonically_increasing_id())
    .select("payer_id", "payer_name")
)

display(payer_dim.limit(5))

# Write out as Delta
payer_output = "abfss://warehouse@projhealthcaresa.dfs.core.windows.net/dim_payer_delta"

payer_dim.write \
  .format("delta") \
  .mode("overwrite") \
  .save(payer_output)


# COMMAND ----------

# Extract unique plans with their payer_name, then join to get payer_id
plan_dim = (
  silver_df
    .select("plan_name", "payer_name")
    .distinct()
    .join(payer_dim, on="payer_name", how="left")
    .withColumn("plan_id", monotonically_increasing_id())
    .select("plan_id", "payer_id", "plan_name")
)

display(plan_dim.limit(5))

# Write out as Delta
plan_output = "abfss://warehouse@projhealthcaresa.dfs.core.windows.net/dim_plan_delta"

plan_dim.write \
  .format("delta") \
  .mode("overwrite") \
  .save(plan_output)


# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

# Extract unique care settings
setting_dim = (
  silver_df
    .select("setting")
    .distinct()
    .withColumn("setting_id", monotonically_increasing_id())
    .select("setting_id", "setting")
)

display(setting_dim)

# Persist as Delta
setting_output = "abfss://warehouse@projhealthcaresa.dfs.core.windows.net/dim_setting_delta"

setting_dim.write \
  .format("delta") \
  .mode("overwrite") \
  .save(setting_output)


# COMMAND ----------

# Extract unique contract methodologies
from pyspark.sql.functions import col

methodology_dim = (
  silver_df
    .select(col("standard_charge_methodology").alias("methodology"))
    .distinct()
    .withColumn("methodology_id", monotonically_increasing_id())
    .select("methodology_id", "methodology")
)

display(methodology_dim)

# Persist as Delta
methodology_output = "abfss://warehouse@projhealthcaresa.dfs.core.windows.net/dim_methodology_delta"

methodology_dim.write \
  .format("delta") \
  .mode("overwrite") \
  .save(methodology_output)


# COMMAND ----------

from pyspark.sql.functions import row_number, monotonically_increasing_id, col
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Define which columns form your natural/business key
merge_keys = ["hospital_id","procedure_id","plan_id","setting_id"]

# 1️⃣ Build the raw fact by joining on the **natural keys** from silver → dims
fact_new = (
    silver_df
      # bring in hospital_id by joining on the natural key triple
      .join(hospital_dim, ["hospital_name","hospital_location","last_updated_on"], "left")
      # bring in procedure_id
      .join(procedure_dim, ["cpt_code","description"], "left")
      # bring in payer_id and plan_id
      .join(payer_dim,   "payer_name",                                    "left")
      .join(plan_dim,    ["plan_name","payer_id"],                         "left")
      # bring in setting_id
      .join(setting_dim, "setting",                                        "left")
      # bring in methodology_id
      .join(
         methodology_dim,
         silver_df.standard_charge_methodology == methodology_dim.methodology,
         "left"
      )
      # select the IDs + measures
      .select(
        "hospital_id","procedure_id","plan_id","setting_id","methodology_id",
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

# 2️⃣ Deduplicate so each (hospital,procedure,plan,setting) appears only once
window = Window.partitionBy(*merge_keys).orderBy(monotonically_increasing_id())
fact_dedup = (
  fact_new
    .withColumn("rn", row_number().over(window))
    .filter(col("rn") == 1)
    .drop("rn")
    .withColumn("fact_id", monotonically_increasing_id())
)

# 3️⃣ Upsert into your Delta fact table
fact_path = "abfss://warehouse@projhealthcaresa.dfs.core.windows.net/fact_charge_delta"
delta_fact = DeltaTable.forPath(spark, fact_path)

(
  delta_fact.alias("t")
    .merge(
      fact_dedup.alias("s"),
      " AND ".join([f"t.{k}=s.{k}" for k in merge_keys])
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)


# COMMAND ----------

