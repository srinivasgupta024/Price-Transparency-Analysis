# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.projhealthcare.dfs.core.windows.net",
    "da0/FIoxp43uIM+x3NLKrS17b6XBds5V+AwU/o+we5+7ohHOUxaUk39NxnyjeuzfM2nqrZTlt+M8+ASt7Cp21A=="
)

# COMMAND ----------

from pyspark.sql.functions import col

# 1) CONFIGURE PATHS —————————————————————————————————————————————————
demo_base            = "abfss://demo@projhealthcare.dfs.core.windows.net"
dim_hospital_path    = f"{demo_base}/dim_hospital"
dim_setting_path     = f"{demo_base}/dim_setting"
dim_procedure_path   = f"{demo_base}/dim_procedure_code"
dim_plan_path        = f"{demo_base}/dim_plan"
dim_payer_path       = f"{demo_base}/dim_payer"
fact_charge_path     = f"{demo_base}/fact_charge"
fact_neg_path        = f"{demo_base}/fact_negotiated_charge"
main_path            = f"{demo_base}/fact_main"

# 2) LOAD ALL TABLES —————————————————————————————————————————————————
dim_hospital    = spark.read.format("delta").load(dim_hospital_path)
dim_setting     = spark.read.format("delta").load(dim_setting_path)
dim_procedure   = spark.read.format("delta").load(dim_procedure_path)
dim_plan        = spark.read.format("delta").load(dim_plan_path)
dim_payer       = spark.read.format("delta").load(dim_payer_path)
fact_charge     = spark.read.format("delta").load(fact_charge_path)
fact_neg        = spark.read.format("delta").load(fact_neg_path)

# 3) STEP 1: ENRICH NEGOTIATED_CHARGE WITH PLAN & PAYER ——————————————
neg_enriched = (
    fact_neg.alias("fn")
      .join(
        dim_plan
          .select("plan_id","plan_name","payer_id")
          .alias("dp"),
        on="plan_id", how="left"
      )
      .join(
        dim_payer
          .select("payer_id","payer_name")
          .alias("py"),
        on="payer_id", how="left"
      )
)

# 4) STEP 2: ENRICH FACT_CHARGE WITH HOSPITAL, PROCEDURE, SETTING —————————
charge_enriched = (
    fact_charge.alias("fc")
      .join(
        dim_hospital
          .select("hospital_id","hospital_name","hospital_location")
          .alias("dh"),
        on="hospital_id", how="left"
      )
      .join(
        dim_procedure
          .select("procedure_id","cpt_code","description")
          .alias("dpr"),
        on="procedure_id", how="left"
      )
      .join(
        dim_setting
          .select("setting_id","setting")
          .alias("ds"),
        on="setting_id", how="left"
      )
)

# 5) STEP 3: JOIN CHARGE + NEGOTIATED ON charge_id ————————————————
final_df = (
    charge_enriched.alias("ce")
      .join(
        neg_enriched.alias("ne"),
        on="charge_id", how="left"
      )
      .select(
        "ce.charge_id",

        # hospital attributes
        "hospital_id","hospital_name","hospital_location",

        # setting
        "setting_id","setting",

        # procedure
        "procedure_id","cpt_code","description",

        # plan & payer
        "plan_id","plan_name","payer_id","payer_name",

        # charge metrics
        "standard_charge_gross",
        "standard_charge_discounted_cash",
        "standard_charge_methodology",
        "standard_charge_min",
        "standard_charge_max",

        # negotiated-charge metrics
        "standard_charge_negotiated_dollar",
        "standard_charge_negotiated_percentage",
        "standard_charge_negotiated_algorithm",
        "estimated_amount"
      )
)

# # 6) STEP 4: WRITE OUT THE FINAL MAIN FACT ————————————————————————
# final_df.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .option("mergeSchema","true") \
#     .save(main_path)


# COMMAND ----------

display(final_df)

# COMMAND ----------

from pyspark.sql.functions import col
demo = final_df.filter(col("cpt_code") == "11422")
display(demo)

# COMMAND ----------

display(neg_enriched)
from pyspark.sql.functions import col
demo = neg_enriched.filter(col("cpt_code") == "11422")
display(demo)

# COMMAND ----------

