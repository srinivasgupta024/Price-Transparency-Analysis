# Databricks notebook source
from pyspark.sql.functions import sha2, concat_ws, lit, substring, conv, col
from delta.tables import DeltaTable

# COMMAND ----------

# ----------------------------
# CONFIGURATION
# ----------------------------
gold_container      = "silver"
warehouse_container = "warehouse"
storage_account     = "projhealthcaresa"

gold_base = f"abfss://{gold_container}@{storage_account}.dfs.core.windows.net/"
wh_base   = f"abfss://{warehouse_container}@{storage_account}.dfs.core.windows.net/gold_warehouse/"

# ensure warehouse folder exists
dbutils.fs.mkdirs(wh_base)

# create Hive metastore database
spark.sql(f"""
  CREATE DATABASE IF NOT EXISTS gold_warehouse
  LOCATION '{wh_base}'
""")

# list of (hospital_name, delta_path)    
hospital_files = [
    ("Scripps_Mercy_Hospital_San_Diego", gold_base + "Scripps_Mercy_Hospital_San_Diego/"),
    ("Scripps_Mercy_Hospital_Chula_Vista", gold_base + "Scripps_Mercy_Hospital_Chula_Vista/"),
    ("Scripps_Memorial_Hospital_La_Jolla", gold_base + "Scripps_Memorial_Hospital_La_Jolla/"),
    ("Scripps_Memorial_Hospital_Encinitas", gold_base + "Scripps_Memorial_Hospital_Encinitas/"),
    ("Scripps_Green_Hospital", gold_base + "Scripps_Green_Hospital/")
]


# COMMAND ----------

# ----------------------------
# UPSERT UTILITY
# ----------------------------
def upsert(table_name, df, match_keys):
    tbl = f"gold_warehouse.{table_name}"
    if spark.catalog.tableExists(tbl):
        dt = DeltaTable.forName(spark, tbl)
        cond = " AND ".join([f"target.{k}=source.{k}" for k in match_keys])
        dt.alias("target") \
          .merge(df.alias("source"), cond) \
          .whenNotMatchedInsertAll() \
          .execute()
    else:
        df.write.format("delta").mode("overwrite").saveAsTable(tbl)

# COMMAND ----------

# ----------------------------
# PROCESS EACH HOSPITAL
# ----------------------------
for hospital_name, path in hospital_files:
    print(f"Processing: {hospital_name}")
    
    df_raw = spark.read.format("parquet").load(path)
    
    # 1) Hospital dim
    df_hosp = df_raw.select("hospital_name","hospital_location","last_updated_on") \
        .dropDuplicates() \
        .withColumn("hospital_id",
            conv(substring(sha2(concat_ws("||", col("hospital_name"), col("hospital_location")), 256),1,15),16,10).cast("long")
        )
    
    # 2) ProcedureCode dim
    df_proc = df_raw.selectExpr("cpt_code as code","description").dropDuplicates()
    
    # 3) Payer dim
    df_pay = df_raw.select("payer_name").dropDuplicates() \
        .withColumn("payer_id",
            conv(substring(sha2(col("payer_name"), 256),1,15),16,10).cast("long")
        )
    
    # 4) Plan dim
    df_plan = df_raw.select("plan_name","payer_name").dropDuplicates().alias("x") \
        .join(df_pay.alias("y"), col("x.payer_name")==col("y.payer_name"),"left") \
        .withColumn("plan_id",
            conv(substring(sha2(concat_ws("||", col("plan_name"), col("y.payer_id")),256),1,15),16,10).cast("long")
        ).select("plan_id","plan_name","payer_id")
    
    # 5) Charge dim
    base = df_raw.select("hospital_name","cpt_code","setting",
                         "standard_charge_gross","standard_charge_discounted_cash",
                         "standard_charge_methodology","standard_charge_min","standard_charge_max") \
                 .dropDuplicates().alias("b")
    df_chg = base \
        .join(df_hosp.alias("h"), col("b.hospital_name")==col("h.hospital_name"),"left") \
        .join(df_proc.alias("p"), col("b.cpt_code")==col("p.code"),"left") \
        .withColumn("charge_id",
            conv(substring(sha2(concat_ws("||", col("h.hospital_id"), col("p.code"), col("b.setting")),256),1,15),16,10).cast("long")
        ).select("charge_id","hospital_id","code","setting",
                 "standard_charge_gross","standard_charge_discounted_cash",
                 "standard_charge_methodology","standard_charge_min","standard_charge_max")
    
    # 6) NegotiatedCharge fact
    base2 = df_raw.select("plan_name","cpt_code","setting",
                          "standard_charge_negotiated_dollar","standard_charge_negotiated_percentage",
                          "standard_charge_negotiated_algorithm","estimated_amount") \
                  .dropDuplicates().alias("n")
    df_neg = base2 \
        .join(df_plan.alias("pl"), col("n.plan_name")==col("pl.plan_name"),"left") \
        .join(df_chg.alias("c"), (col("n.cpt_code")==col("c.code")) & (col("n.setting")==col("c.setting")),"left") \
        .select(col("c.charge_id"),col("pl.plan_id"),
                col("n.standard_charge_negotiated_dollar").alias("negotiated_dollar"),
                col("n.standard_charge_negotiated_percentage").alias("negotiated_percentage"),
                col("n.standard_charge_negotiated_algorithm").alias("negotiated_algorithm"),
                col("n.estimated_amount"))
    
    # UPSERT each table
    upsert("hospital",          df_hosp, ["hospital_id"])
    upsert("procedure_code",    df_proc, ["code"])
    upsert("payer",             df_pay,  ["payer_id"])
    upsert("plan",              df_plan, ["plan_id"])
    upsert("charge",            df_chg,  ["charge_id"])
    upsert("negotiated_charge", df_neg,  ["charge_id","plan_id"])


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Switch to your database
# MAGIC USE gold_warehouse;
# MAGIC
# MAGIC -- 1) Simple select
# MAGIC SELECT * 
# MAGIC FROM hospital 
# MAGIC LIMIT 10;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- In a notebook %sql cell, check for missing fields:
# MAGIC USE gold_warehouse;
# MAGIC SELECT hospital_name, hospital_location
# MAGIC FROM hospital
# MAGIC WHERE hospital_id IS NULL;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2) Join hospital → charge → negotiated_charge
# MAGIC SELECT
# MAGIC   h.hospital_name,
# MAGIC   c.setting,
# MAGIC   c.standard_charge_gross,
# MAGIC   n.negotiated_dollar,
# MAGIC   n.estimated_amount
# MAGIC FROM charge c
# MAGIC JOIN hospital h   ON c.hospital_id = h.hospital_id
# MAGIC LEFT JOIN negotiated_charge n 
# MAGIC    ON c.charge_id = n.charge_id
# MAGIC WHERE h.hospital_name = 'Scripps_Mercy_Hospital_San_Diego'
# MAGIC   AND n.negotiated_dollar IS NOT NULL
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_warehouse.hospital LIMIT 10;
# MAGIC

# COMMAND ----------

