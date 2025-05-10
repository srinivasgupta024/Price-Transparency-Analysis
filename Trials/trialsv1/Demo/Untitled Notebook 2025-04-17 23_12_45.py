# Databricks notebook source
from pyspark.sql.functions import sha2, concat_ws, lit, col
from delta.tables import DeltaTable

# ----------------------------
# CONFIGURATION
# ----------------------------
container_name = "silver"
storage_account_name = "projhealthcaresa"
base_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"

# Ensure the target directory exists
dbutils.fs.mkdirs(base_path)

# Create the Hive Metastore database if not exists
spark.sql(f"""
  CREATE DATABASE IF NOT EXISTS silver_warehouse
  LOCATION '{base_path}'
""")


# List of (hospital_name, delta_path) tuples
hospital_files = [
    ("Scripps_Mercy_Hospital_San_Diego", f"{base_path}Scripps_Mercy_Hospital_San_Diego/"),
    ("Scripps_Mercy_Hospital_Chula_Vista", f"{base_path}Scripps_Mercy_Hospital_Chula_Vista/")
    # ("Scripps_Memorial_Hospital_La_Jolla", f"{base_path}Scripps_Memorial_Hospital_La_Jolla/"),
    # ("Scripps_Memorial_Hospital_Encinitas", f"{base_path}Scripps_Memorial_Hospital_Encinitas/"),
    # ("Scripps_Green_Hospital", f"{base_path}Scripps_Green_Hospital/")
]



# COMMAND ----------

# ----------------------------
# UTILITY: Upsert (merge or create) Delta table
# ----------------------------
def upsert_delta(table_name, df, match_keys):
    full_table = f"silver_warehouse.{table_name}"
    if spark.catalog.tableExists(full_table):
        dt = DeltaTable.forName(spark, full_table)
        condition = " AND ".join([f"target.{k} = source.{k}" for k in match_keys])
        dt.alias("target") \
          .merge(df.alias("source"), condition) \
          .whenNotMatchedInsertAll() \
          .execute()
    else:
        df.write.format("delta") \
          .mode("overwrite") \
          .saveAsTable(full_table)

# COMMAND ----------

# ----------------------------
# PROCESS EACH HOSPITAL FILE
# ----------------------------
for hospital_name, path in hospital_files:
    print(f"Processing hospital: {hospital_name}")
    # Read the cleaned Delta file for this hospital
    df_raw = spark.read.format("parquet").load(path)

    # 1) Hospital dimension
    df_hospital = df_raw.select(
        "hospital_name", "hospital_location", "last_updated_on"
    ).dropDuplicates().withColumn(
        "hospital_id",
        sha2(concat_ws("||", col("hospital_name"), col("hospital_location")), 256)
    )

    # 2) ProcedureCode dimension
    df_procedure = df_raw.selectExpr(
        "cpt_code as code", "description"
    ).dropDuplicates()

    # 3) Payer dimension
    df_payer = df_raw.select("payer_name").dropDuplicates().withColumn(
        "payer_id",
        sha2(col("payer_name"), 256)
    )

    # 4) Plan dimension (needs payer_id)
    df_plan = df_raw.select("plan_name", "payer_name").dropDuplicates().alias("p0") \
        .join(df_payer.alias("py"), col("p0.payer_name") == col("py.payer_name"), "left") \
        .withColumn(
            "plan_id",
            sha2(concat_ws("||", col("p0.plan_name"), col("py.payer_id")), 256)
        ).select(col("plan_id"), col("p0.plan_name"), col("py.payer_id"))

    # 5) Charge dimension (needs hospital_id + procedure code)
    c0 = df_raw.select(
        "hospital_name", "cpt_code", "setting",
        "standard_charge_gross", "standard_charge_discounted_cash",
        "standard_charge_methodology", "standard_charge_min", "standard_charge_max"
    ).dropDuplicates().alias("c0")

    df_charge = c0 \
        .join(df_hospital.alias("h"), col("c0.hospital_name") == col("h.hospital_name"), "left") \
        .join(df_procedure.alias("pc"), col("c0.cpt_code") == col("pc.code"), "left") \
        .withColumn(
            "charge_id",
            sha2(concat_ws("||", col("h.hospital_id"), col("pc.code"), col("c0.setting")), 256)
        ).select(
            col("charge_id"), col("h.hospital_id"), col("pc.code").alias("code"),
            col("c0.setting"), col("c0.standard_charge_gross"),
            col("c0.standard_charge_discounted_cash"),
            col("c0.standard_charge_methodology"), col("c0.standard_charge_min"),
            col("c0.standard_charge_max")
        )

    # 6) NegotiatedCharge fact (needs charge_id + plan_id)
    n0 = df_raw.select(
        "plan_name", "cpt_code", "setting",
        "standard_charge_negotiated_dollar", "standard_charge_negotiated_percentage",
        "standard_charge_negotiated_algorithm", "estimated_amount"
    ).dropDuplicates().alias("n0")

    df_negotiated = n0 \
        .join(df_plan.alias("p"), col("n0.plan_name") == col("p.plan_name"), "left") \
        .join(df_charge.alias("c"), (col("n0.cpt_code") == col("c.code")) & (col("n0.setting") == col("c.setting")), "left") \
        .select(
            col("c.charge_id"), col("p.plan_id"),
            col("n0.standard_charge_negotiated_dollar").alias("negotiated_dollar"),
            col("n0.standard_charge_negotiated_percentage").alias("negotiated_percentage"),
            col("n0.standard_charge_negotiated_algorithm").alias("negotiated_algorithm"),
            col("n0.estimated_amount")
        )

    # ----------------------------
    # UPSERT EACH TABLE
    # ----------------------------
    upsert_delta("hospital", df_hospital, ["hospital_id"])
    upsert_delta("procedure_code", df_procedure, ["code"])
    upsert_delta("payer", df_payer, ["payer_id"])
    upsert_delta("plan", df_plan, ["plan_id"])
    upsert_delta("charge", df_charge, ["charge_id"])
    upsert_delta("negotiated_charge", df_negotiated, ["charge_id", "plan_id"])


# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Switch to the database
# MAGIC USE gold_warehouse;
# MAGIC
# MAGIC -- 2. Drop each table (add any you’ve created)
# MAGIC DROP TABLE IF EXISTS negotiated_charge;
# MAGIC DROP TABLE IF EXISTS charge;
# MAGIC DROP TABLE IF EXISTS plan;
# MAGIC DROP TABLE IF EXISTS payer;
# MAGIC DROP TABLE IF EXISTS procedure_code;
# MAGIC DROP TABLE IF EXISTS hospital;
# MAGIC
# MAGIC -- 3. Drop the database itself
# MAGIC DROP DATABASE IF EXISTS gold_warehouse;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Switch to the database
# MAGIC USE silver_warehouse;
# MAGIC
# MAGIC -- 2. Drop each table (add any you’ve created)
# MAGIC DROP TABLE IF EXISTS negotiated_charge;
# MAGIC DROP TABLE IF EXISTS charge;
# MAGIC DROP TABLE IF EXISTS plan;
# MAGIC DROP TABLE IF EXISTS payer;
# MAGIC DROP TABLE IF EXISTS procedure_code;
# MAGIC DROP TABLE IF EXISTS hospital;
# MAGIC
# MAGIC -- 3. Drop the database itself
# MAGIC DROP DATABASE IF EXISTS silver_warehouse;
# MAGIC

# COMMAND ----------

# in a Python cell
dbutils.fs.rm("abfss://gold@projhealthcaresa.dfs.core.windows.net/gold_warehouse/", recurse=True)


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Switch to your database
# MAGIC USE silver_warehouse;
# MAGIC
# MAGIC -- 1) Simple select
# MAGIC -- SELECT * 
# MAGIC -- FROM hospital 
# MAGIC -- LIMIT 10;
# MAGIC
# MAGIC -- 2) Join hospital → charge → negotiated_charge
# MAGIC -- SELECT
# MAGIC --   h.hospital_name,
# MAGIC --   c.setting,
# MAGIC --   c.standard_charge_gross,
# MAGIC --   n.negotiated_dollar,
# MAGIC --   n.estimated_amount
# MAGIC -- FROM charge c
# MAGIC -- JOIN hospital h   ON c.hospital_id = h.hospital_id
# MAGIC -- LEFT JOIN negotiated_charge n 
# MAGIC --    ON c.charge_id = n.charge_id
# MAGIC -- WHERE h.hospital_name = 'Scripps_Mercy_Hospital_San_Diego'
# MAGIC --   AND n.negotiated_dollar IS NOT NULL
# MAGIC -- LIMIT 20;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hospital  

# COMMAND ----------

