# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.projhealthcare.dfs.core.windows.net",
    "da0/FIoxp43uIM+x3NLKrS17b6XBds5V+AwU/o+we5+7ohHOUxaUk39NxnyjeuzfM2nqrZTlt+M8+ASt7Cp21A=="
)

# COMMAND ----------

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

def upsert_dimension(path: str, df_new, key_cols: list, id_col: str):
    dedup = df_new.dropDuplicates(key_cols)
    try:
        existing = spark.read.format("delta").load(path)
        new_rows = dedup.join(existing.select(key_cols), key_cols, "left_anti")
        if new_rows.count() > 0:
            max_id = existing.agg(F.max(id_col)).collect()[0][0] or 0
            win    = Window.orderBy(key_cols)
            new_rows = new_rows.withColumn(id_col, row_number().over(win) + max_id)
            new_rows.write.format("delta").mode("append").save(path)
    except Exception:
        win     = Window.orderBy(key_cols)
        initial = dedup.withColumn(id_col, row_number().over(win))
        initial.write.format("delta").mode("overwrite").save(path)


def process_hospital_data(hospital_name: str):
    silver = f"abfss://stage@projhealthcare.dfs.core.windows.net/{hospital_name}/"
    gold   = "abfss://demo@projhealthcare.dfs.core.windows.net"

    df   = spark.read.parquet(silver)
    raw  = df.alias("raw")

    # ─────────────── DIMENSIONS ───────────────

    dim_hosp = raw.selectExpr(
        "hospital_name AS name",
        "hospital_location AS location",
        "last_updated_on"
    )
    upsert_dimension(f"{gold}/dim_hospital/",         dim_hosp,  ["name"],                "hospital_id")

    dim_proc = raw.selectExpr(
        "cpt_code AS code",
        "description"
    )
    upsert_dimension(f"{gold}/dim_procedure_code/",   dim_proc,  ["code","description"],  "code_id")

    dim_set  = raw.selectExpr(
        "setting AS Setting"
    )
    upsert_dimension(f"{gold}/dim_setting/",          dim_set,   ["Setting"],             "setting_id")

    dim_plan = raw.selectExpr(
        "payer_name",
        "plan_name"
    )
    upsert_dimension(f"{gold}/dim_plan/",             dim_plan,  ["payer_name","plan_name"], "plan_id")

    # reload dims with aliases
    hosp = spark.read.format("delta").load(f"{gold}/dim_hospital/").alias("hosp")
    proc = spark.read.format("delta").load(f"{gold}/dim_procedure_code/").alias("proc")
    sett = spark.read.format("delta").load(f"{gold}/dim_setting/").alias("sett")
    plan = spark.read.format("delta").load(f"{gold}/dim_plan/").alias("plan")

    # ─────────────── FACT_CHARGE ───────────────

    fact_keys = [
        "hospital_id","code_id","setting_id","plan_id",
        "standard_charge_gross","standard_charge_discounted_cash",
        "standard_charge_methodology","standard_charge_min","standard_charge_max",
        "standard_charge_negotiated_dollar",
        "standard_charge_negotiated_percentage",
        "standard_charge_negotiated_algorithm",
        "estimated_amount"
    ]

    raw_fact = (
        raw.select(
            "hospital_name","cpt_code","setting","plan_name",
            "standard_charge_gross",
            "standard_charge_discounted_cash",
            "standard_charge_methodology",
            "standard_charge_min",
            "standard_charge_max",
            "standard_charge_negotiated_dollar",
            "standard_charge_negotiated_percentage",
            "standard_charge_negotiated_algorithm",
            "estimated_amount"
        )
        .dropDuplicates()
        .join(hosp,  F.col("hosp.name")   == F.col("raw.hospital_name"),     "left")
        .join(proc,  F.col("proc.code")   == F.col("raw.cpt_code"),           "left")
        .join(sett,  F.col("sett.Setting")== F.col("raw.setting"),            "left")
        .join(plan,  F.col("plan.plan_name")==F.col("raw.plan_name"),         "left")
        .select(*fact_keys)
    )

    existing_path = f"{gold}/fact_charge/"

    try:
        existing = spark.read.format("delta").load(existing_path)
        new_rows = raw_fact.dropDuplicates(fact_keys) \
                           .join(existing.select(fact_keys), fact_keys, "left_anti")
        if new_rows.count() > 0:
            max_id = existing.agg(F.max("charge_id")).collect()[0][0] or 0
            win    = Window.orderBy(["hospital_id","code_id","setting_id","plan_id"])
            new_rows = new_rows.withColumn("charge_id", row_number().over(win) + max_id)
            new_rows.write.format("delta").mode("append").save(existing_path)
    except Exception:
        win     = Window.orderBy(fact_keys)
        initial = raw_fact.dropDuplicates(fact_keys) \
                          .withColumn("charge_id", row_number().over(win))
        initial.write.format("delta").mode("overwrite").save(existing_path)

    print(f"✅ Completed processing for: {hospital_name}")


# COMMAND ----------

hospital_list = [
    "Contra_Costa_Regional_Medical_Center",
    "Priscilla_Chan_Mark_Zuckerberg_San_Francisco_General_Hospital_Trauma_Center",
    # "Scripps_Green_Hospital",
    # "Scripps_Memorial_Hospital_Encinitas",
    # "Scripps_Memorial_Hospital_La_Jolla",
    # "Scripps_Mercy_Hospital_Chula_Vista",
    # "Scripps_Mercy_Hospital_San_Diego",
    # "Sharp_Chula_Vista_Medical_Center",
    # "Sharp_Coronado_Hospital",
    # "Sharp_Grossmont_Hospital",
    "Sharp_Memorial_Hospital"
]

for h in hospital_list:
    process_hospital_data(h)


# COMMAND ----------

