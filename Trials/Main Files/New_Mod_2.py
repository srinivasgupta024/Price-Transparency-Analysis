# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.projhealthcare.dfs.core.windows.net",
    "da0/FIoxp43uIM+x3NLKrS17b6XBds5V+AwU/o+we5+7ohHOUxaUk39NxnyjeuzfM2nqrZTlt+M8+ASt7Cp21A=="
)

# COMMAND ----------

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import row_number, lower
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

def upsert_dimension(path: str, df_new, key_cols: list, id_col: str):
    """
    Upsert logic for a dimension table in Delta:
     - df_new: DataFrame of new rows
     - key_cols: columns to dedupe/join on
     - id_col: surrogate key column to generate
    """
    dedup = df_new.dropDuplicates(key_cols)
    try:
        existing = spark.read.format("delta").load(path)
        # Isolate only rows not already present
        new_rows = dedup.join(existing.select(key_cols), key_cols, "left_anti")
        if new_rows.count() > 0:
            max_id = existing.agg(F.max(id_col)).collect()[0][0] or 0
            win = Window.orderBy(key_cols)
            new_rows = new_rows.withColumn(id_col, row_number().over(win) + max_id)
            new_rows.write.format("delta").mode("append").save(path)
    except Exception:
        # First load: assign ids from 1..N and overwrite
        win = Window.orderBy(key_cols)
        initial = dedup.withColumn(id_col, row_number().over(win))
        initial.write.format("delta").mode("overwrite").save(path)


def process_hospital_data(hospital_name: str):
    silver = f"abfss://stage@projhealthcare.dfs.core.windows.net/{hospital_name}/"
    gold = "abfss://gold@projhealthcare.dfs.core.windows.net"

    # Read raw data
    df = spark.read.parquet(silver)
    raw = df.alias("raw")

    # ────────────── Dimension: Hospital ──────────────
    dim_hosp = raw.selectExpr(
        "hospital_name AS name",
        "hospital_location AS location",
        "last_updated_on"
    )
    upsert_dimension(f"{gold}/dim_hospital/", dim_hosp, ["name"], "hospital_id")

    # Reload hospital dimension for FK
    hosp = spark.read.format("delta").load(f"{gold}/dim_hospital/").alias("hosp")

    # ────────────── Dimension: Procedure Code ──────────────
    dim_proc = (
        raw.join(hosp, F.col("raw.hospital_name") == F.col("hosp.name"), "inner")
           .select(
               F.col("hosp.hospital_id"),
               F.col("raw.cpt_code").alias("code"),
               F.col("raw.description")
           )
    )
    upsert_dimension(
        f"{gold}/dim_procedure_code/",
        dim_proc,
        ["hospital_id", "code", "description"],
        "code_id"
    )

    # ────────────── Dimension: Setting ──────────────
    dim_set = (
        raw.select(lower(F.col("raw.setting")).alias("Setting"))
    )
    upsert_dimension(
        f"{gold}/dim_setting/",
        dim_set,
        ["Setting"],
        "setting_id"
    )

    # ────────────── Dimension: Plan ──────────────
    dim_plan = raw.selectExpr("payer_name", "plan_name")
    upsert_dimension(
        f"{gold}/dim_plan/",
        dim_plan,
        ["payer_name", "plan_name"],
        "plan_id"
    )

    # ────────────── Fact: Charge ──────────────
    # Reload all dims
    hosp = spark.read.format("delta").load(f"{gold}/dim_hospital/").alias("hosp")
    proc = spark.read.format("delta").load(f"{gold}/dim_procedure_code/").alias("proc")
    sett = spark.read.format("delta").load(f"{gold}/dim_setting/").alias("sett")
    plan = spark.read.format("delta").load(f"{gold}/dim_plan/").alias("plan")

    # Define the set of business + measure columns for dedupe/join
    fact_keys = [
        "hospital_id", "code_id", "setting_id", "plan_id",
        "standard_charge_gross", "standard_charge_discounted_cash",
        "standard_charge_methodology", "standard_charge_min", "standard_charge_max",
        "standard_charge_negotiated_dollar", "standard_charge_negotiated_percentage",
        "standard_charge_negotiated_algorithm", "estimated_amount"
    ]

    # Build raw_fact and explicitly select/alias the four FK columns
    raw_fact = (
        raw.select(
            "hospital_name", "cpt_code", "description", "setting", "plan_name",
            "standard_charge_gross", "standard_charge_discounted_cash",
            "standard_charge_methodology", "standard_charge_min", "standard_charge_max",
            "standard_charge_negotiated_dollar", "standard_charge_negotiated_percentage",
            "standard_charge_negotiated_algorithm", "estimated_amount"
        )
        .dropDuplicates()
        # Join keys and select explicit FK columns to avoid ambiguity
        .join(hosp, F.col("raw.hospital_name") == F.col("hosp.name"), "inner")
        .join(
            proc,
            (F.col("proc.hospital_id") == F.col("hosp.hospital_id")) &
            (F.col("proc.code") == F.col("raw.cpt_code")) &
            (F.col("proc.description") == F.col("raw.description")),
            "inner"
        )
        .join(sett, F.col("sett.Setting") == lower(F.col("raw.setting")), "left")
        .join(plan, F.col("plan.plan_name") == F.col("raw.plan_name"), "left")
        .select(
            F.col("hosp.hospital_id").alias("hospital_id"),
            F.col("proc.code_id").alias("code_id"),
            F.col("sett.setting_id").alias("setting_id"),
            F.col("plan.plan_id").alias("plan_id"),
            *[F.col(c) for c in fact_keys if c not in {"hospital_id","code_id","setting_id","plan_id"}]
        )
    )

    existing_path = f"{gold}/fact_charge/"
    try:
        existing = spark.read.format("delta").load(existing_path)
        new_rows = (
            raw_fact.dropDuplicates(fact_keys)
                    .join(existing.select(fact_keys), fact_keys, "left_anti")
        )
        if new_rows.count() > 0:
            max_id = existing.agg(F.max("charge_id")).collect()[0][0] or 0
            win = Window.orderBy(["hospital_id","code_id","setting_id","plan_id"])
            new_rows = new_rows.withColumn("charge_id", row_number().over(win) + max_id)
            new_rows.write.format("delta").mode("append").save(existing_path)
    except Exception:
        win = Window.orderBy(fact_keys)
        initial = raw_fact.dropDuplicates(fact_keys).withColumn("charge_id", row_number().over(win))
        initial.write.format("delta").mode("overwrite").save(existing_path)

    print(f"✅ Completed processing for: {hospital_name}")


# COMMAND ----------

hospital_list = [
    # "Contra_Costa_Regional_Medical_Center",
    # "Priscilla_Chan_Mark_Zuckerberg_San_Francisco_General_Hospital_Trauma_Center",
    # "Scripps_Green_Hospital",
    # "Scripps_Memorial_Hospital_Encinitas",
    # "Scripps_Memorial_Hospital_La_Jolla",
    # "Scripps_Mercy_Hospital_Chula_Vista",
    # "Scripps_Mercy_Hospital_San_Diego",
    # "Sharp_Chula_Vista_Medical_Center",
    # "Sharp_Coronado_Hospital",
    # "Sharp_Grossmont_Hospital",
    # "Sharp_Memorial_Hospital"

    # "Kaiser_Foundation_Hospital_Walnut_Creek",
    # "Kaiser_Foundation_Hospital_Vallejo",
    # "Mercy_General_Hospital",
    # "Mercy_Hospital_of_Folsom",
    # "Mercy_San_Juan_Medical_Center",
    # "Methodist_Hospital_of_Sacramento",
    # "Saint_Francis_Memorial_Hospital",
    # "San_Ramon_Regional_Medical_Center",
    # "Sequoia_Hospital",
    # "St.Mary's_Medical_Center",
    # "Woodland_Memorial_Hospital",
    "John_Muir_Health_Walnut_Creek_Medical_Center",
    "John_Muir_Medical_Center_Concord_Campus"

]

for h in hospital_list:
    process_hospital_data(h)


# COMMAND ----------

