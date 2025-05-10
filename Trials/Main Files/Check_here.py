# Databricks notebook source
# HARD CODE YOUR STORAGE KEY HERE (be very careful!)
spark.conf.set(
    "fs.azure.account.key.projhealthcare.dfs.core.windows.net",
    "da0/FIoxp43uIM+x3NLKrS17b6XBds5V+AwU/o+we5+7ohHOUxaUk39NxnyjeuzfM2nqrZTlt+M8+ASt7Cp21A=="
)


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, monotonically_increasing_id

def process_hospital_data(hospital_name):
    silver_path  = f"abfss://stage@projhealthcare.dfs.core.windows.net/{hospital_name}/"
    gold_base    = "abfss://demo@projhealthcare.dfs.core.windows.net"

    # 0️⃣ Load cleaned data
    df = spark.read.parquet(silver_path)

    # ─── 1️⃣ DIM_HOSPITAL ─────────────────────────────────────────────────────────────
    # Build staging
    dim_hosp = df.select("hospital_name", "hospital_location", "last_updated_on").dropDuplicates()
    hosp_path = f"{gold_base}/dim_hospital/"

    try:
        existing = spark.read.format("delta").load(hosp_path)
        new_rows = dim_hosp.join(existing, ["hospital_name"], "left_anti")
        if new_rows.count() > 0:
            max_id = existing.agg({"hospital_id": "max"}).collect()[0][0] or 0
            to_append = new_rows.withColumn(
                "hospital_id",
                row_number().over(Window.orderBy("hospital_name")) + max_id
            )
            to_append.write.format("delta").mode("append").save(hosp_path)
    except:
        # first time: generate IDs for all
        with_ids = dim_hosp.withColumn(
            "hospital_id",
            row_number().over(Window.orderBy("hospital_name"))
        )
        with_ids.write.format("delta").mode("overwrite").save(hosp_path)

    # ─── 2️⃣ DIM_PROCEDURE_CODE ────────────────────────────────────────────────────────
    proc_path = f"{gold_base}/dim_procedure_code/"
    dim_proc  = df.select("cpt_code", "description").dropDuplicates()

    try:
        existing = spark.read.format("delta").load(proc_path)
        new_rows = dim_proc.join(existing, ["cpt_code"], "left_anti")
        if new_rows.count() > 0:
            new_rows.write.format("delta").mode("append").save(proc_path)
    except:
        dim_proc.write.format("delta").mode("overwrite").save(proc_path)

    # ─── 3️⃣ DIM_PAYER ──────────────────────────────────────────────────────────────────
    payer_path = f"{gold_base}/dim_payer/"
    dim_payer0 = df.select("payer_name").dropDuplicates()

    try:
        existing = spark.read.format("delta").load(payer_path)
        new_rows = dim_payer0.join(existing, ["payer_name"], "left_anti")
        if new_rows.count() > 0:
            max_id = existing.agg({"payer_id": "max"}).collect()[0][0] or 0
            to_append = new_rows.withColumn(
                "payer_id",
                row_number().over(Window.orderBy("payer_name")) + max_id
            )
            to_append.write.format("delta").mode("append").save(payer_path)
    except:
        init = dim_payer0.withColumn(
            "payer_id",
            row_number().over(Window.orderBy("payer_name"))
        )
        init.write.format("delta").mode("overwrite").save(payer_path)

    # ─── 4️⃣ DIM_PLAN ───────────────────────────────────────────────────────────────────
    plan_path = f"{gold_base}/dim_plan/"
    # need payer_id to join
    payer_df = spark.read.format("delta").load(payer_path)
    dim_plan0 = (
        df.select("plan_name", "payer_name")
          .dropDuplicates()
          .join(payer_df, "payer_name", "left")
          .select("plan_name", "payer_id")
    )

    try:
        existing = spark.read.format("delta").load(plan_path)
        new_rows = dim_plan0.join(existing, ["plan_name","payer_id"], "left_anti")
        if new_rows.count() > 0:
            max_id = existing.agg({"plan_id": "max"}).collect()[0][0] or 0
            to_append = new_rows.withColumn(
                "plan_id",
                monotonically_increasing_id() + max_id + 1
            )
            to_append.write.format("delta").mode("append").save(plan_path)
    except:
        init = dim_plan0.withColumn("plan_id", monotonically_increasing_id())
        init.write.format("delta").mode("overwrite").save(plan_path)

    # ─── 5️⃣ DIM_SETTING ───────────────────────────────────────────────────────────────
    setting_path = f"{gold_base}/dim_setting/"
    dim_set0     = df.select("setting").dropDuplicates()

    try:
        existing = spark.read.format("delta").load(setting_path)
        new_rows = dim_set0.join(existing, ["setting"], "left_anti")
        if new_rows.count() > 0:
            max_id = existing.agg({"setting_id": "max"}).collect()[0][0] or 0
            to_append = new_rows.withColumn(
                "setting_id",
                monotonically_increasing_id() + max_id + 1
            )
            to_append.write.format("delta").mode("append").save(setting_path)
    except:
        init = dim_set0.withColumn("setting_id", monotonically_increasing_id())
        init.write.format("delta").mode("overwrite").save(setting_path)



    # ─── 6️⃣ FACT_CHARGE ───────────────────────────────────────────────────────────────
    fact_charge_path = f"{gold_base}/fact_charge/"
    staging_charge = (
        df.select(
            "hospital_name", "cpt_code", "setting",
            "standard_charge_gross", "standard_charge_discounted_cash",
            "standard_charge_methodology",
            "standard_charge_min", "standard_charge_max"
        )
        .dropDuplicates()
        .join(spark.read.format("delta").load(hosp_path),    "hospital_name", "left")
        .join(spark.read.format("delta").load(setting_path),"setting"      , "left")
        .select(
            "hospital_id","cpt_code","setting_id",
            "standard_charge_gross","standard_charge_discounted_cash",
            "standard_charge_methodology",
            "standard_charge_min","standard_charge_max"
        )
    )

    try:
        existing = spark.read.format("delta").load(fact_charge_path)
        # 1️⃣ get the current max charge_id (or 0 if none)
        max_id = existing.agg(F.max("charge_id")).collect()[0][0] or 0

        # 2️⃣ find only brand-new rows
        join_cols = [
            "hospital_id","cpt_code","setting_id",
            "standard_charge_gross","standard_charge_discounted_cash",
            "standard_charge_methodology",
            "standard_charge_min","standard_charge_max"
        ]
        new_rows = staging_charge.join(existing, join_cols, "left_anti")

        if new_rows.count() > 0:
            # 3️⃣ assign NEW IDs = ROW_NUMBER() + max_id
            to_append = new_rows.withColumn(
                "charge_id",
                row_number()
                  .over(Window.orderBy("hospital_id","cpt_code","setting_id"))
                + F.lit(max_id)
            )
            to_append.write.format("delta").mode("append").save(fact_charge_path)

    except:
        # first time only: just assign sequential IDs
        to_write = staging_charge.withColumn(
            "charge_id",
            row_number().over(Window.orderBy("hospital_id","cpt_code","setting_id"))
        )
        to_write.write.format("delta").mode("overwrite").save(fact_charge_path)


    # ─── 7️⃣ FACT_NEGOTIATED_CHARGE ────────────────────────────────────────────────────
    neg_path     = f"{gold_base}/fact_negotiated_charge/"
    staging_neg  = (
        df.select(
            "cpt_code","plan_name",
            "standard_charge_negotiated_dollar","standard_charge_negotiated_percentage",
            "standard_charge_negotiated_algorithm","estimated_amount"
        )
        .dropDuplicates()
        .join(spark.read.format("delta").load(plan_path)       , "plan_name", "left")
        .join(spark.read.format("delta").load(fact_charge_path), "cpt_code", "left")
        .select(
            "charge_id","plan_id",
            "standard_charge_negotiated_dollar","standard_charge_negotiated_percentage",
            "standard_charge_negotiated_algorithm","estimated_amount"
        )
    )

    try:
        existing = spark.read.format("delta").load(neg_path)
        join_cols = ["charge_id","plan_id","standard_charge_negotiated_dollar","standard_charge_negotiated_percentage","standard_charge_negotiated_algorithm","estimated_amount"]
        new_rows = staging_neg.join(existing, join_cols, "left_anti")
        if new_rows.count() > 0:
            new_rows.write.format("delta").mode("append").save(neg_path)
    except:
        staging_neg.write.format("delta").mode("overwrite").save(neg_path)

    print(f"✅ Completed processing for: {hospital_name}")


# COMMAND ----------

process_hospital_data("Contra_Costa_Regional_Medical_Center")


# COMMAND ----------

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

# COMMAND ----------

# Set input/output paths
# hospital = "Sharp_Coronado_Hospital"
silver_path = f"abfss://demo@projhealthcare.dfs.core.windows.net/dim_code/"
# gold_base_path = "abfss://stage@projhealthcare.dfs.core.windows.net/Sharp_Memorial_Hospital/"

# Load Parquet
df = spark.read.format("delta").load(silver_path)

# Show sample
df.display()
# df.schema

# COMMAND ----------

# Set input/output paths
# hospital = "Sharp_Coronado_Hospital"
silver_path = f"abfss://demo@projhealthcare.dfs.core.windows.net/fact_charge/"
# gold_base_path = "abfss://stage@projhealthcare.dfs.core.windows.net/Sharp_Memorial_Hospital/"

# Load Parquet
df = spark.read.format("delta").load(silver_path)

# Show sample
df.display()
# df.schema

# COMMAND ----------

# Set input/output paths
# hospital = "Sharp_Coronado_Hospital"
silver_path = f"abfss://silver@projhealthcare.dfs.core.windows.net/Contra_Costa_Regional_Medical_Center/"
# gold_base_path = "abfss://stage@projhealthcare.dfs.core.windows.net/Sharp_Memorial_Hospital/"

# Load Parquet
df = spark.read.format("parquet").load(silver_path)

# Show sample
display(df)
# df.schema

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import col
demo = df.filter(col("cpt_code") == "11422")
display(demo)


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# 1️⃣ Load all tables from your Gold layer
dim_hospital          = spark.read.format("delta").load("abfss://demo@projhealthcare.dfs.core.windows.net/dim_hospital/")
dim_payer             = spark.read.format("delta").load("abfss://demo@projhealthcare.dfs.core.windows.net/dim_payer/")
dim_plan              = spark.read.format("delta").load("abfss://demo@projhealthcare.dfs.core.windows.net/dim_plan/")
dim_procedure_code    = spark.read.format("delta").load("abfss://demo@projhealthcare.dfs.core.windows.net/dim_procedure_code/")
dim_setting           = spark.read.format("delta").load("abfss://demo@projhealthcare.dfs.core.windows.net/dim_setting/")
fact_charge           = spark.read.format("delta").load("abfss://demo@projhealthcare.dfs.core.windows.net/fact_charge/")
fact_negotiated_charge= spark.read.format("delta").load("abfss://demo@projhealthcare.dfs.core.windows.net/fact_negotiated_charge/")


# 2️⃣ Join them in the logical order of your star schema
main_df = (
    fact_negotiated_charge
      # bring in charge → hospital, procedure, setting
      .join(fact_charge.select("charge_id","hospital_id","cpt_code","setting_id"), "charge_id", "left")
      .join(dim_hospital.select("hospital_id","hospital_name","hospital_location","last_updated_on"), "hospital_id", "left")
      .join(dim_procedure_code.select("cpt_code","description"), "cpt_code", "left")
      .join(dim_setting.select("setting_id","setting"), "setting_id", "left")
      .join(dim_plan.select("plan_id","plan_name","payer_id"), "plan_id", "left")
      .join(dim_payer.select("payer_id","payer_name"), "payer_id", "left")
)

# 3️⃣ Inspect the result
main_df.printSchema()
main_df.show(10, truncate=False)


# COMMAND ----------

display(main_df)

# COMMAND ----------

