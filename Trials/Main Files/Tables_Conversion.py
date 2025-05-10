# Databricks notebook source
# HARD CODE YOUR STORAGE KEY HERE (be very careful!)
spark.conf.set(
    "fs.azure.account.key.projhealthcare.dfs.core.windows.net",
    "da0/FIoxp43uIM+x3NLKrS17b6XBds5V+AwU/o+we5+7ohHOUxaUk39NxnyjeuzfM2nqrZTlt+M8+ASt7Cp21A=="
)


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

from pyspark.sql.functions import col, trim, row_number, monotonically_increasing_id
from pyspark.sql.window import Window

# Set up paths
hospital = "Sharp_Memorial_Hospital"
silver_path = f"abfss://stage@projhealthcare.dfs.core.windows.net/{hospital}/"
gold_base_path = "abfss://gold@projhealthcare.dfs.core.windows.net"

# Load and clean raw hospital data
df_raw = spark.read.parquet(silver_path)
df = df_raw.select([trim(col(c)).alias(c) for c in df_raw.columns])

# ─────────────────────── DIMENSION TABLES ───────────────────────

# 1. Hospital
dim_hospital = df.select("hospital_name", "hospital_location", "last_updated_on").dropDuplicates()
existing_hospitals_path = f"{gold_base_path}/dim_hospital/"
try:
    existing_hospitals = spark.read.format("delta").load(existing_hospitals_path)
    new_hospitals = dim_hospital.join(existing_hospitals, on=["hospital_name"], how="left_anti")
    if new_hospitals.count() > 0:
        max_id = existing_hospitals.agg({"hospital_id": "max"}).collect()[0][0] or 0
        new_hospitals = new_hospitals.withColumn("hospital_id", row_number().over(Window.orderBy("hospital_name")) + max_id)
        new_hospitals.write.format("delta").mode("append").save(existing_hospitals_path)
except:
    dim_hospital = dim_hospital.withColumn("hospital_id", row_number().over(Window.orderBy("hospital_name")))
    dim_hospital.write.format("delta").mode("overwrite").save(existing_hospitals_path)

# 2. Procedure Code
dim_procedure = df.select("cpt_code", "description").dropDuplicates()
dim_procedure.write.format("delta").mode("overwrite").save(f"{gold_base_path}/dim_procedure_code/")

# 3. Payer
dim_payer = df.select("payer_name").dropDuplicates()
existing_payers_path = f"{gold_base_path}/dim_payer"
try:
    existing_payers = spark.read.format("delta").load(existing_payers_path)
    new_payers = dim_payer.join(existing_payers, on="payer_name", how="left_anti")
    if new_payers.count() > 0:
        max_id = existing_payers.agg({"payer_id": "max"}).collect()[0][0] or 0
        new_payers = new_payers.withColumn("payer_id", row_number().over(Window.orderBy("payer_name")) + max_id)
        new_payers.write.format("delta").mode("append").save(existing_payers_path)
except:
    dim_payer = dim_payer.withColumn("payer_id", row_number().over(Window.orderBy("payer_name")))
    dim_payer.write.format("delta").mode("overwrite").save(existing_payers_path)

# 4. Plan
dim_plan = df.select("plan_name", "payer_name").dropDuplicates()
payer_df = spark.read.format("delta").load(existing_payers_path)
dim_plan = dim_plan.join(payer_df, on="payer_name", how="left")
dim_plan = dim_plan.withColumn("plan_id", monotonically_increasing_id())
dim_plan.write.format("delta").mode("overwrite").save(f"{gold_base_path}/dim_plan/")

# 5. Setting
dim_setting = df.select("setting").dropDuplicates()
dim_setting = dim_setting.withColumn("setting_id", monotonically_increasing_id())
dim_setting.write.format("delta").mode("overwrite").save(f"{gold_base_path}/dim_setting/")

# ─────────────────────── FACT TABLES ───────────────────────

# 6. Charge
fact_charge = df.select(
    "hospital_name", "cpt_code", "setting",
    "standard_charge_gross", "standard_charge_discounted_cash",
    "standard_charge_methodology", "standard_charge_min", "standard_charge_max"
).dropDuplicates()

dim_hospital = spark.read.format("delta").load(f"{gold_base_path}/dim_hospital/")
dim_setting = spark.read.format("delta").load(f"{gold_base_path}/dim_setting/")

fact_charge = fact_charge.join(dim_hospital, on="hospital_name", how="left") \
                         .join(dim_setting, on="setting", how="left")
fact_charge = fact_charge.withColumn("charge_id", monotonically_increasing_id())
fact_charge.write.format("delta").mode("overwrite").save(f"{gold_base_path}/fact_charge/")

# 7. Negotiated Charge
plan_df = spark.read.format("delta").load(f"{gold_base_path}/dim_plan/")
fact_charge_df = spark.read.format("delta").load(f"{gold_base_path}/fact_charge/")

negotiated_charge = df.select(
    "cpt_code", "plan_name",
    "standard_charge_negotiated_dollar", "standard_charge_negotiated_percentage",
    "standard_charge_negotiated_algorithm", "estimated_amount"
).dropDuplicates()

negotiated_charge = negotiated_charge \
    .join(fact_charge_df.select("cpt_code", "hospital_id", "charge_id"), on="cpt_code", how="left") \
    .join(plan_df.select("plan_name", "plan_id"), on="plan_name", how="left")

negotiated_charge = negotiated_charge.select(
    "charge_id", "plan_id",
    "standard_charge_negotiated_dollar", "standard_charge_negotiated_percentage",
    "standard_charge_negotiated_algorithm", "estimated_amount"
)

negotiated_charge.write.format("delta").mode("overwrite").save(f"{gold_base_path}/fact_negotiated_charge/")


# COMMAND ----------

# Set input/output paths
# hospital = "Sharp_Coronado_Hospital"
silver_path = f"abfss://gold@projhealthcare.dfs.core.windows.net/fact_charge/"
gold_base_path = "abfss://stage@projhealthcare.dfs.core.windows.net/Sharp_Memorial_Hospital/"

# Load Parquet
df = spark.read.format("delta").load(silver_path)

# Show sample
df.display()
df.schema

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import col, row_number, monotonically_increasing_id
from pyspark.sql.window import Window

def process_hospital_data(hospital_name):
    silver_path = f"abfss://stage@projhealthcare.dfs.core.windows.net/{hospital_name}/"
    gold_base_path = "abfss://gold@projhealthcare.dfs.core.windows.net"

    # Load cleaned data
    df = spark.read.parquet(silver_path)

    # ─────────────── DIMENSION TABLES ───────────────

    # 1. Hospital
    dim_hospital = df.select("hospital_name", "hospital_location", "last_updated_on").dropDuplicates()
    existing_hospitals_path = f"{gold_base_path}/dim_hospital/"
    try:
        existing_hospitals = spark.read.format("delta").load(existing_hospitals_path)
        new_hospitals = dim_hospital.join(existing_hospitals, on=["hospital_name"], how="left_anti")
        if new_hospitals.count() > 0:
            max_id = existing_hospitals.agg({"hospital_id": "max"}).collect()[0][0] or 0
            new_hospitals = new_hospitals.withColumn("hospital_id", row_number().over(Window.orderBy("hospital_name")) + max_id)
            new_hospitals.write.format("delta").mode("append").save(existing_hospitals_path)
    except:
        dim_hospital = dim_hospital.withColumn("hospital_id", row_number().over(Window.orderBy("hospital_name")))
        dim_hospital.write.format("delta").mode("overwrite").save(existing_hospitals_path)

    # 2. Procedure Code
    dim_procedure = df.select("cpt_code", "description").dropDuplicates()
    dim_procedure.write.format("delta").mode("overwrite").save(f"{gold_base_path}/dim_procedure_code/")

    # 3. Payer
    dim_payer = df.select("payer_name").dropDuplicates()
    existing_payers_path = f"{gold_base_path}/dim_payer"
    try:
        existing_payers = spark.read.format("delta").load(existing_payers_path)
        new_payers = dim_payer.join(existing_payers, on="payer_name", how="left_anti")
        if new_payers.count() > 0:
            max_id = existing_payers.agg({"payer_id": "max"}).collect()[0][0] or 0
            new_payers = new_payers.withColumn("payer_id", row_number().over(Window.orderBy("payer_name")) + max_id)
            new_payers.write.format("delta").mode("append").save(existing_payers_path)
    except:
        dim_payer = dim_payer.withColumn("payer_id", row_number().over(Window.orderBy("payer_name")))
        dim_payer.write.format("delta").mode("overwrite").save(existing_payers_path)

    # 4. Plan
    dim_plan = df.select("plan_name", "payer_name").dropDuplicates()
    payer_df = spark.read.format("delta").load(existing_payers_path)
    dim_plan = dim_plan.join(payer_df, on="payer_name", how="left")
    dim_plan = dim_plan.withColumn("plan_id", monotonically_increasing_id())
    dim_plan.write.format("delta").mode("overwrite").save(f"{gold_base_path}/dim_plan/")

    # 5. Setting
    dim_setting = df.select("setting").dropDuplicates()
    dim_setting = dim_setting.withColumn("setting_id", monotonically_increasing_id())
    dim_setting.write.format("delta").mode("overwrite").save(f"{gold_base_path}/dim_setting/")

    # ─────────────── FACT TABLES ───────────────

    # 6. Charge
    fact_charge = df.select(
        "hospital_name", "cpt_code", "setting",
        "standard_charge_gross", "standard_charge_discounted_cash",
        "standard_charge_methodology", "standard_charge_min", "standard_charge_max"
    ).dropDuplicates()

    dim_hospital = spark.read.format("delta").load(f"{gold_base_path}/dim_hospital/")
    dim_setting = spark.read.format("delta").load(f"{gold_base_path}/dim_setting/")

    fact_charge = fact_charge.join(dim_hospital, on="hospital_name", how="left") \
                             .join(dim_setting, on="setting", how="left")
    fact_charge = fact_charge.withColumn("charge_id", monotonically_increasing_id())
    fact_charge.write.format("delta").mode("overwrite").save(f"{gold_base_path}/fact_charge/")

    # 7. Negotiated Charge
    plan_df = spark.read.format("delta").load(f"{gold_base_path}/dim_plan/")
    fact_charge_df = spark.read.format("delta").load(f"{gold_base_path}/fact_charge/")

    negotiated_charge = df.select(
        "cpt_code", "plan_name",
        "standard_charge_negotiated_dollar", "standard_charge_negotiated_percentage",
        "standard_charge_negotiated_algorithm", "estimated_amount"
    ).dropDuplicates()

    negotiated_charge = negotiated_charge \
        .join(fact_charge_df.select("cpt_code", "hospital_id", "charge_id"), on="cpt_code", how="left") \
        .join(plan_df.select("plan_name", "plan_id"), on="plan_name", how="left")

    negotiated_charge = negotiated_charge.select(
        "charge_id", "plan_id",
        "standard_charge_negotiated_dollar", "standard_charge_negotiated_percentage",
        "standard_charge_negotiated_algorithm", "estimated_amount"
    )

    negotiated_charge.write.format("delta").mode("overwrite").save(f"{gold_base_path}/fact_negotiated_charge/")

    print(f"✅ Completed processing for: {hospital_name}")


# COMMAND ----------

process_hospital_data("Sharp_Memorial_Hospital")


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
]

for h in hospital_list:
    process_hospital_data(h)


# COMMAND ----------

