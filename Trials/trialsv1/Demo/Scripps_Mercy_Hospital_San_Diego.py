# Databricks notebook source
# Define storage details
storage_account_name = "projhealthcaresa"
container_name = "silver"
file_path = "Scripps_Mercy_Hospital_Chula_Vista"

# Construct the full path
parquet_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}/"



# COMMAND ----------

df = spark.read.format("parquet").load(parquet_path)

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

df_hospital = df.select("hospital_name", "hospital_location", "last_updated_on").dropDuplicates()
df_hospital = df_hospital.withColumn("hospital_id", monotonically_increasing_id())


# COMMAND ----------

display(df_hospital)

# COMMAND ----------

df_procedure = df.selectExpr("cpt_code as code", "description").dropDuplicates()


# COMMAND ----------

display(df_procedure)

# COMMAND ----------

df_payer = df.select("payer_name").dropDuplicates()
df_payer = df_payer.withColumn("payer_id", monotonically_increasing_id())


# COMMAND ----------

display(df_payer)

# COMMAND ----------

df_plan = df.select("plan_name", "payer_name").dropDuplicates()
df_plan = df_plan.join(df_payer, on="payer_name", how="left") \
                 .withColumn("plan_id", monotonically_increasing_id()) \
                 .select("plan_id", "plan_name", "payer_id")


# COMMAND ----------

display(df_plan)

# COMMAND ----------

df_charge = df.select(
    "hospital_name",
    "cpt_code",
    "setting",
    "standard_charge_gross",
    "standard_charge_discounted_cash",
    "standard_charge_methodology",
    "standard_charge_min",
    "standard_charge_max"
).dropDuplicates()

df_charge = df_charge.join(df_hospital, on="hospital_name", how="left") \
                     .join(df_procedure, df["cpt_code"] == df_procedure["code"], how="left") \
                     .withColumn("charge_id", monotonically_increasing_id()) \
                     .select("charge_id", "hospital_id", "code",
                             "standard_charge_gross", "standard_charge_discounted_cash",
                             "standard_charge_methodology", "standard_charge_min", "standard_charge_max")


# COMMAND ----------

display(df_charge)

# COMMAND ----------

df_negotiated = df.select(
    "plan_name", "cpt_code",
    "standard_charge_negotiated_dollar",
    "standard_charge_negotiated_percentage",
    "standard_charge_negotiated_algorithm",
    "estimated_amount"
).dropDuplicates()

df_negotiated = df_negotiated \
    .join(df_plan, on="plan_name", how="left") \
    .join(df_charge, df["cpt_code"] == df_charge["code"], how="left") \
    .selectExpr("charge_id", "plan_id",
                "standard_charge_negotiated_dollar as negotiated_dollar",
                "standard_charge_negotiated_percentage as negotiated_percentage",
                "standard_charge_negotiated_algorithm as negotiated_algorithm",
                "estimated_amount")


# COMMAND ----------

display(df_negotiated)

# COMMAND ----------

base_path = "abfss://gold@projhealthcaresa.dfs.core.windows.net/gold_warehouse/"

tables = {
    "hospital": df_hospital,
    "procedure_code": df_procedure,
    "payer": df_payer,
    "plan": df_plan,
    "charge": df_charge,
    "negotiated_charge": df_negotiated
}


# COMMAND ----------

for name, df in tables.items():
    output_path = f"{base_path}{name}/"
    df.write.format("delta").mode("overwrite").save(output_path)


# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS gold_warehouse LOCATION '" + base_path + "'")

for name in tables.keys():
    table_path = f"{base_path}{name}/"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS gold_warehouse.{name}
        USING DELTA
        LOCATION '{table_path}'
    """)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_warehouse.hospital;
# MAGIC SELECT * FROM gold_warehouse.charge;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_warehouse.hospital;
# MAGIC
# MAGIC

# COMMAND ----------

