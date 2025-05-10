# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.projhealthcare.dfs.core.windows.net",
    "da0/FIoxp43uIM+x3NLKrS17b6XBds5V+AwU/o+we5+7ohHOUxaUk39NxnyjeuzfM2nqrZTlt+M8+ASt7Cp21A=="
)

# COMMAND ----------

# Set input/output paths
# hospital = "Sharp_Coronado_Hospital"
silver_path = f"abfss://demo@projhealthcare.dfs.core.windows.net/dim_hospital/"
# Load Parquet
df = spark.read.format("delta").load(silver_path)
display(df)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# ─────────────── Read your Delta tables ───────────────
dim_hosp = spark.read.format("delta") \
    .load("abfss://demo@projhealthcare.dfs.core.windows.net/dim_hospital/") \
    .alias("h")

dim_proc = spark.read.format("delta") \
    .load("abfss://demo@projhealthcare.dfs.core.windows.net/dim_procedure_code/") \
    .alias("pr")

dim_set  = spark.read.format("delta") \
    .load("abfss://demo@projhealthcare.dfs.core.windows.net/dim_setting/") \
    .alias("st")

dim_plan = spark.read.format("delta") \
    .load("abfss://demo@projhealthcare.dfs.core.windows.net/dim_plan/") \
    .alias("pl")

fact = spark.read.format("delta") \
    .load("abfss://demo@projhealthcare.dfs.core.windows.net/fact_charge/") \
    .alias("f")

# ─────────────── Join them to rebuild your “initial” table ───────────────
initial_df = (
    fact
      .join(dim_hosp,
            col("f.hospital_id") == col("h.hospital_id"),
            "inner"
      )
      .join(dim_proc,
            (col("f.code_id")       == col("pr.code_id"))  &
            (col("f.hospital_id")   == col("pr.hospital_id")),
            "inner"
      )
      .join(dim_set,
            col("f.setting_id")  == col("st.setting_id"),
            "inner"
      )
      .join(dim_plan,
            col("f.plan_id")     == col("pl.plan_id"),
            "inner"
      )
      .select(
          col("h.name").alias("hospital_name"),
          col("h.location").alias("hospital_location"),
          col("h.last_updated_on"),
          col("pr.code").alias("cpt_code"),
          col("pr.description"),
          col("st.Setting").alias("setting"),
          col("f.standard_charge_gross"),
          col("f.standard_charge_discounted_cash"),
          col("pl.payer_name"),
          col("pl.plan_name"),
          col("f.standard_charge_negotiated_dollar"),
          col("f.standard_charge_negotiated_percentage"),
          col("f.standard_charge_negotiated_algorithm"),
          col("f.estimated_amount"),
          col("f.standard_charge_methodology"),
          col("f.standard_charge_min"),
          col("f.standard_charge_max")
      )
)

# Preview
# display(initial_df)


# COMMAND ----------

initial_df.select("hospital_name").distinct().show(truncate=False)


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# … (load and join as before into initial_df) …

# Specify which hospital you want:
target_hospital = "KAISER FOUNDATION HOSPITAL & REHAB CENTER - VALLEJO"

# Filter down to just that hospital’s rows:
hospital_df = initial_df \
    .filter(col("hospital_name") == target_hospital)

# Preview:
display(hospital_df)

# Or, if you’d rather overwrite a per-hospital Delta:
# hospital_df.write.format("delta").mode("overwrite") \
#     .save(f"abfss://demo@projhealthcare.dfs.core.windows.net/initial_by_hospital/{target_hospital}/")


# COMMAND ----------

display(fact)

# COMMAND ----------

from pyspark.sql.functions import col
demo = initial_df.filter(col("hospital_name") == "Sharp_Coronado_Hospital")
display(demo)

# COMMAND ----------

