# Databricks notebook source
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import *

# — 1) GLOBAL CONFIG (run once) —
spark.conf.set(
    "fs.azure.account.key.projhealthcare.dfs.core.windows.net",
    "da0/FIoxp43uIM+x3NLKrS17b6XBds5V+AwU/o+we5+7ohHOUxaUk39NxnyjeuzfM2nqrZTlt+M8+ASt7Cp21A=="
)
stage_base       = "abfss://stage@projhealthcare.dfs.core.windows.net"
demo_base        = "abfss://demo@projhealthcare.dfs.core.windows.net"

dim_paths = {
    "hospital":  f"{demo_base}/dim_hospital",
    "setting":   f"{demo_base}/dim_setting",
    "procedure": f"{demo_base}/dim_procedure_code",
    "payer":     f"{demo_base}/dim_payer",
    "plan":      f"{demo_base}/dim_plan",
}

fact_charge_path = f"{demo_base}/fact_charge"
fact_neg_path    = f"{demo_base}/fact_negotiated_charge"


# — 2) GENERIC DIM UPSERT —————————————————————————————————————————————
def upsert_dimension(df_src, natural_keys, attrs, dim_name):
    path = dim_paths[dim_name]
    try:
        df_dim = spark.read.format("delta").load(path)
    except:
        schema = StructType(
            [StructField(c, StringType(), True) for c in natural_keys + attrs] +
            [StructField(f"{dim_name}_id", LongType(), True)]
        )
        df_dim = spark.createDataFrame([], schema)

    df_new = (df_src.select(*(natural_keys + attrs)).distinct()
                 .join(df_dim, natural_keys, "left_anti"))
    if df_new.count() > 0:
        max_id = df_dim.agg({f"{dim_name}_id":"max"}).collect()[0][0] or 0
        w = Window.orderBy(*natural_keys)
        df_new = df_new.withColumn(
            f"{dim_name}_id",
            row_number().over(w) + max_id
        )
        df_new.write.format("delta").mode("append").save(path)
        df_dim = spark.read.format("delta").load(path)

    return df_dim


# — 3) MAIN PROCESSING FUNCTION ——————————————————————————————————————
def process_hospital_all(hospital_name: str):
    # a) read cleaned Parquet for this hospital
    df = spark.read.parquet(f"{stage_base}/{hospital_name}")

    # b) build all dimensions
    dim_hospital = upsert_dimension(
        df.select(
            col("hospital_name"),
            col("hospital_location"),
            col("last_updated_on")
        ).distinct(),
        natural_keys=["hospital_name"],
        attrs=["hospital_location","last_updated_on"],
        dim_name="hospital"
    )

    dim_setting = upsert_dimension(
        df.select(col("setting")).distinct(),
        natural_keys=["setting"],
        attrs=[],
        dim_name="setting"
    )

    dim_procedure = upsert_dimension(
        df.select(col("cpt_code"), col("description")).distinct(),
        natural_keys=["cpt_code","description"],
        attrs=[],
        dim_name="procedure"
    )

    dim_payer = upsert_dimension(
        df.select(col("payer_name")).distinct(),
        natural_keys=["payer_name"],
        attrs=[],
        dim_name="payer"
    )

    df_plan_src = (
        df.select(col("plan_name"), col("payer_name")).distinct()
          .join(dim_payer.select("payer_name","payer_id"), on="payer_name")
    )
    dim_plan = upsert_dimension(
        df_plan_src,
        natural_keys=["plan_name","payer_id"],
        attrs=[],
        dim_name="plan"
    )

    # c) enrich your raw data with surrogate keys
    df_fact = (
        df.join(dim_hospital.select("hospital_name","hospital_id"), "hospital_name")
          .join(dim_setting.select("setting","setting_id"),     "setting")
          .join(dim_procedure.select("cpt_code","description","procedure_id"),
                ["cpt_code","description"])
          .join(dim_payer.select("payer_name","payer_id"),     "payer_name")
          .join(dim_plan.select("plan_name","payer_id","plan_id"),
                ["plan_name","payer_id"])
    )

    # d) build & append fact_charge
    fc_cols = [
        "hospital_id","setting_id","procedure_id",
        "standard_charge_gross","standard_charge_discounted_cash",
        "standard_charge_methodology","standard_charge_min","standard_charge_max"
    ]
    df_fc_new = df_fact.select(*fc_cols).distinct()
    try:
        df_fc_exist = spark.read.format("delta").load(fact_charge_path)
    except:
        schema = StructType([StructField(c, LongType(), True) for c in fc_cols]
                            + [StructField("charge_id", LongType(), True)])
        df_fc_exist = spark.createDataFrame([], schema)

    df_fc_to_app = (
        df_fc_new.join(df_fc_exist.drop("charge_id"), fc_cols, "left_anti")
    )
    if df_fc_to_app.count() > 0:
        max_cid = df_fc_exist.agg({"charge_id":"max"}).collect()[0][0] or 0
        w = Window.orderBy(*fc_cols)
        df_fc_to_app = df_fc_to_app.withColumn(
            "charge_id", row_number().over(w) + max_cid
        )
        df_fc_to_app.write\
            .format("delta")\
            .mode("append")\
            .option("mergeSchema","true")\
            .save(fact_charge_path)

    # e) build & append fact_negotiated_charge
    fn_cols = [
        "charge_id","plan_id",
        "standard_charge_negotiated_dollar",
        "standard_charge_negotiated_percentage",
        "standard_charge_negotiated_algorithm",
        "estimated_amount"
    ]
    # join newly-added charge_id back to df_fact to get the negotiation fields
    df_fn_new = (
        df_fc_to_app.select("procedure_id","charge_id")
          .join(
             df_fact.select(
               "procedure_id","plan_id",
               "standard_charge_negotiated_dollar",
               "standard_charge_negotiated_percentage",
               "standard_charge_negotiated_algorithm",
               "estimated_amount"
             ),
             on="procedure_id"
          )
          .select(*fn_cols)
          .distinct()
    )
    try:
        df_fn_exist = spark.read.format("delta").load(fact_neg_path)
    except:
        schema = StructType(
            [StructField(c, LongType(), True) for c in ["charge_id","plan_id"]]
            + [StructField("standard_charge_negotiated_dollar", DoubleType(), True),
               StructField("standard_charge_negotiated_percentage", DoubleType(), True),
               StructField("standard_charge_negotiated_algorithm", StringType(), True),
               StructField("estimated_amount", DoubleType(), True)]
        )
        df_fn_exist = spark.createDataFrame([], schema)

    df_fn_to_app = (
        df_fn_new.join(df_fn_exist.select("charge_id","plan_id"),
                       ["charge_id","plan_id"], "left_anti")
    )
    if df_fn_to_app.count() > 0:
        df_fn_to_app.write\
            .format("delta")\
            .mode("append")\
            .option("mergeSchema","true")\
            .save(fact_neg_path)


# COMMAND ----------

process_hospital("Priscilla_Chan_Mark_Zuckerberg_San_Francisco_General_Hospital_Trauma_Center")

# COMMAND ----------



# COMMAND ----------

gold_base_path = "abfss://demo@projhealthcare.dfs.core.windows.net/fact_negotiated_charge"
df_demo = spark.read.format("delta").load(gold_base_path)
display(df_demo)

# COMMAND ----------

df_demo.count()

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import *

demo_base        = "abfss://demo@projhealthcare.dfs.core.windows.net"
dim_paths = {
    "hospital":  f"{demo_base}/dim_hospital",
    "setting":   f"{demo_base}/dim_setting",
    "procedure": f"{demo_base}/dim_procedure_code",
    "payer":     f"{demo_base}/dim_payer",
    "plan":      f"{demo_base}/dim_plan",
}
fact_charge_path = f"{demo_base}/fact_charge"
fact_neg_path    = f"{demo_base}/fact_negotiated_charge"
main_path        = f"{demo_base}/fact_main"

# ——— 0) Make sure you run ALL of your process_hospital_all() calls *first* ———
# process_hospital_all("HospitalA")
# process_hospital_all("HospitalB")
# …etc.

# ——— 1) Reload updated dims & facts —————————————
dim_hospital  = spark.read.format("delta").load(dim_paths["hospital"])
dim_setting   = spark.read.format("delta").load(dim_paths["setting"])
dim_procedure = spark.read.format("delta").load(dim_paths["procedure"])
dim_payer     = spark.read.format("delta").load(dim_paths["payer"])
dim_plan      = spark.read.format("delta").load(dim_paths["plan"])

fact_charge = spark.read.format("delta").load(fact_charge_path)
fact_neg    = spark.read.format("delta").load(fact_neg_path)

# ——— 2) Build base_fact as before ———————————————
base_fact = (
    fact_charge.alias("fc")
      .join(fact_neg.alias("fn"), "charge_id", "left")
      .select(
         col("fc.charge_id"), col("fc.hospital_id"), col("fc.setting_id"),
         col("fc.procedure_id"), col("fn.plan_id"),
         col("fc.standard_charge_gross"),
         col("fc.standard_charge_discounted_cash"),
         col("fc.standard_charge_methodology"),
         col("fc.standard_charge_min"), col("fc.standard_charge_max"),
         col("fn.standard_charge_negotiated_dollar"),
         col("fn.standard_charge_negotiated_percentage"),
         col("fn.standard_charge_negotiated_algorithm"),
         col("fn.estimated_amount")
      )
)

# ——— 3) Left-join your dimensions so nothing gets dropped ——
main_df = (
    base_fact
      .join(dim_hospital  .select("hospital_id","hospital_name","hospital_location"),
            on="hospital_id", how="left")
      .join(dim_setting   .select("setting_id","setting"),
            on="setting_id", how="left")
      .join(dim_procedure .select("procedure_id","cpt_code","description"),
            on="procedure_id", how="left")
      .join(dim_plan      .select("plan_id","plan_name","payer_id"),
            on="plan_id", how="left")
      .join(dim_payer     .select("payer_id","payer_name"),
            on="payer_id", how="left")
)

# ——— 4) Write out your full, denormalized “main” table —————
main_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema","true") \
    .save(main_path)


# COMMAND ----------

display(main_df)

# COMMAND ----------

# a) Do you have a plan_id for every charge in fact_charge?
spark.read.format("delta").load(fact_charge_path) \
     .select("plan_id").distinct().show()

# b) Do you have any rows at all in fact_negotiated_charge?
spark.read.format("delta").load(fact_neg_path).show(10)


# COMMAND ----------

from pyspark.sql.functions import col
demo = main_df.filter(col("hospital_id") == 1)
display(demo)

# COMMAND ----------

