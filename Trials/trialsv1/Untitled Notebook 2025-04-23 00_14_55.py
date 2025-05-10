# Databricks notebook source
#–– 1.1 Widgets to parameterize each hospital load
dbutils.widgets.text("file_path",  "",      "silver sub-folder (e.g. Sharp_Coronado_Hospital)")
dbutils.widgets.text("hospital",   "",      "hospital_name (business key)")
dbutils.widgets.text("storage",    "<your-storage-acct-name>", "storage account")
# read them
file_path    = dbutils.widgets.get("file_path")
hospital_key = dbutils.widgets.get("hospital")
sa           = dbutils.widgets.get("storage")

#–– 1.2 Construct your silver path for this hospital
silver_path = f"abfss://silver@{sa}.dfs.core.windows.net/{file_path}/"


# COMMAND ----------

from pyspark.sql.functions import col

silver_df = (spark.read
  .parquet(silver_path)
#   .filter(col("hospital_name") == hospital_key)         # focus on one hospital
#   .dropDuplicates()                                     # in-file dedupe
)


# COMMAND ----------

display(silver_df)

# COMMAND ----------

from delta.tables import DeltaTable

hospitals = silver_df.select(
    col("hospital_name").alias("name"),
    col("hospital_location").alias("location"),
    col("last_updated_on").alias("last_updated_on")
).distinct()

delta_hosp = DeltaTable.forName("default.Hospital") if DeltaTable.isDeltaTable(spark, "default.Hospital") else None

if delta_hosp:
    delta_hosp.alias("t").merge(
      hospitals.alias("s"),
      "t.name = s.name"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    hospitals.write.format("delta").saveAsTable("default.Hospital")


# COMMAND ----------

codes = silver_df.select(
    col("cpt_code").alias("code")
).where(col("code").isNotNull()).distinct()

delta_code = DeltaTable.forName("default.ProcedureCode") if DeltaTable.isDeltaTable(spark, "default.ProcedureCode") else None

if delta_code:
    delta_code.alias("t").merge(
      codes.alias("s"), "t.code = s.code"
    ).whenNotMatchedInsertAll().execute()
else:
    codes.write.format("delta").saveAsTable("default.ProcedureCode")


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col
from delta.tables import DeltaTable

# 1) Pull the distinct payer names
raw_payers = silver_df.select("payer_name").distinct()

# 2) Table name
db, tbl = "default", "Payer"
full_name = f"{db}.{tbl}"

# 3) Check metastore for an existing table
if not spark.catalog.tableExists(db, tbl):
    # INITIAL LOAD: assign surrogate keys 1,2,3… in alphabetical order
    w = Window.orderBy("payer_name")
    dim_payers = raw_payers.withColumn("payer_id", row_number().over(w))
    dim_payers.write.format("delta") \
        .saveAsTable(full_name)
else:
    # INCREMENTAL LOAD: append any brand-new payer_name
    existing = spark.table(full_name)
    new_payers = raw_payers.join(existing, on="payer_name", how="left_anti")
    if new_payers.rdd.isEmpty():
        print("No new payers to add.")
    else:
        max_id = existing.agg({"payer_id":"max"}).collect()[0][0] or 0
        w = Window.orderBy("payer_name")
        to_append = (
          new_payers
            .withColumn("rn", row_number().over(w))
            .withColumn("payer_id", col("rn") + max_id)
            .drop("rn")
        )
        to_append.write.format("delta") \
            .mode("append") \
            .saveAsTable(full_name)


# COMMAND ----------

# join back to dims to get their surrogate keys
h_df = spark.table("default.Hospital")
c_df = spark.table("default.ProcedureCode")
pl_df= spark.table("default.Plan")

charge_fact = (silver_df
  .join(h_df,    silver_df.hospital_name == h_df.name)
  .join(c_df,    silver_df.cpt_code      == c_df.code)
  .join(pl_df,   silver_df.plan_name     == pl_df.plan_name)
  .selectExpr(
    "hospital_id",
    "code as procedure_code",    # keep business key for merge condition
    "plan_id",
    "standard_charge_gross",
    "standard_charge_discounted_cash",
    "standard_charge_negotiated_dollar",
    "standard_charge_negotiated_percentage",
    "negotiated_algorithm",
    "estimated_amount",
    "standard_charge_methodology",
    "standard_charge_min",
    "standard_charge_max",
    "setting",
    "description"
  )
)

delta_charge = DeltaTable.forName("default.Charge") if DeltaTable.isDeltaTable(spark, "default.Charge") else None

if delta_charge:
    delta_charge.alias("t").merge(
      charge_fact.alias("s"),
      "t.hospital_id = s.hospital_id AND t.procedure_code = s.procedure_code AND t.plan_id = s.plan_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    charge_fact.write.format("delta").saveAsTable("default.Charge")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT h.name, c.procedure_code, c.standard_charge_gross, p.plan_name, pay.payer_name
# MAGIC FROM default.Charge c
# MAGIC JOIN default.Hospital h  ON c.hospital_id = h.hospital_id
# MAGIC JOIN default.Plan    p  ON c.plan_id      = p.plan_id
# MAGIC JOIN default.Payer   pay ON p.payer_id      = pay.payer_id;
# MAGIC

# COMMAND ----------

