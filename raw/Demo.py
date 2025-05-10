# Databricks notebook source
# Define storage details
storage_account_name = "projhealthcaresa"
container_name = "demo"
file_path = "ucsf-medical-center.json"

# Construct the full path
json_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}"

# Read JSON file
# df = spark.read.option("multiline", "true").json(json_path)
df_raw = spark.read.option("multiline", "true").option("InferSchema", "false").json(json_path)

# Display schema and sample data
df_raw.show(5)
# df.show(5, truncate=False)


# COMMAND ----------

from pyspark.sql.functions import explode, col, lit

df_exploded = df_raw.select(
    "hospital_name",
    "last_updated_on",
    "version",
    explode("standard_charge_information").alias("sci")
)


# COMMAND ----------

df_flat = df_exploded.select(
    "hospital_name",
    "last_updated_on",
    "version",
    col("sci.description").alias("description"),
    col("sci.drug_information.type").alias("drug_type"),
    col("sci.drug_information.unit").alias("drug_unit"),
    explode("sci.code_information").alias("code_info"),
    explode("sci.standard_charges").alias("charges")
)


# COMMAND ----------

df_normalized = df_flat.select(
    "hospital_name",
    "last_updated_on",
    "version",
    "description",
    "drug_type",
    "drug_unit",
    col("code_info.code").alias("code"),
    col("code_info.type").alias("code_type"),
    col("charges.gross_charge"),
    col("charges.discounted_cash"),
    col("charges.minimum"),
    col("charges.maximum"),
    col("charges.setting"),
    col("charges.billing_class"),
    col("charges.additional_generic_notes")
)


# COMMAND ----------

df_with_payers = df_flat.select(
    "hospital_name",
    "last_updated_on",
    "version",
    "description",
    "drug_type",
    "drug_unit",
    col("code_info.code").alias("code"),
    col("code_info.type").alias("code_type"),
    col("charges.setting").alias("setting"),
    explode("charges.payers_information").alias("payer")
).select(
    "hospital_name",
    "last_updated_on",
    "version",
    "description",
    "drug_type",
    "drug_unit",
    "code",
    "code_type",
    "setting",
    col("payer.payer_name"),
    col("payer.plan_name"),
    col("payer.standard_charge_algorithm"),
    col("payer.standard_charge_percentage"),
    # col("payer.standard_charge_dollar"),
    col("payer.estimated_amount"),
    col("payer.methodology"),
    col("payer.additional_payer_notes")
)


# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import explode, lit

# Flatten the array of standard charges
df_exploded = df_raw.select(
    lit(df_raw.hospital_name).alias("hospital_name"),
    lit(df_raw.last_updated_on).alias("last_updated_on"),
    lit(df_raw.hospital_location[0]).alias("hospital_location"),
    explode(df_raw.standard_charge_information).alias("item")
)


# COMMAND ----------

df_flat = df_exploded.select(
    "hospital_name",
    "last_updated_on",
    "hospital_location",
    "item.description",
    "item.drug_information.unit",
    "item.drug_information.type",
    explode("item.code_information").alias("code_info"),
    explode("item.standard_charges").alias("charges")
)


# COMMAND ----------

df_final = df_flat.select(
    "hospital_name",
    "last_updated_on",
    "hospital_location",
    "description",
    "unit",
    "type",
    "code_info.code",
    "code_info.type",
    "charges.minimum",
    "charges.maximum",
    "charges.gross_charge",
    "charges.discounted_cash",
    "charges.setting",
    "charges.additional_generic_notes"
)


# COMMAND ----------

df_with_payers = df_flat.select(
    "hospital_name",
    "last_updated_on",
    "hospital_location",
    "description",
    "unit",
    "type",
    "code_info.code",
    "code_info.type",
    "charges.setting",
    explode("charges.payers_information").alias("payer")
).select(
    "hospital_name",
    "last_updated_on",
    "hospital_location",
    # "description",
    "unit",
    # "code_info.type",
    # "code",
    # "code_info.type",
    "setting",
    "payer.payer_name",
    "payer.plan_name",
    # "payer.standard_charge_dollar",
    "payer.standard_charge_percentage",
    "payer.standard_charge_algorithm",
    "payer.estimated_amount",
    "payer.methodology"
)

# COMMAND ----------

df_modifiers = df_raw.select(
    "hospital_name",
    "last_updated_on",
    explode("modifier_information").alias("modifier")
).select(
    "hospital_name",
    "last_updated_on",
    col("modifier.code"),
    col("modifier.description"),
    explode("modifier.modifier_payer_information").alias("mod_payer")
).select(
    "hospital_name",
    "last_updated_on",
    "code",
    "description",
    col("mod_payer.payer_name"),
    col("mod_payer.plan_name"),
    col("mod_payer.description").alias("mod_description")
)


# COMMAND ----------



# COMMAND ----------

display(df_with_payers)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import col, explode

df_flat = df \
    .withColumn("hospital_location", explode("hospital_location")) \
    .withColumn("hospital_address", explode("hospital_address")) \
    .select(
        "hospital_name",
        "last_updated_on",
        "version",
        "hospital_location",
        "hospital_address",
        col("affirmation.affirmation").alias("affirmation_text"),
        col("affirmation.confirm_affirmation").alias("affirmation_confirmed"),
        col("license_information.license_number").alias("license_number"),
        col("license_information.state").alias("license_state"),
        "financial_aid_policy"  # optional
    )


# COMMAND ----------

df_modifier = df \
    .withColumn("modifier", explode("modifier_information")) \
    .withColumn("modifier_payer", explode("modifier.modifier_payer_information")) \
    .select(
        "hospital_name",
        col("modifier.code").alias("modifier_code"),
        col("modifier.description").alias("modifier_description"),
        col("modifier_payer.payer_name").alias("payer_name"),
        col("modifier_payer.plan_name").alias("plan_name"),
        col("modifier_payer.description").alias("payer_modifier_description")
    )


# COMMAND ----------

from pyspark.sql.functions import col

df_charge = df \
    .withColumn("charge", explode("standard_charge_information")) \
    .withColumn("charge_code", explode("charge.code_information")) \
    .withColumn("standard", explode("charge.standard_charges")) \
    .withColumn("payer_info", explode("standard.payers_information")) \
    .select(
        "hospital_name",
        col("charge.description").alias("charge_description"),
        col("charge.drug_information.unit").alias("drug_unit"),
        col("charge.drug_information.type").alias("drug_type"),
        col("charge_code.code").alias("code"),
        col("charge_code.type").alias("code_type"),
        col("standard.billing_class").alias("billing_class"),
        col("standard").getField("gross_charge").cast("double").alias("gross_charge"),
        col("standard").getField("discounted_cash").cast("double").alias("discounted_cash"),
        col("standard.minimum").cast("double").alias("min_charge"),
        col("standard.maximum").cast("double").alias("max_charge"),
        col("standard.setting").alias("setting"),
        col("standard.additional_generic_notes").alias("notes"),
        col("payer_info.payer_name"),
        col("payer_info.plan_name"),
        col("payer_info.estimated_amount").cast("double"),
        col("payer_info.standard_charge_percentage").cast("double"),
        col("payer_info.standard_charge_algorithm"),
        col("payer_info.methodology")
    )


# COMMAND ----------

df_final = df_flat.join(df_modifier, "hospital_name", "left") \
                  .join(df_charge, "hospital_name", "left")


# COMMAND ----------

# Display the first 10 rows using display()
display(df_final.limit(10))


# COMMAND ----------

unique_count = df_final.select("gross_charge").distinct().count()
print(f"Total unique hospital names: {unique_count}")


# COMMAND ----------

df.selectExpr("explode(standard_charge_information) as charge") \
  .selectExpr("explode(charge.standard_charges) as standard") \
  .select("standard.*") \
  .show(5, truncate=False)


# COMMAND ----------

from pyspark.sql.functions import col, explode

# Read and parse JSON
df = spark.read.option("multiline", True).json("abfss://demo@projhealthcaresa.dfs.core.windows.net/ucsf-medical-center.json")

# Explode and extract standard charges
df_charge = df \
    .withColumn("charge", explode("standard_charge_information")) \
    .withColumn("standard", explode("charge.standard_charges")) \
    .select(
        "hospital_name",
        col("standard.billing_class"),
        col("standard.gross_charge").cast("double").alias("gross_charge"),
        col("standard.discounted_cash").cast("double").alias("discounted_cash"),
        col("standard.setting"),
        col("standard.additional_generic_notes").alias("notes")
    )

df_charge.filter("gross_charge IS NOT NULL").show(truncate=False)


# COMMAND ----------

df_payer = df \
    .withColumn("charge", explode("standard_charge_information")) \
    .withColumn("standard", explode("charge.standard_charges")) \
    .filter(col("standard.payers_information").isNotNull()) \
    .withColumn("payer_info", explode("standard.payers_information")) \
    .select(
        "hospital_name",
        col("payer_info.payer_name"),
        col("payer_info.plan_name"),
        col("payer_info.estimated_amount").cast("double")
    )


# COMMAND ----------

df_exploded = df \
    .withColumn("charge", explode("standard_charge_information")) \
    .withColumn("standard", explode("charge.standard_charges")) \
    .select("standard.*")

df_exploded.select("gross_charge", "discounted_cash").where("gross_charge IS NOT NULL").show()


# COMMAND ----------

from pyspark.sql.functions import col, explode

# Define storage details
storage_account_name = "projhealthcaresa"
container_name = "demo"
file_path = "ucsf-medical-center.json"

# Construct ABFSS path
json_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}"

# Read the JSON
df = spark.read.option("multiline", True).json(json_path)

# -------------------------
# 1. Extract gross & cash charges
# -------------------------
df_charge = df \
    .withColumn("charge", explode("standard_charge_information")) \
    .withColumn("standard", explode("charge.standard_charges")) \
    .select(
        "hospital_name",
        col("standard.billing_class"),
        col("standard.gross_charge").cast("double").alias("gross_charge"),
        col("standard.discounted_cash").cast("double").alias("discounted_cash"),
        col("standard.setting"),
        col("standard.additional_generic_notes").alias("notes")
    )

# -------------------------
# 2. Extract payer information (if exists)
# -------------------------
df_payer = df \
    .withColumn("charge", explode("standard_charge_information")) \
    .withColumn("standard", explode("charge.standard_charges")) \
    .filter(col("standard.payers_information").isNotNull()) \
    .withColumn("payer_info", explode("standard.payers_information")) \
    .select(
        "hospital_name",
        col("payer_info.payer_name"),
        col("payer_info.plan_name"),
        col("payer_info.estimated_amount").cast("double"),
        col("payer_info.standard_charge_percentage").cast("double"),
        col("payer_info.standard_charge_algorithm"),
        col("payer_info.methodology"),
        col("payer_info.additional_payer_notes")
    )

# -------------------------
# Show output
# -------------------------
print("=== Gross & Cash Charges ===")
df_charge.show(truncate=False)

print("=== Payer Info (if any) ===")
df_payer.show(truncate=False)


# COMMAND ----------

from pyspark.sql.functions import col

# Perform join
df_combined = df_charge.join(
    df_payer,
    on="hospital_name",  # You can also add ["hospital_name", "billing_class"] if needed
    how="left"
)

# Inspect combined data
df_combined.show(truncate=False)


# COMMAND ----------

display(df_combined.limit(5))

# COMMAND ----------

from pyspark.sql.functions import explode_outer, col
from pyspark.sql.functions import col, explode

# Define storage details
storage_account_name = "projhealthcaresa"
container_name = "demo"
file_path = "ucsf-medical-center.json"

# Construct ABFSS path
json_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}"

# Step 1: Load the nested JSON file
df = spark.read.option("multiline", "true").json(json_path)

# Step 2: Explode standard_charge_information
flat_df = df.select(explode_outer("standard_charge_information").alias("charge"))

# Step 3: Select required fields and flatten drug + charge structure
final_df = flat_df.select(
    col("charge.description"),
    col("charge.drug_information.unit").alias("unit"),
    col("charge.drug_information.type").alias("drug_type"),
    col("charge.code_information").alias("code_information"),
    col("charge.standard_charges").alias("standard_charges")
)

# Step 4: Explode code_information
step1_df = final_df.select(
    "description",
    "unit",
    "drug_type",
    explode_outer("code_information").alias("code_info"),
    "standard_charges"
)

# Step 5: Explode standard_charges
step2_df = step1_df.select(
    "description",
    "unit",
    "drug_type",
    col("code_info.code").alias("code"),
    col("code_info.type").alias("code_type"),
    explode_outer("standard_charges").alias("price")
)

# Step 6: Final projection
flattened = step2_df.select(
    "description",
    "unit",
    "drug_type",
    "code",
    "code_type",
    "price.gross_charge",
    "price.discounted_cash",
    "price.setting",
    "price.billing_class",
    "price.additional_generic_notes"
)

# Step 7: Display the table
flattened.display()  # Use .show() if not using Databricks notebooks


# COMMAND ----------

from pyspark.sql.functions import explode_outer, col

# Step 1: Define path and load JSON
storage_account_name = "projhealthcaresa"
container_name = "demo"
file_path = "ucsf-medical-center.json"

json_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}"

df = spark.read.option("multiline", "true").json(json_path)

# Step 2: Explode standard_charge_information
flat_df = df \
    .withColumn("charge", explode_outer("standard_charge_information")) \
    .select(
        "hospital_name",
        "last_updated_on",
        "version",
        col("charge.description"),
        col("charge.drug_information.unit").alias("unit"),
        col("charge.drug_information.type").alias("drug_type"),
        col("charge.code_information").alias("code_information"),
        col("charge.standard_charges").alias("standard_charges")
    )

# Step 3: Explode code_information
step1_df = flat_df \
    .withColumn("code_info", explode_outer("code_information")) \
    .select(
        "hospital_name",
        "last_updated_on",
        "version",
        "description",
        "unit",
        "drug_type",
        col("code_info.code").alias("code"),
        col("code_info.type").alias("code_type"),
        "standard_charges"
    )

# Step 4: Explode standard_charges
step2_df = step1_df \
    .withColumn("price", explode_outer("standard_charges")) \
    .select(
        "hospital_name",
        "last_updated_on",
        "version",
        "description",
        "unit",
        "drug_type",
        "code",
        "code_type",
        col("price.gross_charge").cast("double").alias("gross_charge"),
        col("price.discounted_cash").cast("double").alias("discounted_cash"),
        col("price.setting").alias("setting"),
        col("price.billing_class").alias("billing_class"),
        col("price.additional_generic_notes").alias("notes")
    )

# Step 5: Display or return final result
step2_df.display()  # Or use .show(truncate=False)


# COMMAND ----------

from pyspark.sql.functions import explode_outer, col

# Load JSON
storage_account_name = "projhealthcaresa"
container_name = "demo"
file_path = "ucsf-medical-center.json"

json_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}"
df = spark.read.option("multiline", True).json(json_path)

# Step 1: Explode standard_charge_information
df_flat = df \
    .withColumn("charge", explode_outer("standard_charge_information")) \
    .withColumn("code_info", explode_outer("charge.code_information")) \
    .withColumn("standard", explode_outer("charge.standard_charges")) \
    .withColumn("payer_info", explode_outer("standard.payers_information")) \
    .withColumn("financial_policy", explode_outer("financial_aid_policy")) \
    .withColumn("address", explode_outer("hospital_address")) \
    .withColumn("location", explode_outer("hospital_location")) \
    .withColumn("modifier", explode_outer("modifier_information")) \
    .withColumn("modifier_payer", explode_outer("modifier.modifier_payer_information"))

# Step 2: Flatten all fields
final_df = df_flat.select(
    # Top-level info
    "hospital_name",
    "last_updated_on",
    "version",
    
    # Affirmation
    col("affirmation.affirmation").alias("affirmation_text"),
    col("affirmation.confirm_affirmation").alias("affirmation_confirmed"),
    
    # License
    col("license_information.license_number").alias("license_number"),
    col("license_information.state").alias("license_state"),
    
    # Flattened arrays
    col("financial_policy").alias("financial_aid_policy"),
    col("address").alias("hospital_address"),
    col("location").alias("hospital_location"),
    
    # Modifier info
    col("modifier.code").alias("modifier_code"),
    col("modifier.description").alias("modifier_description"),
    col("modifier_payer.payer_name").alias("modifier_payer_name"),
    col("modifier_payer.plan_name").alias("modifier_plan_name"),
    col("modifier_payer.description").alias("modifier_payer_description"),
    
    # Charge-level info
    col("charge.description").alias("charge_description"),
    col("charge.drug_information.unit").alias("drug_unit"),
    col("charge.drug_information.type").alias("drug_type"),
    col("code_info.code").alias("charge_code"),
    col("code_info.type").alias("charge_code_type"),
    
    # Charge breakdown
    col("standard.billing_class"),
    col("standard.gross_charge").cast("double"),
    col("standard.discounted_cash").cast("double"),
    col("standard.minimum").cast("double"),
    col("standard.maximum").cast("double"),
    col("standard.setting"),
    col("standard.additional_generic_notes").alias("charge_notes"),
    col("standard.modifiers").alias("charge_modifiers"),
    
    # Payer info
    col("payer_info.payer_name"),
    col("payer_info.plan_name"),
    col("payer_info.estimated_amount").cast("double"),
    col("payer_info.standard_charge_percentage").cast("double"),
    col("payer_info.standard_charge_algorithm"),
    col("payer_info.methodology"),
    col("payer_info.additional_payer_notes")
)

# Display full result
# final_df.display()  # or final_df.show(truncate=False)    


# COMMAND ----------

final_df.limit(100).show(truncate=False)


# COMMAND ----------

 df_parquet = spark.read.parquet(output_path)

# Show sample rows
df_parquet.show(truncate=False)

# COMMAND ----------

