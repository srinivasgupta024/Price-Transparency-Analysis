# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.projhealthcare.dfs.core.windows.net",
    "da0/FIoxp43uIM+x3NLKrS17b6XBds5V+AwU/o+we5+7ohHOUxaUk39NxnyjeuzfM2nqrZTlt+M8+ASt7Cp21A=="
)

# COMMAND ----------

dbutils.widgets.text("hospital_name", "")
hospital_name = dbutils.widgets.get("hospital_name")
print("Running notebook for:", hospital_name)


# COMMAND ----------

