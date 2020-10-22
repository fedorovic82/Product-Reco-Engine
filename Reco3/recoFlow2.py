# Databricks notebook source
# MAGIC %md ### 0) Select the countries for the steps

# COMMAND ----------

# MAGIC %run "/Shared/dataScience/Reco3/recoFlowConfig"

# COMMAND ----------

# MAGIC %md ### 3) Estimate the models

# COMMAND ----------

today = datetime.now(timezone('Europe/Amsterdam'))
today = today.strftime("%Y%m%d")
print(today)

# COMMAND ----------

# # Google Analytics
for c in countriesGa:
  try:
    dbutils.notebook.run("02_logitModelGaPy", 12000, {"countryCode": c, "date": today})
  except Exception as e:
    print(c + ': ' + str(e))

# COMMAND ----------

# Sales
for c in countriesSales:
  try:
    dbutils.notebook.run("02_logitModelSalesPy", 12000, {"countryCode": c, "date": today})
  except Exception as e:
    print(c + ': ' + str(e))
