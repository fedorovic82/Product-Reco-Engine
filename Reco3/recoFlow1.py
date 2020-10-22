# Databricks notebook source
# MAGIC %md ### 0) Select the countries for the steps

# COMMAND ----------

# MAGIC %run "/Shared/dataScience/Reco3/recoFlowConfig"

# COMMAND ----------

# MAGIC %md ### 0) Prepare input data

# COMMAND ----------

dbutils.notebook.run("00_processSales", 12000)

# COMMAND ----------

dbutils.notebook.run("00_processGa", 12000)

# COMMAND ----------

# MAGIC %md ### 1) Create feature tables

# COMMAND ----------

for countryCode in countriesGa:
  try:
    dbutils.notebook.run("01_createAnalyticalFeatureTablesGA", 12000, {"countryCode": countryCode, "largeCountriesGa": ','.join(largeCountriesGa)})
  except Exception as e:
    print(countryCode + ": " + str(e))

# COMMAND ----------

for countryCode in countriesSales:
  try:
    dbutils.notebook.run("01_createAnalyticalFeatureTablesSales", 12000, {"countryCode": countryCode})
  except Exception as e:
    print(countryCode + ": " + str(e))
