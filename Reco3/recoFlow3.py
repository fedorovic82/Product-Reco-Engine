# Databricks notebook source
# MAGIC %md ### 0) Select the countries for the steps

# COMMAND ----------

# MAGIC %run "/Shared/dataScience/Reco3/recoFlowConfig"

# COMMAND ----------

# countriesGa = ["NL"]

# COMMAND ----------

# MAGIC %md ### 4) Create recommendations

# COMMAND ----------

for c in countriesGa:
  try:
    dbutils.notebook.run("03_createRecommendationSet", 
      12000, 
      {"countryCode": c, 
      "countriesNoSalesScores": ','.join(countriesNoSalesScores), 
      "countriesProductCodeLocal": ','.join(countriesProductCodeLocal),
      "countriesBusinessRuling": ','.join(countriesBusinessRuling)})
  except Exception as e:
    print(c + str(e))

# COMMAND ----------

# MAGIC %md ### 5) Create recommendation files to dispatch

# COMMAND ----------

# Create recommendation files for export to different locations
dfpConfig = spark.table('dev_sources_meta.cleaned_website_region').toPandas()
tuples = []
for c in countriesGa:
  countryRows = dfpConfig[dfpConfig['country_iso'] == c]
  for index, row in countryRows.iterrows():
    MCO = row['mco_wunderman']
    tuples.append((c, MCO))
# Acm doesn't take files with the language specified in the filename, we want to have 1 file per country
tuples = list(set(tuples))
tuples.sort()

for c, mco in tuples:
  try:
    dbutils.notebook.run("04_recoFeed", 12000, {
      "countryCode": c, 
      "mco": mco,
      "countriesBusinessRuling": ','.join(countriesBusinessRuling)})
  except Exception as e:
    print(c + str(e))
