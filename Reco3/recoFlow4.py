# Databricks notebook source
# MAGIC %md ### 0) Select the countries for the steps

# COMMAND ----------

# MAGIC %run "/Shared/dataScience/Reco3/recoFlowConfig"

# COMMAND ----------

# countriesExport = ['NL']

# COMMAND ----------

# MAGIC %md ### 6) Export recommendations in varying format to Oview (S3), UFS.com (S3), ACM (Neolane SFTP), Marketo(Azure Storage Account)

# COMMAND ----------

date = datetime.now(timezone('Europe/Amsterdam')).date().strftime('%Y%m%d')
print(date)

# COMMAND ----------

# Ufs.com
for c in countriesExport:
  for recoType in ['product', 'recipe']:
    dbutils.notebook.run("05_exportToS3Ufs", 12000, {"date": date, "recommendationType": recoType, "countryCode": c})

# COMMAND ----------

# Oview
for c in countriesExport:
  dbutils.notebook.run("05_exportToS3Oview", 12000, {"date": date, "countryCode": c})

# COMMAND ----------

# ACM
dfpConfig = spark.table('dev_sources_meta.cleaned_website_region').toPandas()
tuples = []
for c in countriesExport:
  countryRows = dfpConfig[dfpConfig['country_iso'] == c]
  for index, row in countryRows.iterrows():
    MCO = row['mco_wunderman']
    tuples.append((c, MCO))
# Remove duplicates and sort the list alphabetically
mcoCountryCombinations = list(set(tuples))
mcoCountryCombinations.sort()

for i in mcoCountryCombinations:
  dbutils.notebook.run("05_exportToBlobToNeolane", 12000, {"mco": i[1], "countryCode": i[0]})

# COMMAND ----------

# Marketo
for c in countriesExport:
  dbutils.notebook.run("05_exportToMarketo", 12000, {"date": date, "countryCode": c})
