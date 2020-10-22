# Databricks notebook source
import re
from pyspark.sql import functions as f

countryCodeU = getArgument('countryCode').upper()
date = getArgument('date')
# countryCodeU = 'NL'
# date = 'test'

countryCodeL = countryCodeU.lower()

fileDir = 'dbfs:/mnt/marketoimportexport/recommendations/'

fileName = 'recommendations'
fileNameCsv1 = fileName + "_" + countryCodeL + "_" + date
fileNameCsv2 = fileName + "_" + countryCodeL + "_" + date + '.csv'

filePathCsv1 = fileDir + fileNameCsv1
filePathCsv2 = fileDir + fileNameCsv2

# COMMAND ----------

df = spark.table('dev_derived_reco.output_recommendations_marketo').where(f.col('countryCode') == countryCodeU)
df.show()

# COMMAND ----------

# Write csv file
df \
  .repartition(1) \
  .write \
  .mode('overwrite') \
  .option('sep', ';') \
  .option("encoding", "UTF-8") \
  .option('header', 'true') \
  .option('quoteAll', 'true') \
  .csv(filePathCsv1)

# COMMAND ----------

# MAGIC %run "/Shared/S01_Libraries_Functions"

# COMMAND ----------

# Merge multiple csv into One
copyMerge(filePathCsv1, filePathCsv2, overwrite=True, deleteSource=True, debug=True)

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/marketoimportexport/recommendations
