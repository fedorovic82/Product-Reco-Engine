# Databricks notebook source
from pyspark.sql import functions as f

# COMMAND ----------

df = spark.table('dev_derived_integration_ga.output_ga_fact_hits')

minDate = '20200301'
maxDate = '20200401'

def remove_string_column(s1, s2):
  if s1 in s2:
    s = s2.replace(s1,'')
  return s
udf_remove_string_column = f.udf(remove_string_column)

df = df \
  .where(f.col('date') >= minDate) \
  .where(f.col('date') <= maxDate)

dfProductRecoClicks = df \
  .where(f.lower(f.col('eventType')).contains('recommended')) \
  .where(~f.lower(f.col('eventType')).contains('recommended click to purchase')) \
  .where(~f.lower(f.col('eventType')).contains('recommended recipe click')) \
  .withColumn('recoType', f.trim(f.substring_index('eventDetails', '-', 1))) \
  .withColumn('recoRank', f.trim(f.regexp_replace(udf_remove_string_column(f.substring_index('eventDetails', '-', 1), f.substring_index('eventDetails', '-', 2)), '-', ''))) \
  .withColumn('recoContent', f.trim(f.regexp_replace(udf_remove_string_column(f.substring_index('eventDetails', '-', 2), 'eventDetails'), '^-', ''))) \
  .cache()

dfRecipeRecoClicks = df \
  .where(f.lower(f.col('eventType')).contains('recommended recipe click')) \
  .withColumn('recoType', f.trim(f.substring_index('eventDetails', '-', 1))) \
  .withColumn('recoRank', f.trim(f.regexp_replace(udf_remove_string_column(f.substring_index('eventDetails', '-', 1), f.substring_index('eventDetails', '-', 2)), '-', ''))) \
  .withColumn('recoContent', f.trim(f.regexp_replace(udf_remove_string_column(f.substring_index('eventDetails', '-', 2), 'eventDetails'), '^-', ''))) \
  .cache()

# COMMAND ----------

display(dfProductRecoClicks.groupBy('countryCode', 'eventType', 'recoType').count().orderBy('countryCode', 'eventType', 'recoType'))

# COMMAND ----------


