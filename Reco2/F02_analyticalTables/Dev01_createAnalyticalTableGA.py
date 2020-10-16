# Databricks notebook source

# COMMAND ----------

spark.sql('refresh table data_reco_feature_tables.ods_features_pageviews')
spark.sql('refresh table data_reco_feature_tables.ods_features_users')

# COMMAND ----------

countryCodeU = getArgument("runCountryCode").upper()
countryCodeL = countryCodeU.lower()

# COMMAND ----------

from pyspark.sql import functions as f
# Input tables with all features
dfFeaturePageviews = spark.table('data_reco_feature_tables.ods_features_pageviews')
dfFeatureUsers = spark.table('data_reco_feature_tables.ods_features_users')

# Change to independent feature matrices
dfFeaturePageviewsWide = readFeatureMatrix(dfFeaturePageviews, 
                                           countryCodeU, 
                                           'trackingid', 
                                           'NA', 
                                           ['ind_dum_ppv', 'ind_dum_rpv'])
dfFeatureUsersWide = readFeatureMatrix(dfFeatureUsers, 
                                       countryCodeU, 
                                       'trackingid', 
                                       'NA', 
                                       ['ind_dum_dur', 'ind_dum_rec', 'ind_dum_frq'])

# Select dependent features
dfAnalyticsGaDep = dfFeaturePageviews \
  .where(f.lower(f.col('FEATURE')).like('%dep_dum_ppv%')) \
  .where(f.col('country_code') == countryCodeU)

dfAnalytical = dfAnalyticsGaDep \
  .join(dfFeaturePageviewsWide, ['country_code', 'trackingid'], 'inner') \
  .join(dfFeatureUsersWide, ['country_code', 'trackingid'], 'inner')

# COMMAND ----------

deltaFolder = '/delta/guus/'
deltaTable = "'" + deltaFolder + countryCodeL + "'"
hiveTable = "data_reco_analytical_tables." + countryCodeL + "_analytical_table_ga"

# COMMAND ----------

dfAnalytical.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .save(deltaFolder + countryCodeL)

# COMMAND ----------

sqlQuery1 = "Drop table if exists " + hiveTable
sqlQuery2 = "CREATE TABLE " + hiveTable + " USING DELTA LOCATION " + deltaTable

spark.sql(sqlQuery1)
spark.sql(sqlQuery2)
