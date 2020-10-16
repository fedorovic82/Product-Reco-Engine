# Databricks notebook source

# COMMAND ----------

countryCodeU = getArgument("runCountryCode").upper()
countryCodeL = countryCodeU.lower()

# COMMAND ----------

spark.sql('refresh table data_reco_feature_tables.ods_features_sales')

# COMMAND ----------

from pyspark.sql import functions as f
# Input table with all features
dfFeatureSales = spark.table('data_reco_feature_tables.ods_features_sales')

# Change to independent feature matrix
dfFeatureSalesWidePrd = readFeatureMatrix(dfFeatureSales, 
                                       countryCodeU, 
                                       'operatorOhubId', 
                                       'NA', 
                                       ['ind_dum_prs_prd_'], 
                                       'prd_')
dfFeatureSalesWideEst = readFeatureMatrix(dfFeatureSales, 
                                       countryCodeU, 
                                       'operatorOhubId', 
                                       'NA', 
                                       ['ind_dum_prs_est_'], 
                                       'est_')

# Select dependent features
dfAnalyticalSalesDep = dfFeatureSales \
  .where(f.lower(f.col('FEATURE')).like('%dep_dum_prs%')) \
  .where(f.col('countryCode') == countryCodeU)

# Join indep and dep variables
dfAnalyticalPrd = dfAnalyticalSalesDep \
  .join(dfFeatureSalesWidePrd, ['operatorOhubId', 'countryCode'], 'inner')
# Join indep and dep variables
dfAnalyticalEst = dfAnalyticalSalesDep \
  .join(dfFeatureSalesWideEst, ['operatorOhubId', 'countryCode'], 'inner')

# COMMAND ----------

deltaFolder1 = '/delta/guus/sales/est/'
deltaFolder2 = '/delta/guus/sales/prd/'
deltaTable1 = "'" + deltaFolder1 + countryCodeL + "'"
deltaTable2 = "'" + deltaFolder2 + countryCodeL + "'"
hiveTable1 = "data_reco_analytical_tables." + countryCodeL + "_ods_analytical_table_sales_est"
hiveTable2 = "data_reco_analytical_tables." + countryCodeL + "_ods_analytical_table_sales_prd"

# COMMAND ----------

# Save Dataframe    
dfAnalyticalPrd.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .save(deltaFolder2 + countryCodeL)

# Save Dataframe    
dfAnalyticalEst.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .save(deltaFolder1 + countryCodeL)

# COMMAND ----------

sqlQuery1 = "Drop Table if exists " + hiveTable1
sqlQuery2 = "CREATE TABLE " + hiveTable1 + " USING DELTA LOCATION " + deltaTable1
sqlQuery3 = "Drop Table if exists " + hiveTable2
sqlQuery4 = "CREATE TABLE " + hiveTable2 + " USING DELTA LOCATION " + deltaTable2

spark.sql(sqlQuery1)
spark.sql(sqlQuery2)
spark.sql(sqlQuery3)
spark.sql(sqlQuery4)
