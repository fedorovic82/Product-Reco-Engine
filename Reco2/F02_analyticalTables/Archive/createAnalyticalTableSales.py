# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC refresh table data_reco_feature_tables.ods_features_sales;
# MAGIC --refresh table data_reco_feature_tables.ods_features_users;

# COMMAND ----------

from pyspark.sql.functions import *

def readFeatureMatrix(df_feature_long, var_country_code, dimension1, dimension2,features):

  i = 0
  filter_sting = str('')

  for feature in features:
    
    i += 1

    
    if i == 1:
      filter_sting += 'feature like "'+str(feature)+'%"'

    if i > 1:
      filter_sting += ' or feature like "'+str(feature)+'%"'
  
  print(filter_sting)
 
  df_feature_long = df_feature_long.where(filter_sting)
  
  df_feature_long = df_feature_long.where(col('countryCode').like(var_country_code))

  if dimension2 == 'NA':
    df_feature = df_feature_long.groupBy('countryCode', dimension1).pivot('feature').max('value')

  if dimension2 != 'NA':
    df_feature = df_feature_long.groupBy('countryCode', dimension1, dimension2).pivot('feature').max('value')
     
  return df_feature


# COMMAND ----------

df_feature_sales = spark.table('data_reco_feature_tables.ods_features_sales')
df_feature_sales_wide_prd = readFeatureMatrix(df_feature_sales, 'AT', "operatorOhubId", 'NA', ['dep_dum_prs_', 'ind_dum_prs_prd_'])

df_feature_sales_wide_prd.show()

# COMMAND ----------

df_feature_sales_wide_prd.agg(countDistinct(concat(col('countryCode'),col('operatorOhubId')))).show()
df_feature_sales_wide_prd.count()

# COMMAND ----------

#df_analytical = df_feature_sales_wide.join(df_feature_users_wide, ['COUNTRY_CODE','operatorOhubId'], 'inner')
df_analytical_prd = df_feature_sales_wide_prd

# COMMAND ----------

df_analytical_prd.agg(countDistinct(concat(col('countryCode'),col('operatorOhubId')))).show()
df_analytical_prd.count()

# COMMAND ----------

# Save Dataframe    
df_analytical_prd.write.mode("Overwrite").saveAsTable('data_reco_analytical_tables.ods_analytical_sales_at_prd')


# COMMAND ----------

df_feature_sales_est = spark.table('data_reco_feature_tables.ods_features_sales')
df_feature_sales_est_wide = readFeatureMatrix(df_feature_sales_est, 'AT', "operatorOhubId", 'NA', ['dep_dum_prs_', 'ind_dum_prs_est_'])

df_feature_sales_est_wide.show()

# COMMAND ----------

df_analytical_est = df_feature_sales_est_wide

# COMMAND ----------

df_analytical_est.agg(countDistinct(concat(col('countryCode'),col('operatorOhubId')))).show()
df_analytical_est.count()

# COMMAND ----------

# Save Dataframe    
df_feature_sales_est_wide.write.mode("Overwrite").saveAsTable('data_reco_analytical_tables.ods_analytical_sales_at_est')
