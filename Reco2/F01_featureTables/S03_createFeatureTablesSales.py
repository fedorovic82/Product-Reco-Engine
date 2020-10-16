# Databricks notebook source
varCountryCode = getArgument("varCountryCode")
largeCountries = getArgument("largeCountries").split(',')

# COMMAND ----------

###########################################################################
##### Import Packages
###########################################################################
from pyspark.sql.types import StringType as StringType
from pyspark.sql import functions as F
from pyspark.sql import SparkSession as SparkSession
from featureGen.basic_functions.logging import printRunStatements
from featureGen.create_feature_dataframes.createCategoryDummies import createCategoryDummies as createCategoryDummies
from featureGen.create_feature_dataframes.createScaleDummies import createScaleDummies as createScaleDummies
from featureGen.create_feature_dataframes.createBehaviorFeatures import createBehaviorFeatures as createBehaviorFeatures
from featureGen.dataframe_transformations.unpivotDataframe import unpivotDataframe as unpivotDataframe

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table data_reco_input.ods_datascience_sales_max_corr_filtered;
# MAGIC refresh table data_reco_input.ods_datascience_sales_act_ops;

# COMMAND ----------

dfProductSales = spark.table('data_reco_input.ods_datascience_sales_max_corr_filtered')
dfOperatorsActive = spark.table('data_reco_input.ods_datascience_sales_act_ops')

# COMMAND ----------

dfProductSales = dfProductSales.where(F.col('countryCode') == varCountryCode)
dfOperatorsActiveRun = dfOperatorsActive.where(F.col('countryCode') == varCountryCode)

# COMMAND ----------

if varCountryCode.upper() in largeCountries:
  dfProductSalesIds = dfProductSales \
    .withColumn('date_dateformat', F.to_date(F.col('transactionDateString'), 'yyyyMMdd')) \
    .where(F.col('date_dateformat') >= F.col('max_yearmonth_min3')).select('operatorOHubId').distinct()
  dfOperatorsActiveRunFiltered = dfOperatorsActiveRun.join(dfProductSalesIds, ['operatorOHubId'], 'inner')
else:
  dfOperatorsActiveRunFiltered = dfOperatorsActiveRun

# COMMAND ----------

# Create Dependent Variables for Product Pageviews 
dfProductSalesDepDummies = createBehaviorFeatures(dataframe_ids = dfOperatorsActiveRunFiltered,
                                        dataframe_feature = dfProductSales,
                                        feature = 'cuEanCode',                        # group by id, feature
                                        ID = 'operatorOHubId',                        # operator id
                                        metric = '',                                  # not relevant for feature dummies
                                        feature_type = 'dummy',                       # occurrence or not
                                        date_column = 'transactionDateString',
                                        lower_bound = 80,                             # minimum number of occurrences of feature
                                        timeframe_start = 0,                          # months back in time from latest date
                                        timeframe_end = 3,                            # months back in time from latest date
                                        prefix = 'dep_dum_prs_',                      # prefix for each dummy
                                        denominator_metric = '',                      # not relevant for feature dummies
                                        denominator_type = '',                        # not relevant for feature dummies
                                        indicator_unixtime = 0)                       # 1 if date is unix time, 0 if not

# transform df to long format
dfProductSalesDepDummiesLong = unpivotDataframe(df = dfProductSalesDepDummies,
                                  id_vars = ["operatorOHubId"],
                                  value_vars = dfProductSalesDepDummies.columns[1:],
                                  var_name = "FEATURE",
                                  value_name = "VALUE")

# Add country_code
dfProductSalesDepDummiesLong = dfProductSalesDepDummiesLong \
  .withColumn('countryCode', F.lit(varCountryCode)) \
  .withColumn('featureType', F.lit('productOrder'))

# Save Dataframe  
dfProductSalesDepDummiesLong.write \
  .mode("overwrite") \
  .saveAsTable('data_reco_feature_tables.tmp1_ods_features_sales')

# COMMAND ----------

if varCountryCode.upper() in largeCountries:
  varTimeframeEnd = 6
else:
  varTimeframeEnd = 15
  
# Create Independent Variables for Product Pageviews 
dfProductSalesIndDummiesEst = createBehaviorFeatures(dataframe_ids = dfOperatorsActiveRunFiltered,
                                        dataframe_feature = dfProductSales,
                                        feature = "cuEanCode",                       # group by id, feature
                                        ID = "operatorOHubId",                       # operator id
                                        metric = "",                                 # not relevant for feature dummies
                                        feature_type = 'dummy',                      # occurrence or not
                                        date_column = 'transactionDateString',
                                        lower_bound = 80,                            # minimum number of occurrences of feature
                                        timeframe_start = 3,                         # months back in time from latest date
                                        timeframe_end = varTimeframeEnd,             # months back in time from latest date
                                        prefix = 'ind_dum_prs_est_',                 # prefix for each dummy
                                        denominator_metric = '',                     # not relevant for feature dummies
                                        denominator_type = '',                       # not relevant for feature dummies
                                        indicator_unixtime = 0)                      # 1 if date is unix time, 0 if not


# transform df to long format
dfProductSalesIndDummiesEstLong = unpivotDataframe(df = dfProductSalesIndDummiesEst,
                                  id_vars = ["operatorOHubId"],
                                  value_vars = dfProductSalesIndDummiesEst.columns[1:],
                                  var_name = "FEATURE",
                                  value_name = "VALUE")

# transform df to long format
dfProductSalesIndDummiesEstLong = dfProductSalesIndDummiesEstLong \
  .withColumn('countryCode', F.lit(varCountryCode)) \
  .withColumn('featureType', F.lit('productOrder'))

# Save Dataframe    
dfProductSalesIndDummiesEstLong.write \
  .mode("append") \
  .saveAsTable('data_reco_feature_tables.tmp1_ods_features_sales')

# COMMAND ----------

if varCountryCode.upper() in largeCountries:
  varTimeframeEnd = 3
else:
  varTimeframeEnd = 12
  
# Create Independent Variables for Product Pageviews
dfProductSalesIndDummiesPrd = createBehaviorFeatures(dataframe_ids = dfOperatorsActiveRunFiltered,
                                        dataframe_feature = dfProductSales,
                                        feature = "cuEanCode",                        # group by id, feature
                                        ID = "operatorOHubId",                        # operator id
                                        metric = "",                                  # not relevant for feature dummies
                                        feature_type = 'dummy',                       # occurrence or not
                                        date_column = 'transactionDateString',
                                        lower_bound = 80,                             # minimum number of occurrences of feature
                                        timeframe_start = 0,                          # months back in time from latest date
                                        timeframe_end = varTimeframeEnd,              # months back in time from latest date
                                        prefix = 'ind_dum_prs_prd_',                  # prefix for each dummy
                                        denominator_metric = '',                      # not relevant for feature dummies
                                        denominator_type = '',                        # not relevant for feature dummies
                                        indicator_unixtime = 0)                       # 1 if date is unix time, 0 if not

# transform df to long format
dfProductSalesIndDummiesPrdLong = unpivotDataframe(df = dfProductSalesIndDummiesPrd,
                                  id_vars = ["operatorOHubId"],
                                  value_vars = dfProductSalesIndDummiesPrd.columns[1:],
                                  var_name = "FEATURE",
                                  value_name = "VALUE")

# transform df to long format
dfProductSalesIndDummiesPrdLong = dfProductSalesIndDummiesPrdLong \
  .withColumn('countryCode', F.lit(varCountryCode)) \
  .withColumn('featureType', F.lit('productOrder'))

# Save Dataframe    
dfProductSalesIndDummiesPrdLong.write \
  .mode("append") \
  .saveAsTable('data_reco_feature_tables.tmp1_ods_features_sales')

# COMMAND ----------

# Create Dependent Variables for Product Pageviews 
dfProductSalesWsDummies = createBehaviorFeatures(dataframe_ids = dfOperatorsActiveRun,
                                        dataframe_feature = dfProductSales,     
                                        feature = 'cuEanCode',                        # group by id, feature
                                        ID = 'operatorOHubId',                        # operator id
                                        metric = '',                                  # not relevant for feature dummies
                                        feature_type = 'dummy',                       # occurrence or not
                                        date_column = 'transactionDateString',        
                                        lower_bound = 0,                              # minimum number of occurrences of feature
                                        timeframe_start = 0,                          # months back in time from latest date
                                        timeframe_end = 6,                            # months back in time from latest date
                                        prefix = 'ws_dum_prs_',                       # prefix for each dummy
                                        denominator_metric = '',                      # not relevant for feature dummies
                                        denominator_type = '',                        # not relevant for feature dummies
                                        indicator_unixtime = 0)                       # 1 if date is unix time, 0 if not

# Transform df to long format
dfProductSalesWsDummiesLong = unpivotDataframe(df = dfProductSalesWsDummies,
                                  id_vars = ["operatorOHubId"],
                                  value_vars = dfProductSalesWsDummies.columns[1:],
                                  var_name = "FEATURE",
                                  value_name = "VALUE")

# Add country_code
dfProductSalesWsDummiesLong = dfProductSalesWsDummiesLong \
  .withColumn('countryCode', F.lit(varCountryCode)) \
  .withColumn('featureType', F.lit('productOrder'))

# Save Dataframe  
dfProductSalesWsDummiesLong.write \
  .mode("append") \
  .saveAsTable('data_reco_feature_tables.tmp1_ods_features_sales')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists data_reco_feature_tables.tmp2_ods_features_sales;
# MAGIC create table data_reco_feature_tables.tmp2_ods_features_sales (countryCode STRING, operatorOhubId STRING, featureType STRING, FEATURE STRING, VALUE FLOAT);

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into data_reco_feature_tables.tmp2_ods_features_sales
# MAGIC (
# MAGIC   select countryCode, operatorOhubId, featureType, FEATURE, VALUE
# MAGIC   from data_reco_feature_tables.tmp1_ods_features_sales sal1
# MAGIC   where 1=1
# MAGIC   and lower(feature) like '%ind_dum_prs_est_%'
# MAGIC   and replace(lower(sal1.feature),'ind_dum_prs_est_','') in 
# MAGIC   (
# MAGIC     select distinct replace(lower(sal2.feature),'ind_dum_prs_prd_','')
# MAGIC     from data_reco_feature_tables.tmp1_ods_features_sales sal2
# MAGIC     where 1=1
# MAGIC     and replace(lower(sal2.feature),'ind_dum_prs_prd_','') is not Null
# MAGIC   )
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into data_reco_feature_tables.tmp2_ods_features_sales
# MAGIC (
# MAGIC   select countryCode, operatorOhubId, featureType, FEATURE, VALUE
# MAGIC   from data_reco_feature_tables.tmp1_ods_features_sales sal1
# MAGIC   where 1=1
# MAGIC   and lower(feature) like '%ind_dum_prs_prd_%'
# MAGIC   and replace(lower(sal1.feature),'ind_dum_prs_prd_','') in 
# MAGIC   (
# MAGIC     select distinct replace(lower(sal2.feature),'ind_dum_prs_est_','')
# MAGIC     from data_reco_feature_tables.tmp1_ods_features_sales sal2
# MAGIC     where 1=1
# MAGIC     and replace(lower(sal2.feature),'ind_dum_prs_est_','') is not Null
# MAGIC   )
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into data_reco_feature_tables.tmp2_ods_features_sales
# MAGIC (
# MAGIC   select countryCode, operatorOhubId, featureType, FEATURE, VALUE
# MAGIC   from data_reco_feature_tables.tmp1_ods_features_sales sal1
# MAGIC   where 1=1
# MAGIC   and lower(feature) not like '%ind_dum_prs_%'
# MAGIC )

# COMMAND ----------

# Save Dataframe  
dfOdsFeaturesSales = spark.table('data_reco_feature_tables.tmp2_ods_features_sales')

dfOdsFeaturesSales.write.format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .option("replaceWhere", "countryCode = '" + varCountryCode + "'") \
  .partitionBy("countryCode", "featureType") \
  .save("/delta/ods_features_sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists data_reco_feature_tables.tmp1_ods_features_sales;
# MAGIC drop table if exists data_reco_feature_tables.tmp2_ods_features_sales;
