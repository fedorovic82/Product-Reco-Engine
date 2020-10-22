# Databricks notebook source
from pyspark.sql import functions as f
from pyspark.sql import window as w
# from featureGen.create_feature_dataframes.createScaleDummies import createScaleDummies as createScaleDummies
# from featureGen.create_feature_dataframes.createBehaviorFeatures import createBehaviorFeatures as createBehaviorFeatures
# from featureGen.dataframe_transformations.unpivotDataframe import unpivotDataframe as unpivotDataframe
from featureGen.numericalProcessing.createFeatureDataframes.createScaleDummies import createScaleDummies as createScaleDummies
from featureGen.numericalProcessing.createFeatureDataframes.createBehaviorFeatures import createBehaviorFeatures as createBehaviorFeatures
from featureGen.numericalProcessing.dataframeTransformations.unpivotDataframe import unpivotDataframe as unpivotDataframe

# COMMAND ----------

countryCodeU = getArgument("countryCode").upper()
# countryCodeU = 'ZA'
countryCodeL = countryCodeU.lower()

# COMMAND ----------

salesTable = 'dev_derived_reco.sales_max_corrected_filtered'
operatorTable = 'dev_derived_reco.sales_operators_active'

# COMMAND ----------

dfProductSales = spark.table(salesTable).where(f.col('countryCode') == countryCodeU)
dfOperatorsActive = spark.table(operatorTable).where(f.col('countryCode') == countryCodeU)

# COMMAND ----------

# Create Dependent Variables for Product Sales 
dfProductSalesDepDummies = createBehaviorFeatures(
  dataframe_ids = dfOperatorsActive,
  dataframe_feature = dfProductSales,
  feature = 'cuEanCode',                        # group by id, feature
  ID = 'operatorOhubId',                        # operator id
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
dfProductSalesDepDummiesLong = unpivotDataframe(
  df = dfProductSalesDepDummies,
  id_vars = ["operatorOhubId"],
  value_vars = dfProductSalesDepDummies.columns[1:],
  var_name = "FEATURE",
  value_name = "VALUE")

# Add country_code
dfProductSalesDepDummiesLong = dfProductSalesDepDummiesLong \
  .withColumn('countryCode', f.lit(countryCodeU)) \
  .withColumn('featureType', f.lit('productOrder'))

# COMMAND ----------

# Create Independent Variables for Product Sales 
dfProductSalesIndDummiesEst = createBehaviorFeatures(
  dataframe_ids = dfOperatorsActive,
  dataframe_feature = dfProductSales,
  feature = "cuEanCode",                       # group by id, feature
  ID = "operatorOhubId",                       # operator id
  metric = "",                                 # not relevant for feature dummies
  feature_type = 'dummy',                      # occurrence or not
  date_column = 'transactionDateString',
  lower_bound = 30,                            # minimum number of occurrences of feature
  timeframe_start = 3,                         # months back in time from latest date
  timeframe_end = 15,                          # months back in time from latest date
  prefix = 'ind_dum_prs_est_',                 # prefix for each dummy
  denominator_metric = '',                     # not relevant for feature dummies
  denominator_type = '',                       # not relevant for feature dummies
  indicator_unixtime = 0)                      # 1 if date is unix time, 0 if not


# transform df to long format
dfProductSalesIndDummiesEstLong = unpivotDataframe(
  df = dfProductSalesIndDummiesEst,
  id_vars = ["operatorOhubId"],
  value_vars = dfProductSalesIndDummiesEst.columns[1:],
  var_name = "FEATURE",
  value_name = "VALUE")

# transform df to long format
dfProductSalesIndDummiesEstLong = dfProductSalesIndDummiesEstLong \
  .withColumn('countryCode', f.lit(countryCodeU)) \
  .withColumn('featureType', f.lit('productOrder'))

# COMMAND ----------

# Create Independent Variables for Product Sales
dfProductSalesIndDummiesPrd = createBehaviorFeatures(
  dataframe_ids = dfOperatorsActive,
  dataframe_feature = dfProductSales,
  feature = "cuEanCode",                        # group by id, feature
  ID = "operatorOhubId",                        # operator id
  metric = "",                                  # not relevant for feature dummies
  feature_type = 'dummy',                       # occurrence or not
  date_column = 'transactionDateString',
  lower_bound = 30,                             # minimum number of occurrences of feature
  timeframe_start = 0,                          # months back in time from latest date
  timeframe_end = 12,                           # months back in time from latest date
  prefix = 'ind_dum_prs_prd_',                  # prefix for each dummy
  denominator_metric = '',                      # not relevant for feature dummies
  denominator_type = '',                        # not relevant for feature dummies
  indicator_unixtime = 0)                       # 1 if date is unix time, 0 if not

# transform df to long format
dfProductSalesIndDummiesPrdLong = unpivotDataframe(
  df = dfProductSalesIndDummiesPrd,
  id_vars = ["operatorOhubId"],
  value_vars = dfProductSalesIndDummiesPrd.columns[1:],
  var_name = "FEATURE",
  value_name = "VALUE")

# transform df to long format
dfProductSalesIndDummiesPrdLong = dfProductSalesIndDummiesPrdLong \
  .withColumn('countryCode', f.lit(countryCodeU)) \
  .withColumn('featureType', f.lit('productOrder'))

# COMMAND ----------

# Create Dependent Variables for Product Sales 
dfProductSalesWsDummies = createBehaviorFeatures(
  dataframe_ids = dfOperatorsActive,
  dataframe_feature = dfProductSales,     
  feature = 'cuEanCode',                        # group by id, feature
  ID = 'operatorOhubId',                        # operator id
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
dfProductSalesWsDummiesLong = unpivotDataframe(
  df = dfProductSalesWsDummies,
  id_vars = ["operatorOhubId"],
  value_vars = dfProductSalesWsDummies.columns[1:],
  var_name = "FEATURE",
  value_name = "VALUE")

# Add country_code
dfProductSalesWsDummiesLong = dfProductSalesWsDummiesLong \
  .withColumn('countryCode', f.lit(countryCodeU)) \
  .withColumn('featureType', f.lit('productOrder'))

# COMMAND ----------

tmpFeaturesSales = dfProductSalesDepDummiesLong \
  .union(dfProductSalesIndDummiesEstLong) \
  .union(dfProductSalesIndDummiesPrdLong) \
  .union(dfProductSalesWsDummiesLong) \
  .cache()

productListPrd = tmpFeaturesSales \
  .select(f.regexp_replace(f.lower(f.col('feature')), 'ind_dum_prs_prd_', '').alias('feature')) \
  .where(f.col('feature').isNotNull()) \
  .distinct().rdd.flatMap(lambda x: x).collect()

productListEst = tmpFeaturesSales \
  .select(f.regexp_replace(f.lower(f.col('feature')), 'ind_dum_prs_est_', '').alias('feature')) \
  .where(f.col('feature').isNotNull()) \
  .distinct().rdd.flatMap(lambda x: x).collect()

dfFeatureSales = tmpFeaturesSales \
  .where(((f.lower(f.col('feature')).like('%ind_dum_prs_est_%')) & (f.regexp_replace(f.lower(f.col('feature')), 'ind_dum_prs_est_', '').isin(productListPrd))) | 
         ((f.lower(f.col('feature')).like('%ind_dum_prs_prd_%')) & (f.regexp_replace(f.lower(f.col('feature')), 'ind_dum_prs_prd_', '').isin(productListEst))) |
         (~f.lower(f.col('feature')).like('%ind_dum_prs_%')))

# COMMAND ----------

# Change to independent feature matrix
for i, feature in enumerate(['ind_dum_prs_prd_']): 
  if i == 0:
    filterStringPrd = 'feature like "' + feature + '%"'
  else:
    filterStringPrd += ' or feature like "' + feature + '%"'

for i, feature in enumerate(['ind_dum_prs_est_']): 
  if i == 0:
    filterStringEst = 'feature like "' + feature + '%"'
  else:
    filterStringEst += ' or feature like "' + feature + '%"'
    
dfFeatureSalesWidePrd = dfFeatureSales \
  .where(filterStringPrd) \
  .withColumn('feature', f.regexp_replace('feature', 'prd_', '')) \
  .groupBy('countryCode', 'operatorOhubId') \
  .pivot('feature') \
  .max('value')

dfFeatureSalesWideEst = dfFeatureSales \
  .where(filterStringEst) \
  .withColumn('feature', f.regexp_replace('feature', 'est_', '')) \
  .groupBy('countryCode', 'operatorOhubId') \
  .pivot('feature') \
  .max('value')

# Select dependent features
dfAnalyticalSalesDep = dfFeatureSales \
  .where(f.lower(f.col('FEATURE')).like('%dep_dum_prs%'))

# Join indep and dep variables
dfAnalyticalPrd = dfAnalyticalSalesDep.join(dfFeatureSalesWidePrd, ['operatorOhubId', 'countryCode'], 'inner')
# Join indep and dep variables
dfAnalyticalEst = dfAnalyticalSalesDep.join(dfFeatureSalesWideEst, ['operatorOhubId', 'countryCode'], 'inner')

# COMMAND ----------

deltaTable1 = "/mnt/datamodel/dev/derived/reco/analytical/sales/est/" + countryCodeL
deltaTable2 = "/mnt/datamodel/dev/derived/reco/analytical/sales/prd/" + countryCodeL
hiveTable1 = "dev_derived_reco.analytical_sales_est_" + countryCodeL
hiveTable2 = "dev_derived_reco.analytical_sales_prd_" + countryCodeL

# Save Dataframe
dfAnalyticalEst.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .save(deltaTable1)

# Save Dataframe    
dfAnalyticalPrd.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .save(deltaTable2)

sqlQuery1 = "drop table if exists " + hiveTable1
sqlQuery2 = "drop table if exists " + hiveTable2
sqlQuery3 = "create table " + hiveTable1 + " using delta location '" + deltaTable1 + "'"
sqlQuery4 = "create table " + hiveTable2 + " using delta location '" + deltaTable2 + "'"
spark.sql(sqlQuery1)
spark.sql(sqlQuery2)
spark.sql(sqlQuery3)
spark.sql(sqlQuery4)
