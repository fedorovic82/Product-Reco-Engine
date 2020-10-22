# Databricks notebook source
from pyspark.sql import functions as f
from pyspark.sql import types as t

from featureGen.numericalProcessing.createFeatureDataframes.createScaleDummies import createScaleDummies as createScaleDummies
from featureGen.numericalProcessing.createFeatureDataframes.createBehaviorFeatures import createBehaviorFeatures as createBehaviorFeatures
from featureGen.numericalProcessing.dataframeTransformations.unpivotDataframe import unpivotDataframe as unpivotDataframe

# COMMAND ----------

countryCodeU = getArgument('countryCode').upper()
largeCountries = getArgument('largeCountriesGa').split(',')
# countryCodeU = 'NL'
# largeCountries = ['NL']

countryCodeL = countryCodeU.lower()

# COMMAND ----------

dfHitsPageMaxCorrected = spark.table('dev_derived_reco.ga_fact_hits_max_corrected').where(f.col('countryCode') == countryCodeU)
dfUsers = spark.table('dev_derived_reco.ga_users').where(f.col('countryCode') == countryCodeU)

# COMMAND ----------

# Filter data 
if countryCodeU in largeCountries:
  varTimeframeEnd = 3
else:
  varTimeframeEnd = 6

# COMMAND ----------

# DBTITLE 1,Filter Hits Page Data to desired TimeFrame (3 or 6 months back)
# Add trackingId and filter on date
dfHitsPageMaxCorrectedFiltered = dfHitsPageMaxCorrected \
  .where(f.to_date(f.col('visitStartTimeConverted')) >= f.col('maxDateMin' + str(varTimeframeEnd))) \
  .where(f.to_date(f.col('visitStartTimeConverted')) <= f.col('maxDate'))

# Filter the users based on the ones with a trackingId and with a hit in ga within the timeframe 
dfUsersFiltered = dfUsers.join(dfHitsPageMaxCorrectedFiltered.select('trackingid'), ['trackingId'], 'left_semi')

# COMMAND ----------

# DBTITLE 1,Create Product and Recipe Pageviews
dfProductPageviews = dfHitsPageMaxCorrectedFiltered \
  .where((f.lower(f.col('type')) == 'page') & 
         f.col('productCode').isNotNull() & 
         (f.col('productCode') != '')) \
  .withColumn('productCodeCorrected', f.upper(f.regexp_replace(f.col('productCode'), '-', '_')))

dfRecipePageviews = dfHitsPageMaxCorrectedFiltered \
  .where((f.lower(f.col('type')) == 'page') & 
         f.col('recipeId').isNotNull() & 
         (f.col('recipeId') != '')) \
  .withColumn('recipeId', f.upper(f.col('recipeId')))

# COMMAND ----------

# Create Dependent Variables for Product Pageviews 
dfProductPageviewsDepDummies = createBehaviorFeatures(
  dataframe_ids = dfUsersFiltered,
  dataframe_feature = dfProductPageviews,    
  feature = 'productCodeCorrected',             # group by id, feature
  ID = 'trackingId',                            # operator id
  metric = '',                                  # not relevant for feature dummies
  feature_type = 'dummy',                       # occurrence or not
  date_column = 'gaDate',
  lower_bound = 80,                             # minimum number of occurrences of feature
  timeframe_start = 0,                          # months back in time from latest date
  timeframe_end = varTimeframeEnd,              # months back in time from latest date
  prefix = 'dep_dum_ppv_',                      # prefix for each dummy
  denominator_metric = '',                      # not relevant for feature dummies
  denominator_type = '',                        # not relevant for feature dummies
  indicator_unixtime = 0)                       # 1 if date is unix time, 0 if not

# transform df to long format
dfProductPageviewsDepDummiesLong = unpivotDataframe(
  df = dfProductPageviewsDepDummies,
  id_vars = ["trackingId"],
  value_vars = dfProductPageviewsDepDummies.columns[1:],
  var_name = "FEATURE",
  value_name = "VALUE")

# Add countryCode
dfProductPageviewsDepDummiesLong = dfProductPageviewsDepDummiesLong \
  .withColumn('countryCode', f.lit(countryCodeU)) \
  .withColumn('featureType', f.lit('productPageview'))

# COMMAND ----------

# Create Independent Variables for Product Pageviews 
dfProductPageviewsIndDummies = createBehaviorFeatures(
  dataframe_ids = dfUsersFiltered,
  dataframe_feature = dfProductPageviews,
  feature = "productCodeCorrected",             # group by id, feature
  ID = "trackingId",                            # operator id
  metric = "",                                  # not relevant for feature dummies
  feature_type = 'dummy',                       # occurrence or not
  date_column = 'gaDate',
  lower_bound = 30,                             # minimum number of occurrences of feature
  timeframe_start = 0,                          # months back in time from latest date
  timeframe_end = varTimeframeEnd,              # months back in time from latest date
  prefix = 'ind_dum_ppv_',                      # prefix for each dummy
  denominator_metric = '',                      # not relevant for feature dummies
  denominator_type = '',                        # not relevant for feature dummies
  indicator_unixtime = 0)                       # 1 if date is unix time, 0 if not

# transform df to long format
dfProductPageviewsIndDummiesLong = unpivotDataframe(
  df = dfProductPageviewsIndDummies,
  id_vars = ["trackingId"],
  value_vars = dfProductPageviewsIndDummies.columns[1:],
  var_name = "FEATURE",
  value_name = "VALUE")

# transform df to long format
dfProductPageviewsIndDummiesLong = dfProductPageviewsIndDummiesLong \
  .withColumn('countryCode', f.lit(countryCodeU)) \
  .withColumn('featureType', f.lit('productPageview'))

# COMMAND ----------

# Create Dependent Variables for Recipe Pageviews 
dfProductPageviewsDepDummies = createBehaviorFeatures(
  dataframe_ids = dfUsersFiltered,
  dataframe_feature = dfRecipePageviews,
  feature = "recipeId",             # group by id, feature
  ID = "trackingId",                            # operator id
  metric = "",                                  # not relevant for feature dummies
  feature_type = 'dummy',                       # occurrence or not
  date_column = 'gaDate',
  lower_bound = 80,                             # minimum number of occurrences of feature
  timeframe_start = 0,                          # months back in time from latest date
  timeframe_end = varTimeframeEnd,              # months back in time from latest date
  prefix = 'dep_dum_rpv_',                      # prefix for each dummy
  denominator_metric = '',                      # not relevant for feature dummies
  denominator_type = '',                        # not relevant for feature dummies
  indicator_unixtime = 0)                       # 1 if date is unix time, 0 if not

# transform df to long format
dfRecipePageviewsDepDummiesLong = unpivotDataframe(
  df = dfProductPageviewsDepDummies,
  id_vars = ["trackingId"],
  value_vars = dfProductPageviewsDepDummies.columns[1:],
  var_name = "FEATURE",
  value_name = "VALUE")

# Add countryCode
dfRecipePageviewsDepDummiesLong = dfRecipePageviewsDepDummiesLong \
  .withColumn('countryCode', f.lit(countryCodeU)) \
  .withColumn('featureType', f.lit('recipePageview'))

# COMMAND ----------

# Create Independent Variables for Recipe Pageviews 
dfRecipePageviewsIndDummies = createBehaviorFeatures(
  dataframe_ids = dfUsersFiltered,
  dataframe_feature = dfRecipePageviews,
  feature = "recipeId",             # group by id, feature
  ID = "trackingId",                            # operator id
  metric = "",                                  # not relevant for feature dummies
  feature_type = 'dummy',                       # occurrence or not
  date_column = 'gaDate',
  lower_bound = 30,                             # minimum number of occurrences of feature
  timeframe_start = 0,                          # months back in time from latest date
  timeframe_end = varTimeframeEnd,              # months back in time from latest date
  prefix = 'ind_dum_rpv_',                      # prefix for each dummy
  denominator_metric = '',                      # not relevant for feature dummies
  denominator_type = '',                        # not relevant for feature dummies
  indicator_unixtime = 0)                       # 1 if date is unix time, 0 if not

# transform df to long format
dfRecipePageviewsIndDummiesLong = unpivotDataframe(
  df = dfRecipePageviewsIndDummies,
  id_vars = ["trackingId"],
  value_vars = dfRecipePageviewsIndDummies.columns[1:],
  var_name = "FEATURE",
  value_name = "VALUE")

# transform df to long format
dfRecipePageviewsIndDummiesLong = dfRecipePageviewsIndDummiesLong \
  .withColumn('countryCode', f.lit(countryCodeU)) \
  .withColumn('featureType', f.lit('recipePageview'))

# COMMAND ----------

# Create Features for User Visit Frequency
dfUsersIndFrqDummies = createScaleDummies(
  dfUsersFiltered,
  "cntVisits",        # group by id, feature
  "trackingId",        # operator id
  0,                   # minimum number of occurrences of feature
  "ind_dum_frq_")      # prefix for each dummy

# transform df to long format
dfUsersIndFrqDummiesLong = unpivotDataframe(
  df = dfUsersIndFrqDummies,
  id_vars = ["trackingId"],
  value_vars = dfUsersIndFrqDummies.columns[1:],
  var_name = "FEATURE",
  value_name = "VALUE")

# transform df to long format
dfUsersIndFrqDummiesLong = dfUsersIndFrqDummiesLong \
  .withColumn('countryCode', f.lit(countryCodeU)) \
  .withColumn('featureType', f.lit('userAttribute'))

# COMMAND ----------

# Create Feature for User Visit Recency
dfUsersIndRecDummies = createScaleDummies(
  dfUsersFiltered,   
  "weeksSinceLastView",      # group by id, feature
  "trackingId",                 # operator id
  0,                            # minimum number of occurrences of feature
  'ind_dum_rec_')               # prefix for each dummy

# transform df to long format
dfUsersIndRecDummiesLong = unpivotDataframe(
  df = dfUsersIndRecDummies,
  id_vars = ["trackingId"],
  value_vars = dfUsersIndRecDummies.columns[1:],
  var_name = "FEATURE",
  value_name = "VALUE")

# transform df to long format
dfUsersIndRecDummiesLong = dfUsersIndRecDummiesLong \
  .withColumn('countryCode', f.lit(countryCodeU)) \
  .withColumn('featureType', f.lit('userAttribute'))

# COMMAND ----------

# Create Feature for User Average Time On Site 
dfUsersIndDurDummies = createScaleDummies(
  dfUsersFiltered,   
  "avgTimeOnSite",           # group by id, feature
  "trackingId",                 # operator id
  0,                            # minimum number of occurrences of feature
  'ind_dum_dur_')               # prefix for each dummy

# transform df to long format
dfUsersIndDurDummiesLong = unpivotDataframe(
  df = dfUsersIndDurDummies,
  id_vars = ["trackingId"],
  value_vars = dfUsersIndDurDummies.columns[1:],
  var_name = "FEATURE",
  value_name = "VALUE")

# transform df to long format
dfUsersIndDurDummiesLong = dfUsersIndDurDummiesLong \
  .withColumn('countryCode', f.lit(countryCodeU)) \
  .withColumn('featureType', f.lit('userAttribute'))

# COMMAND ----------

# dfRecipePageviewsIndDummiesLong.printSchema()

# COMMAND ----------

dfFeaturePageviews = dfProductPageviewsDepDummiesLong \
  .union(dfProductPageviewsIndDummiesLong) \
  .union(dfRecipePageviewsDepDummiesLong) \
  .union(dfRecipePageviewsIndDummiesLong)

dfFeatureUsers = dfUsersIndFrqDummiesLong \
  .union(dfUsersIndRecDummiesLong) \
  .union(dfUsersIndDurDummiesLong)

# COMMAND ----------

for i, feature in enumerate(['ind_dum_ppv', 'ind_dum_rpv']): 
  if i == 0:
    filterStringPageViews = 'feature like "' + feature + '%"'
  else:
    filterStringPageViews += ' or feature like "' + feature + '%"'

for i, feature in enumerate(['ind_dum_dur', 'ind_dum_rec', 'ind_dum_frq']): 
  if i == 0:
    filterStringUsers = 'feature like "' + feature + '%"'
  else:
    filterStringUsers += ' or feature like "' + feature + '%"'
    
dfFeaturePageviewsWide = dfFeaturePageviews \
  .where(filterStringPageViews) \
  .groupBy('countryCode', 'trackingId') \
  .pivot('feature') \
  .max('value')

dfFeatureUsersWide = dfFeatureUsers \
  .where(filterStringUsers) \
  .groupBy('countryCode', 'trackingId') \
  .pivot('feature') \
  .max('value')

# COMMAND ----------

# Select dependent features
dfAnalyticsGaDep = dfFeaturePageviews \
  .where(f.lower(f.col('FEATURE')).like('%dep_dum_ppv%')) \
  .where(f.col('countryCode') == countryCodeU)

dfAnalytical = dfAnalyticsGaDep \
  .join(dfFeaturePageviewsWide, ['countryCode', 'trackingId'], 'inner') \
  .join(dfFeatureUsersWide, ['countryCode', 'trackingId'], 'inner')

# COMMAND ----------

# display(dfAnalytical)

# COMMAND ----------

deltaTable = "/mnt/datamodel/dev/derived/reco/analytical/ga/" + countryCodeL
hiveTable = "dev_derived_reco.analytical_ga_" + countryCodeL

dfAnalytical.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .save(deltaTable)

sqlQuery1 = "drop table if exists " + hiveTable
sqlQuery2 = "create table " + hiveTable + " using delta location '" + deltaTable + "'"
spark.sql(sqlQuery1)
spark.sql(sqlQuery2)
