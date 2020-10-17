# Databricks notebook source
from pyspark.sql import functions as f
from pyspark.sql import window as w
from pyspark.sql import types as t
from datetime import datetime, timedelta

countryCodeL = getArgument("countryCode").lower()
countryCodeU = countryCodeL.upper()
recoDate = getArgument("recoDate")
countriesProductCodeLocal = getArgument("countriesProductCodeLocal").split(',')
countriesBilingual = getArgument("countriesBilingual").split(',')

# COMMAND ----------

# Set the old recommendations date (two weeks ago)
datetimeObject = datetime.strptime(recoDate, '%Y%m%d')
recoDateOld = datetime.strftime(datetimeObject - timedelta(days=14), '%Y%m%d')
recoDateOld = '20191124'
# Remove previous 2 week old recos
database = 'data_reco_output'
tableName = "ods_" + countryCodeL + "_recommendations_min2wk"
spark.sql("DROP TABLE IF EXISTS " + database + "." + tableName)

# Load 2 week old recos, if file does not exist, take the file of 3 weeks ago
tableNameSource = "ods_" + countryCodeL + "_recommendations_" + recoDateOld
try:
  dfOldRecommendations = spark.table(database + "." + tableNameSource)
except:
  recoDateOld = datetime.strftime(datetime.strptime(recoDateOld, '%Y%m%d') - timedelta(days=7), '%Y%m%d')
  tableNameSource = "ods_" + countryCodeL + "_recommendations_" + recoDateOld
  try:
    dfOldRecommendations = spark.table(database + "." + tableNameSource) \
      .where(f.col('recommendation_id_acm') <= 3)
  except:
    dfOldRecommendations = None

# Save old reco's as 2 week old reco's. If there al no old reco's, do nothing.
if dfOldRecommendations:
  dfOldRecommendations.write.mode('overwrite').saveAsTable(database + "." + tableName)
else:
  pass

# COMMAND ----------

# Join the operators with all products with a recipe linked to it
dfOperators = spark.table('data_reco_input.ods_reco_contacts_IDS') \
  .where(f.col('countryCode') == countryCodeU) \
  .select('countryCode', 'operatorOhubId', 'trackingid', 'local_channel', 'emailAddress', 'userId')
dfProducts = spark.table('data_sifu.sifu_product_details') \
  .where(f.col('countryCode') == countryCodeU) \
  .select('cuEanCodes', 'language', 'productCode', f.col('productNumber').alias('productCodeLocal'), 'countryCode', 'productName', f.col('url').alias('productUrl'))
dfRecipes = spark.table('data_sifu.sifu_recipe_details_long') \
  .where(f.col('countryCode') == countryCodeU) \
  .withColumnRenamed('productCodeLong', 'productCode')
# Only take one recipe per product
dfProductsWithRecipes = dfProducts \
  .join(dfRecipes, ['countryCode', 'language', 'productCode'], 'inner') \
  .select('cuEanCodes', 'language', 'productCode', 'productCodeLocal', 'productName', 'productUrl', 'recipeId', 'recipeName', f.col('url').alias('recipeUrl')) 
dfRecommendationSet = dfOperators.crossJoin(dfProductsWithRecipes)

# COMMAND ----------

try:
  dfScores = spark.table('data_reco_output.' + countryCodeL + '_scores_logit_ga_' + recoDate) \
    .withColumn('label', f.regexp_replace(f.col('label'), 'dep_dum_ppv_', '')) \
    .where(f.col('trackingid').isNotNull() & f.col('score').isNotNull()) \
    .withColumn('label', f.regexp_replace(f.col('label'),'_', '-')) \
    .withColumnRenamed('label', 'productCode') \
    .withColumnRenamed('score', 'affinity') \
    .select('trackingid', 'productCode', 'affinity')
  scoresAvailable = True
except:
  scoresAvailable = False

# COMMAND ----------

if scoresAvailable:
  dfRecommendationSetScores = dfRecommendationSet \
    .join(dfScores, ['trackingid', 'productCode'], 'left') \
    .withColumn('affinity', f.when(f.col('affinity').isNotNull(), f.col('affinity')).otherwise(0))
else:
  dfRecommendationSetScores = dfRecommendationSet \
    .withColumn('affinity', f.lit(0))

# COMMAND ----------

# MAGIC %md ### Determine Digital Whitespots

# COMMAND ----------

# Create a set of trackingid with the products they already viewed
odsFeaturesProductViewedGa = spark.table('data_reco_feature_tables.ods_features_pageviews') \
  .select(f.col('country_code').alias('countryCode'),
          f.col('trackingid'),
          f.col('feature').alias('productCode'),
          f.col('value').alias('viewed')) \
  .where(f.col('countryCode') == countryCodeU) \
  .where(f.lower(f.col('productCode')).like('%dep_dum_ppv%')) \
  .withColumn('productCode', f.regexp_replace('productCode', 'dep_dum_ppv_', '')) \
  .withColumn('productCode', f.regexp_replace('productCode', '_', '-')) \
  .withColumn('digitalWhitespot', 1 - f.col('viewed')) \
  .where(f.col('digitalWhitespot') == 0) \
  .where(f.col('trackingId').isNotNull()) \
  .where(f.col('productCode').isNotNull()) \
  .select('countryCode', 'trackingid', 'productCode', 'viewed')

# COMMAND ----------

# Remove viewed products
dfRecommendationSetWs = dfRecommendationSetScores \
  .join(odsFeaturesProductViewedGa, ['countryCode', 'trackingid', 'productCode'], 'left_anti') \
  .withColumn('whitespot', f.lit(1))

# COMMAND ----------

# Remove old recommendations if they exist
if dfOldRecommendations:
  if countryCodeU in countriesProductCodeLocal:
    dfRecommendationSetWs = dfRecommendationSetWs.join(dfOldRecommendations, how = 'left_anti', on = ['operatorOhubId', 'productCodeLocal'])
  else:
    dfRecommendationSetWs = dfRecommendationSetWs.join(dfOldRecommendations, how = 'left_anti', on = ['operatorOhubId', 'cuEanCodes'])
else:
  pass

# COMMAND ----------

# Create a integrated ID for ufs.com and ACM
dfRecommendationSetWsId = dfRecommendationSetWs \
  .withColumn('trackingidNotNull', f.when(f.col('trackingid').isNotNull(), f.col('trackingid')).otherwise('')) \
  .withColumn('integrated_reco_id_acm', f.when(f.col('operatorOhubId').isNotNull(), f.col('operatorOhubId')).otherwise(f.col('trackingidNotNull'))) \
  .withColumn('emailAddressOrOhubId', f.when(f.col('emailAddress').isNotNull(), f.col('emailAddress')).otherwise(f.col('operatorOhubId'))) \
  .withColumn('emailAddressOrOhubIdNotNull', f.when(f.col('emailAddressOrOhubId').isNotNull(), f.col('emailAddressOrOhubId')).otherwise('')) \
  .withColumn('integrated_reco_id_ufs_com', f.when(f.col('operatorOhubId').isNotNull(), f.concat(f.concat(f.col('emailAddressOrOhubIdNotNull'), f.lit(' / ')), f.col('trackingidNotNull'))).otherwise(f.col('trackingidNotNull')))

# COMMAND ----------

# Select randomly one recipe per product using w1
w1 = w.Window \
  .partitionBy(f.col('integrated_reco_id_acm'), f.col('language'), f.col('productCode')) \
  .orderBy(f.col('randomRecipeAffinity').desc())
# Rank products based on affinity to click for both acm and ufs.com
w2 =  w.Window \
  .partitionBy(f.col('integrated_reco_id_acm'), f.col('language')) \
  .orderBy(f.col('affinity').desc(), f.col('randomProductAffinity').desc())
w3 = w.Window \
  .partitionBy(f.col('integrated_reco_id_ufs_com'), f.col('language')) \
  .orderBy(f.col('affinity').desc(), f.col('randomProductAffinity').desc())

dfRecommendationSetWsIdRanked = dfRecommendationSetWsId \
  .withColumn('randomRecipeAffinity', f.rand()) \
  .withColumn('recipeRank', f.dense_rank().over(w1)) \
  .where(f.col('recipeRank') == 1) \
  .withColumn('randomProductAffinity', f.rand()) \
  .withColumn('recommendation_id_acm', f.dense_rank().over(w2)) \
  .withColumn('recommendation_id_ufs_com', f.dense_rank().over(w3)) \
  .withColumn('recommendationType', f.lit('PRODUCT')) \
  .withColumn('personalized', f.when((f.col('affinity') != 0), 1).otherwise(0)) \
  .withColumn('channelBased', f.lit(0)) \
  .select('recommendation_id_acm',
          'recommendation_id_ufs_com',
          'operatorOhubId', 
          'userId', 
          'trackingId',
          'integrated_reco_id_acm',
          'integrated_reco_id_ufs_com',
          'recommendationType', 
          'countryCode', 
          'language', 
          'productCode', 
          'productCodeLocal',
          'productName',
          'productUrl',
          'recipeId',
          'recipeName',
          'recipeUrl',
          'cuEanCodes',
          f.col('affinity').alias('propensity'),
          'local_channel',
          'personalized',
          'whitespot', 
          'channelBased')

# COMMAND ----------

dfRecommendationSetWsIdRanked.write \
  .mode("Overwrite") \
  .saveAsTable('data_reco_output.ods_' + countryCodeL + '_recommendations_' + recoDate)
