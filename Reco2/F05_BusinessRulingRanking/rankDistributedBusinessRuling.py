# Databricks notebook source
from pyspark.sql import window as w
from pyspark.sql import functions as f
from pyspark.sql import types as t
from datetime import datetime, timedelta

# COMMAND ----------

countryCodeL = getArgument("countryCode").lower()
countryCodeU = countryCodeL.upper()
recoDate = getArgument("recoDate")
countriesNoSalesScores = getArgument("countriesNoSalesScores").split(',')
countriesProductCodeLocal = getArgument("countriesProductCodeLocal").split(',')
countriesBilingual = getArgument("countriesBilingual").split(',')

# COMMAND ----------

# Create empty tables for the countries with no logit scores based on sales data
def create_empty_table(database, tableName, varNames, varTypes):
  structFields = []
  for n, ty in zip(varNames, varTypes):
    structFields.append(t.StructField(n, ty))
  schema = t.StructType(structFields)
  df = sqlContext.createDataFrame(sc.emptyRDD(), schema)
  df.write.mode('overwrite').saveAsTable(database + "." + tableName)

database = 'data_reco_output'

# varNamesSales = [
#   'countryCode', 
#   'operatorOhubId', 
#   'product_code', 
#   'feature', 
#   'value'
# ]
# varNamesGa = [
#   'country_code', 
#   'trackingid', 
#   'product_code', 
#   'feature', 
#   'value'
# ]
# varTypes = [
#   t.StringType(), 
#   t.StringType(),
#   t.StringType(),
#   t.StringType(),
#   t.FloatType()
# ]

varNamesSales = [
  'operatorOhubId', 
  'label', 
  'score'
]
varNamesGa = [
  'trackingid', 
  'label', 
  'score'
]
varTypes = [
  t.StringType(),
  t.StringType(),
  t.FloatType()
]

if countryCodeU in countriesNoSalesScores:
  tableName = countryCodeL + "_scores_logit_sales_" + recoDate
  create_empty_table(database, tableName, varNamesSales, varTypes)
  print('empty sales table created (1)')
else:
  try:
    spark.table('data_reco_output.' + countryCodeL + '_scores_logit_sales_' + recoDate)
  except:
    tableName = countryCodeL + "_scores_logit_sales_" + recoDate
    create_empty_table(database, tableName, varNamesSales, varTypes)
    print('empty sales table created (2)')
  
try:
  spark.table('data_reco_output.' + countryCodeL + '_scores_logit_ga_' + recoDate)
except:
  tableName = countryCodeL + "_scores_logit_ga_" + recoDate
  create_empty_table(database, tableName, varNamesGa, varTypes)
  print('empty ga table created (2)')

# COMMAND ----------

# Set the old recommendations date (two weeks ago)
datetimeObject = datetime.strptime(recoDate, '%Y%m%d')
recoDateOld = datetime.strftime(datetimeObject - timedelta(days=14), '%Y%m%d')
recoDateOld = '20191124'

# Remove previous 2 week old recos
tableName = "ods_" + countryCodeL + "_recommendations_min2wk"
spark.sql("drop table if exists " + database + "." + tableName)

# Load 2 week old recos, if file does not exist, take the file of the week before
try:
  tableNameSource = "ods_" + countryCodeL + "_recommendations_" + recoDateOld
  oldRecommendations = spark.table(database + "." + tableNameSource)
except:
  recoDateOld = datetime.strftime(datetime.strptime(recoDateOld, '%Y%m%d') - timedelta(days=7), '%Y%m%d')
  tableNameSource = "ods_" + countryCodeL + "_recommendations_" + recoDateOld
  try:
    oldRecommendations = spark.table(database + "." + tableNameSource)
  except:
    print('No old recommendations available')
    oldRecommendations = None

if oldRecommendations:
  oldRecommendations \
    .where(f.col('recommendation_id_acm') <= 3) \
    .write \
    .mode('overwrite') \
    .saveAsTable(database + "." + tableName)

# COMMAND ----------

# DBTITLE 1,Load Spark tables
# Load tables
sifuProductDetails  = spark.table('data_sifu.sifu_product_details')
sifuRecipeDetails   = spark.table('data_sifu.sifu_recipe_details')
scoresLogitGA       = spark.table('data_reco_output.' + countryCodeL + '_scores_logit_ga_'    + recoDate)
scoresLogitSales    = spark.table('data_reco_output.' + countryCodeL + '_scores_logit_sales_' + recoDate)
recommendationSet   = spark.table('data_reco_input.' + countryCodeL + '_recommendation_set')
odsRecoContactsIDS  = spark.table('data_reco_input.ods_reco_contacts_ids')

# Product GoogleId Mapping if exists, otherwise use empty dataframe
try:
  menuMapping = spark.table('lead_intelligence_emea.' + countryCodeL + '_reco_sku_google_unit')
except:
  schema = t.StructType([
    t.StructField("googlePlaceId", t.StringType(), True),
    t.StructField("description", t.StringType(), True),
    t.StructField("productCode", t.StringType(), True),
    t.StructField("rationale", t.StringType(), True)
  ])
  menuMapping = spark.createDataFrame(sc.emptyRDD(), schema)
  
# OhubId GoogleId Mapping
ohubGoogleMapping = spark.table('data_datascience_prod.googleplacesmatching')

# COMMAND ----------

# DBTITLE 1,Load menu info
# Products on menu to ohubId mapping
onMenuTable = ohubGoogleMapping.join(menuMapping, on=f.col('googlePlaceId')==f.col('placesId'), how='inner') \
  .select(f.col('ohubId').alias('operatorOhubId'), 'productCode', f.lit(1).alias('on_menu_card')) \
  .filter(f.col('productCode')!='None') \
  .distinct()

# COMMAND ----------

# DBTITLE 1,Load product / recipe info
# For bilingual countries we need to join the product meta data on cuEanCodes or productNumber instead of recipenet code because the productCode is not language agnostic.
if countryCodeU in countriesBilingual and countryCodeU not in countriesProductCodeLocal:
  productCodeToJoinProductDetails = 'cuEanCodes'
elif countryCodeU in countriesBilingual and countryCodeU in countriesProductCodeLocal:
  productCodeToJoinProductDetails = 'productCodeLocal'
else:
  productCodeToJoinProductDetails = 'productCode'

sifuProductDetails = sifuProductDetails \
  .where(f.col('countryCode') == countryCodeU) \
  .withColumnRenamed('url', 'productUrl') \
  .withColumnRenamed('productNumber', 'productCodeLocal') \
  .select(productCodeToJoinProductDetails, 'language', 'productName', 'productUrl')

sifuRecipeDetails = sifuRecipeDetails \
  .withColumnRenamed('url', 'recipeUrl') \
  .where(f.col('countryCode') == countryCodeU) \
  .select('recipeId', 'language', 'recipeName', 'recipeUrl')

# COMMAND ----------

# DBTITLE 1,Load id x product scores
scoresLogitGA = scoresLogitGA \
  .withColumn('label', f.regexp_replace(f.col('label'), 'dep_dum_ppv_', '')) \
  .where(f.col('trackingid').isNotNull() & f.col('score').isNotNull()) \
  .withColumn('label', f.regexp_replace(f.col('label'),'_', '-')) \
  .withColumnRenamed('label', 'productCode') \
  .withColumnRenamed('score', 'affinity') \
  .select('trackingid', 'productCode', 'affinity')

scoresLogitSales = scoresLogitSales \
  .withColumn('label', f.regexp_replace(f.col('label'), 'dep_dum_prs_', '')) \
  .where(f.col('operatorOhubId').isNotNull() & f.col('score').isNotNull()) \
  .withColumnRenamed('label', 'cuEanCodes') \
  .withColumnRenamed('score', 'propensity') \
  .select('operatorOhubId', 'cuEanCodes', 'propensity')

# scoresLogitGA = scoresLogitGA \
#   .withColumn('product_code', f.regexp_replace(f.col('product_code'),'_', '-')) \
#   .withColumnRenamed('product_code', 'productCode') \
#   .withColumnRenamed('value', 'affinity') \
#   .select('trackingid', 'productCode', 'affinity')

# scoresLogitSales = scoresLogitSales \
#   .withColumnRenamed('product_code', 'cuEanCodes') \
#   .withColumnRenamed('value', 'propensity') \
#   .select('operatorOhubId', 'cuEanCodes', 'propensity')

# COMMAND ----------

# DBTITLE 1,Load (digital) whitespace info
odsFeaturesProductWhitespotsGA = spark.table('data_reco_feature_tables.ods_features_pageviews') \
  .where(f.col('country_code') == countryCodeU) \
  .where("lower(feature) like '%dep_dum_ppv%'") \
  .select(
    f.col('trackingid'),
    f.col('feature').alias('productCode'),
    f.col('value').alias('viewed'),
  ) \
  .distinct() \
  .withColumn('productCode', f.regexp_replace('productCode', 'dep_dum_ppv_', '')) \
  .withColumn('digital_whitespot', 1 - f.col('viewed'))

odsFeaturesProductWhitespotsSales = spark.table('data_reco_input.ods_datascience_sales_max_corr_filtered') \
  .where(f.col('countryCode') == countryCodeU) \
  .where(f.date_format(f.col('transactionDate'),'yyyyMM') > f.col('max_yearmonth_min6')) \
  .select(
    f.col('operatorOhubId'),
    f.col('cuEanCode').alias('cuEanCodes'),
    f.lit(1).alias('ordered'),
    f.lit(0).alias('whitespot')
  ) \
  .distinct()

# COMMAND ----------

# Link product recommendations to businesses without business type (either because the business channel is unknow)
recommendationSetComplete = recommendationSet \
  .where(f.col('channelBased') == 1) \
  .select('operatorOhubId', 'Local_channel', 'productCode', 'cuEanCodes', 'productCodeLocal', 'recipeId', 'channelBased')

# Only for ES: limit the product to only the products in the current business cycle (we already use here it to avoid big memory usage)
if countryCodeU in ['ES']:
  productCycleFilter = spark.table('data_config.es_product_details').where(f.col('productCode').isNotNull()).where(f.lower(f.col('ActivationCycleC4'))== 'yes')
  recipeCycleFilter  = spark.table('data_config.es_recipe_details').where(f.col('recipeId').isNotNull()).where(f.lower(f.col('C4')) == 'yes')
  lstProductCycleFilter = productCycleFilter.select('cuEanCodes').distinct().rdd.map(lambda r: r[0]).collect()
  lstRecipeCycleFilter  = recipeCycleFilter.select('recipeId').distinct().rdd.map(lambda r: r[0]).collect()
  print(lstProductCycleFilter)
  print(lstRecipeCycleFilter)
  recommendationSetComplete = recommendationSetComplete \
    .where(f.col("cuEanCodes").isin(lstProductCycleFilter)) \
    .where(f.col("recipeId").isin(lstRecipeCycleFilter))

# This is the productset that is used in the reco set for businesses with a local channel and in the right season
recosAddition = recommendationSetComplete \
  .select('productCode', 'cuEanCodes', 'productCodeLocal', 'recipeId') \
  .distinct()

# Cross join all operators with no products linked to 
recommendationSetAddition = recommendationSet \
  .where(f.col('channelBased') == 0) \
  .select('operatorOhubId', 'Local_channel', 'channelBased') \
  .distinct() \
  .crossJoin(recosAddition) \
  .select('operatorOhubId', 'Local_channel', 'productCode', 'cuEanCodes', 'productCodeLocal', 'recipeId', 'channelBased') \

# Union recommendations from the recommendationSet if reco's where available and the ops without channel reco based on the set used for other operators
recommendationSetFull = recommendationSetComplete \
  .union(recommendationSetAddition) \
  .distinct()

# COMMAND ----------

# When there is no business or contact attached to a contact we do not know the business type (we only know trackingid), therefore, we crossjoin those cases with all products
odsRecoContactsIDSAnonymous = odsRecoContactsIDS \
  .where(f.col('countryCode') == countryCodeU) \
  .where(f.col('operatorOhubId').isNull()) \
  .select('countryCode', 'integrated_id', 'trackingid').distinct()

odsRecoContactsIDSIdentified = odsRecoContactsIDS \
  .where(f.col('countryCode') == countryCodeU) \
  .where(f.col('operatorOhubId').isNotNull()) \
  .select('countryCode', 'integrated_id', 'trackingid', 'operatorOhubId', 'contactpersonOhubId', 'emailAddress', 'emailId', 'userid') \
  .distinct() 

# Everything empty except productcode, cueancodes, productcodelocal and recipeid --> all products
recommendationSetSelect = recommendationSetFull \
  .withColumn('operatorOhubId', f.lit(None).cast('string')) \
  .withColumn('Local_channel', f.lit(None).cast('string')) \
  .withColumn('contactpersonOhubId', f.lit(None).cast('string')) \
  .withColumn('emailAddress', f.lit(None).cast('string')) \
  .withColumn('emailId', f.lit(None).cast('string')) \
  .withColumn('userid', f.lit(None).cast('string')) \
  .select('operatorOhubId', 'Local_channel', 'productCode', 'cuEanCodes', 'productCodeLocal', 'recipeId', 'contactpersonOhubId', 'emailAddress', 'emailId', 'userid', 'channelBased') \
  .distinct()

# Cross join all anonymous visitors to ufs.com with all relevant product / recipe sets
tmp0aOdsRecommendations = recommendationSetSelect \
  .crossJoin(odsRecoContactsIDSAnonymous) \
  .select('integrated_id', 'operatorOhubId', 'Local_channel', 'productCode', 'cuEanCodes', 'productCodeLocal', 'recipeId', 'contactpersonOhubId', 'emailAddress', 'trackingid', 'emailId', 'userid', 'channelBased') \
  .distinct() 

# Add all contacts person details to the operators with known visitors
tmp0bOdsRecommendations = recommendationSetFull \
  .join(odsRecoContactsIDSIdentified, ['operatorOhubId'], 'inner') \
  .select('integrated_id', 'operatorOhubId', 'Local_channel', 'productCode', 'cuEanCodes', 'productCodeLocal', 'recipeId', 'contactpersonOhubId', 'emailAddress', 'trackingid', 'emailId', 'userid', 'channelBased')

# Union recommendations for identified operators / contacts and anonymous ufs.com visitors
tmp0cOdsRecommendations = tmp0aOdsRecommendations \
  .union(tmp0bOdsRecommendations) \
  .distinct() \
  .select('integrated_id', 'operatorOhubId', 'Local_channel', 'productCode', 'cuEanCodes', 'productCodeLocal', 'recipeId', 'contactpersonOhubId', 'emailAddress', 'trackingid', 'emailId', 'userid', 'channelBased')

# COMMAND ----------

# DBTITLE 1,Join the affinity and propensity scores, product details, recipe details, and on menu information
tmp1OdsRecommendations = tmp0cOdsRecommendations \
  .join(odsFeaturesProductWhitespotsSales, on=['operatorOhubId', 'cuEanCodes'], how='left') \
  .join(odsFeaturesProductWhitespotsGA, on=['trackingid', 'productCode'], how='left') \
  .withColumn('ordered', f.when(f.col('ordered').isNotNull(), f.col('ordered')).otherwise(0)) \
  .withColumn('whitespot', f.when(f.col('whitespot').isNotNull(), f.col('whitespot')).otherwise(1)) \
  .withColumn('digital_whitespot', f.when(f.col('digital_whitespot').isNotNull(), f.col('digital_whitespot')).otherwise(1)) \
  .select('integrated_id', 'operatorOhubId', 'Local_channel', 'productCode', 'cuEanCodes', 'productCodeLocal', 'recipeId', 'contactpersonOhubId', 'emailAddress', 'trackingid', 'emailId', 'userid', 'viewed', 'ordered', 'whitespot', 'digital_whitespot', 'channelBased') \
  .distinct()

if countryCodeU in countriesProductCodeLocal:
  tmp1OdsRecommendations = tmp1OdsRecommendations.where(f.col('productCodeLocal').isNotNull())
else:
  tmp1OdsRecommendations = tmp1OdsRecommendations.where(f.col('cuEanCodes').isNotNull())

# All countries use recipenet code in url, except for the DACH countries. DACH uses PANEER instead of Recipenet and therefor includes the cueancode within the url
if countryCodeU in ['AT', 'CH', 'DE']:
  scoresLogitGA = scoresLogitGA.withColumnRenamed('productCode', 'cuEanCodes')
  productCodeToJoinGaScores = 'cuEanCodes'
else:
  productCodeToJoinGaScores = 'productCode'
  
tmp2OdsRecommendations = tmp1OdsRecommendations \
  .join(scoresLogitGA, ['trackingid', productCodeToJoinGaScores], 'left') \
  .join(scoresLogitSales, ['operatorOhubId', 'cuEanCodes'], 'left') \
  .join(sifuProductDetails, [productCodeToJoinProductDetails], 'inner') \
  .join(sifuRecipeDetails, ['recipeId', 'language'], 'inner') \
  .join(onMenuTable, ['operatorOhubId', 'productCode'], 'left') \
  .withColumn('emailAddressOrOhubId', f.when((f.col('emailAddress').isNotNull() & (f.col('emailAddress') != '')), f.col('emailAddress')).otherwise(f.col('operatorOhubId'))) \
  .withColumn('integrated_reco_id', f.when(f.col('operatorOhubId').isNotNull(), f.col('emailAddressOrOhubId')).otherwise(f.col('trackingid'))) \
  .withColumn('affinity', f.when(f.col('affinity').isNotNull(), f.col('affinity')).otherwise(0)) \
  .withColumn('propensity', f.when(f.col('propensity').isNotNull(), f.col('propensity')).otherwise(0)) \
  .withColumn('on_menu_card', f.when(f.col('on_menu_card').isNotNull(), f.col('on_menu_card')).otherwise(0)) \
  .withColumn('personalized', f.when((f.col('propensity') != 0) | (f.col('affinity') != 0) | (f.col('on_menu_card') != 0), 1).otherwise(0)) \
  .withColumn('random_recipe_affinity', f.rand()) \
  .withColumn('random_product_affinity', f.rand()) \
  .withColumn('whitespot', f.when(f.col('whitespot').isNotNull(), f.col('whitespot')).otherwise(1)) \
  .withColumn('digital_whitespot', f.when(f.col('digital_whitespot').isNotNull(), f.col('digital_whitespot')).otherwise(1)) \
  .where(f.col('whitespot') == 1) \
  .select('operatorOhubId', 'contactpersonOhubId', 'emailAddress', 'integrated_reco_id', 'local_channel', 'productCode', 'language', 'productName', 'productUrl', 'recipeName', 'recipeUrl', 'cuEanCodes', 'productCodeLocal', 'recipeId', 'userid', 'trackingid', 'emailid', 'personalized', 'affinity', 'propensity', 'random_recipe_affinity', 'random_product_affinity', 'whitespot', 'digital_whitespot', 'on_menu_card', 'channelBased')

#   .withColumn('personalized', f.when(f.col('trackingid').isNotNull(), 1).otherwise(0)) \

# COMMAND ----------

# DBTITLE 1,Exclude Recommendations from two weeks before.
if oldRecommendations:
  try:
    if countryCodeU in countriesProductCodeLocal:
      tmp2OdsRecommendations = tmp2OdsRecommendations.join(oldRecommendations, how = 'left_anti', on = ['operatorOhubId', 'productCodeLocal'])
    else:
      tmp2OdsRecommendations = tmp2OdsRecommendations.join(oldRecommendations, how = 'left_anti', on = ['operatorOhubId', 'cuEanCodes'])
  except Exception as e:
    print(e)
else:
  print('No old recommendations available')

# COMMAND ----------

# DBTITLE 1,Choose the best three recommendations.
# Deduplication on operatorId x emailId x trackingId
w0 = w.Window \
  .partitionBy(f.col('integrated_reco_id'), 
               f.col('language'), 
               f.col('productCode')) \
  .orderBy(f.col('propensity').desc(), 
           f.col('affinity').desc(), 
           f.col('on_menu_card').desc(),
           f.col('random_recipe_affinity').desc())

w1 = w.Window \
  .partitionBy(f.col('integrated_reco_id_acm'), 
               f.col('language'), 
               f.col('productCode')) \
  .orderBy(f.col('propensity').desc(),
           f.col('affinity').desc(),
           f.col('on_menu_card').desc(), 
           f.col('random_recipe_affinity').desc(), 
           f.col('random_product_affinity').desc())

w2 = w.Window \
  .partitionBy(f.col('integrated_reco_id_acm'), 
               f.col('language')) \
  .orderBy(f.col('propensity').desc(), 
           f.col('affinity').desc(), 
           f.col('on_menu_card').desc(), 
           f.col('random_product_affinity').desc(), 
           f.col('productCode').desc())

w3 = w.Window \
  .partitionBy(f.col('integrated_reco_id_ufs_com'), f.col('language')) \
  .orderBy(f.col('propensity').desc(), 
           f.col('affinity').desc(), 
           f.col('on_menu_card').desc(), 
           f.col('random_product_affinity').desc(), 
           f.col('productCode').desc())

tmp3OdsRecommendations = tmp2OdsRecommendations \
  .withColumn('recipeRank', f.dense_rank().over(w0)) \
  .where(f.col('recipeRank') == 1) \
  .withColumn('trackingidNotNull', f.when(f.col('trackingid').isNotNull(), f.col('trackingid')).otherwise('')) \
  .withColumn('integrated_reco_id_acm', f.when(f.col('operatorOhubId').isNotNull(), f.col('operatorOhubId')).otherwise(f.col('trackingidNotNull'))) \
  .withColumn('emailAddressOrOhubId', f.when(f.col('emailAddress').isNotNull(), f.col('emailAddress')).otherwise(f.col('operatorOhubId'))) \
  .withColumn('emailAddressOrOhubIdNotNull', f.when(f.col('emailAddressOrOhubId').isNotNull(), f.col('emailAddressOrOhubId')).otherwise('')) \
  .withColumn('integrated_reco_id_ufs_com', f.when(f.col('operatorOhubId').isNotNull(), f.concat(f.concat(f.col('emailAddressOrOhubIdNotNull'), f.lit(' / ')), f.col('trackingidNotNull'))).otherwise(f.col('trackingidNotNull'))) \
  .withColumn('rnk_rec_opr', f.dense_rank().over(w1)) \
  .where(f.col('rnk_rec_opr') == 1) \
  .withColumn('recommendation_id_acm', f.dense_rank().over(w2)) \
  .withColumn('recommendation_id_ufs_com', f.dense_rank().over(w3)) \
  .select('integrated_reco_id_acm', 'integrated_reco_id_ufs_com', 'operatorOhubId', 'contactpersonOhubId', 'language', 'emailAddress', 'local_channel', 'productCode', 'cuEanCodes', 'productCodeLocal', 'recipeId', 'productName', 'productUrl', 'recipeName', 'recipeUrl', 'userid', 'trackingid', 'emailid', 'personalized', 'affinity', 'propensity', 'random_recipe_affinity', 'random_product_affinity', 'whitespot', 'digital_whitespot','recommendation_id_acm', 'recommendation_id_ufs_com', 'on_menu_card', 'channelBased')

# COMMAND ----------

tmp3OdsRecommendations.write \
  .mode("overwrite") \
  .saveAsTable('data_reco_output.ods_' + countryCodeL + '_recommendations_' + recoDate)
