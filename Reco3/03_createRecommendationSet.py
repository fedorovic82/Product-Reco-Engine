# Databricks notebook source
from pyspark.sql import functions as f
from pyspark.sql import window as w
from pyspark.sql import types as t
from datetime import datetime, timedelta
from delta import tables
import math

# COMMAND ----------

countryCodeL = getArgument("countryCode").lower()
countriesNoSalesScores = getArgument("countriesNoSalesScores").split(',')
countriesProductCodeLocal = getArgument("countriesProductCodeLocal").split(',')
countriesBusinessRuling = getArgument("countriesBusinessRuling").split(',')

# countryCodeL = 'ph'
# countriesNoSalesScores = []
# countriesProductCodeLocal = []
# countriesBusinessRuling = []

countryCodeU = countryCodeL.upper()

# COMMAND ----------

print(countryCodeU)

# COMMAND ----------

# Product and recipe details
sifuProductDetails = spark.table('dev_sources_sifu.raw_product_details') \
  .where(f.col('countryCode') == countryCodeU) \
  .where(f.col('language').isNotNull() & f.col('productCode').isNotNull())
sifuRecipeDetails = spark.table('dev_sources_sifu.raw_recipe_details') \
  .where(f.col('countryCode') == countryCodeU) \
  .where(f.col('recipeId').isNotNull() & f.col('language').isNotNull()) \
  .where(f.lower(f.col('previewImageUrl')) != 'none')
sifuRecipeDetailsLong = spark.table('dev_sources_sifu.cleaned_recipe_details_long') \
  .where(f.col('countryCode') == countryCodeU) \
  .where(f.col('recipeId').isNotNull() & f.col('language').isNotNull() & f.col('productCodeLong').isNotNull())

dfOperatorsWithActiveSales = spark.table('dev_derived_reco.sales_operators_active') \
  .where(f.col('countryCode') == countryCodeU) \
  .where(f.col('operatorOhubId').isNotNull())
dfContactsIds = spark.table('dev_derived_integration_ids.output_contacts_ids') \
  .where(f.col('countryCode') == countryCodeU) \
  .where(f.col('operatorOhubId').isNotNull() | f.col('trackingId').isNotNull())
# operators table for channel information, set Others channel to null.
dfOperatorsChannels = spark.table('dev_derived_integration_ids.output_operators') \
  .where(f.col('countryCode') == countryCodeU) \
  .where(f.col('ohubId').isNotNull()) \
  .select(f.col('ohubId').alias('operatorOhubId'), f.col('local_channel').alias('localChannel')) \
  .withColumn('localChannel', f.when(f.lower(f.col('localChannel')).isin(['other', 'all other', 'others']), None).otherwise(f.col('localChannel'))) \
  .distinct()
dfOperatorsWithContact = spark.table('dev_derived_integration_ids.output_operators_withcontact') \
  .where(f.col('countryCode') == countryCodeU) \
  .where(f.col('operatorOhubId').isNotNull())

scoresLogitGa = spark.table("dev_derived_reco.scores_ga_logit") \
  .where(f.col('countryCode') == countryCodeU) \
  .where(f.col('trackingId').isNotNull() & f.col('label').isNotNull() & f.col('score').isNotNull()) 
scoresLogitSales = spark.table("dev_derived_reco.scores_sales_logit") \
  .where(f.col('countryCode') == countryCodeU) \
  .where(f.col('operatorOhubId').isNotNull() & f.col('label').isNotNull() & f.col('score').isNotNull())
menuMapping = spark.table('dev_derived_omenu.output_reco_sku_google_unit') \
  .where(f.col('countryCode') == countryCodeU) \
  .where(f.col('googlePlaceId').isNotNull() & (f.col('productCode')!='None') & f.col('productCode').isNotNull())

dfGaFactHits = spark.table('dev_derived_reco.ga_fact_hits_max_corrected') \
  .where(f.col('countryCode') == countryCodeU) \
  .where(f.col('trackingId').isNotNull() & f.col('productCode').isNotNull() & (f.col('productCode') != ""))
dfSales = spark.table('dev_derived_reco.sales_max_corrected') \
  .where(f.col('countryCode') == countryCodeU) \
  .where((f.col('operatorOhubId').isNotNull()) & (f.col('cuEanCode').isNotNull()))

dfProductViews = spark.table("dev_derived_reco.ga_product_views") \
  .where(f.col('countryCode') == countryCodeU) \
  .where(f.col('productCode').isNotNull()) \
  .select('productCode', 'productViews', 'productViewsNtile')
dfRecipeViews = spark.table("dev_derived_reco.ga_recipe_views") \
  .where(f.col('countryCode') == countryCodeU) \
  .where(f.col('recipeId').isNotNull()) \
  .select('recipeId', 'recipeViews', 'recipeViewsNtile')
dfProductSales = spark.table("dev_derived_reco.sales_product_sales") \
  .where(f.col('countryCode') == countryCodeU) \
  .where(f.col('cuEanCode').isNotNull()) \
  .withColumnRenamed('cuEanCode', 'cuEanCodes') \
  .select('cuEanCodes', 'productSales', 'productSalesNtile')

dfHitsPageMaxCorrected = spark.table('dev_derived_reco.ga_fact_hits_max_corrected') \
  .where(f.col('countryCode') == countryCodeU) \
  .where(f.col('trackingId').isNotNull())
dfUsers = spark.table('dev_derived_reco.ga_users') \
  .where(f.col('countryCode') == countryCodeU) \
  .where(f.col('trackingId').isNotNull())

dfValidationGroups = spark.table("dev_derived_reco.ga_reco_validation_groups") \
  .where(f.col('trackingId').isNotNull()) \
  .select('trackingId', f.col('group').alias('validationGroup'))

productDetailsManuallyEnriched = spark.table('dev_sources_meta.cleaned_product_details') \
  .where(f.col('countryCode') == countryCodeU) \
  .where(f.col('productCode').isNotNull())
recipeDetailsManuallyEnriched = spark.table('dev_sources_meta.cleaned_recipe_details') \
  .where(f.col('countryCode') == countryCodeU) \
  .where(f.col('recipeId').isNotNull())
recipeProductMappingManuallyEnriched = spark.table('dev_sources_meta.cleaned_recipe_product_mapping') \
  .where(f.col('countryCode') == countryCodeU) \
  .where(f.col('productCode').isNotNull()) \
  .where(f.col('recipeId').isNotNull())

# COMMAND ----------

scoresGaAvailable = not scoresLogitGa.rdd.isEmpty()
print(scoresGaAvailable)

scoresSalesAvailable = not scoresLogitSales.rdd.isEmpty()
print(scoresSalesAvailable)

menuFitAvailable = not menuMapping.rdd.isEmpty()
print(menuFitAvailable)

# COMMAND ----------

# DBTITLE 1,Load menu info
if menuFitAvailable:
  # OhubId GoogleId Mapping
  ohubGoogleMapping = spark.table('dev_derived_integration_ids.output_gpl_ohub_matching') \
    .withColumnRenamed('ohubId', 'operatorOhubId') \
    .withColumnRenamed('placesId', 'googlePlaceId') \
    .where(f.col('operatorOhubId').isNotNull()) \
    .where(f.col('googlePlaceId').isNotNull()) \
    .select('operatorOhubId', 'googlePlaceId') \
    .distinct()
  # Products on menu to ohubId mapping
  onMenuTable = menuMapping \
    .join(ohubGoogleMapping, on='googlePlaceId', how='inner') \
    .select('operatorOhubId', 'productCode', f.lit(1).alias('onMenuCard')) \
    .distinct()
else:
  schema = t.StructType([
    t.StructField("operatorOhubId", t.StringType(), True),
    t.StructField("productCode", t.StringType(), True),
    t.StructField("onMenuCard", t.IntegerType(), True)
  ])
  onMenuTable = spark.createDataFrame(sc.emptyRDD(), schema)

# COMMAND ----------

def get_min_max_partition_size(df):
  print("Number of partitions: " + str(df.rdd.getNumPartitions()))
  l = df.rdd.mapPartitionsWithIndex(lambda x,it: [(x,sum(1 for _ in it))]).collect()
  print(min(l, key=lambda item:item[1]))
  print(max(l, key=lambda item:item[1]))

# COMMAND ----------

# DBTITLE 1,Select the right product and recipe information
sifuProductDetails = sifuProductDetails \
  .withColumnRenamed('url', 'productUrl') \
  .withColumnRenamed('productNumber', 'productCodeLocal') \
  .withColumn('productImageUrl', f.when(((f.col('previewImageUrl').isNotNull()) & (f.col('previewImageUrl')!='')), f.col('previewImageUrl')).otherwise('imageUrl')) \
  .select('cuEanCodes', 'productCode', 'productCodeLocal', 'language', 'productName', 'productUrl', 'productImageUrl') \
  .distinct()
print(sifuProductDetails.count())

# Filter out null values on column used in join lateron
if countryCodeU in countriesProductCodeLocal:
  productCodeToJoinOn = 'productCodeLocal'
else:
  productCodeToJoinOn = 'cuEanCodes'
  sifuProductDetails = sifuProductDetails \
    .where(f.col('cuEanCodes').cast('double').isNotNull())
sifuProductDetails = sifuProductDetails \
  .where((f.col(productCodeToJoinOn).isNotNull()) & (f.lower(f.col(productCodeToJoinOn)) != 'none'))
print(sifuProductDetails.count())

# Deduplicate on productCodeToJoinOn and productCode
wP1 = w.Window \
  .partitionBy(productCodeToJoinOn, 'language') \
  .orderBy(f.col('productCode').desc(), f.col('productUrl').desc(), f.col('productName').desc())  
wP2 = w.Window \
  .partitionBy('productCode', 'language') \
  .orderBy(f.col(productCodeToJoinOn).desc(), f.col('productUrl').desc(), f.col('productName').desc())  
sifuProductDetails = sifuProductDetails \
  .withColumn('productRank1', f.row_number().over(wP1)) \
  .where(f.col('productRank1') == 1) \
  .drop('productRank1') \
  .withColumn('productRank2', f.row_number().over(wP2)) \
  .where(f.col('productRank2') == 1) \
  .drop('productRank2')
print(sifuProductDetails.count())

# Product Recipe Mapping SIFU
sifuRecipeDetailsLong = sifuRecipeDetailsLong \
  .withColumnRenamed('url', 'recipeUrl') \
  .withColumnRenamed('productCodeLong', 'productCode') \
  .withColumn('manualMapping', f.lit(0)) \
  .select('recipeId', 'language', 'recipeName', 'recipeUrl', 'productCode', 'manualMapping') \
  .distinct()

wR = w.Window \
  .partitionBy('recipeId', 'language', 'productCode') \
  .orderBy(f.col('manualMapping').desc(), f.col('recipeUrl').desc(), f.col('recipeName').desc())  

if countryCodeU in countriesBusinessRuling:
  # Product Recipe Mapping Manually Enriched
  recipeProductMappingManuallyEnriched = recipeProductMappingManuallyEnriched \
    .withColumn('productCode', f.upper(f.col('productCode'))) \
    .withColumn('recipeId', f.upper(f.col('recipeId'))) \
    .withColumn('language', f.lower(f.col('language'))) \
    .select('recipeId', 'language', 'productCode') \
    .join(sifuRecipeDetails.withColumnRenamed('url', 'recipeUrl'), ['recipeId', 'language'], 'inner') \
    .withColumn('manualMapping', f.lit(1)) \
    .select('recipeId', 'language', 'recipeName', 'recipeUrl', 'productCode', 'manualMapping') \
    .distinct()

  # Only keep recipes with a product linked to it
  # Deduplicate on recipeId x productCode
  sifuRecipeDetailsLongFull = sifuRecipeDetailsLong \
    .union(recipeProductMappingManuallyEnriched) \
    .join(sifuRecipeDetails.withColumnRenamed('previewImageUrl', 'recipeImageUrl').select('recipeId', 'language', 'recipeImageUrl').distinct(), ['recipeId', 'language'], 'inner') \
    .where(f.col('productCode').isNotNull()) \
    .withColumn('recipeRank1', f.row_number().over(wR)) \
    .where(f.col('recipeRank1') == 1) \
    .drop('recipeRank1') \
    .distinct()

  def getSeasonFromDate():
    currentDateTime = datetime.now()
    currentmonth = currentDateTime.month
    season = ''
    if currentmonth >= 3 and currentmonth < 6:
      season = 'spring'
    elif currentmonth >= 6 and currentmonth < 9:
      season = 'summer'
    elif currentmonth >= 9 and currentmonth < 12:
      season = 'autumn'
    else:
      season = 'winter'
    return season

  def getCycleFromDate():
    dayOfYear = datetime.now().timetuple().tm_yday
    cycleNrTmp = math.floor(dayOfYear / (365 / 5)) + 1
    cycleNr = min(cycleNrTmp, 5)
    return cycleNr
  
  season = getSeasonFromDate()
  print(season)

  cycleNr = getCycleFromDate()
  print(cycleNr)

  # These cycles are used by ES (Spain) only.
  productActivationCycleCol = 'ActivationCycleC' + str(cycleNr)
  recipeActivationCycleCol = 'C' + str(cycleNr)
  
  # Product business channel details
  productDetailsManuallyEnrichedT = productDetailsManuallyEnriched \
    .withColumn('productCode', f.upper(f.col('productCode'))) \
    .withColumn('productBusinessChannelRelevant', f.col('RelevantBusinessChannel')) \
    .withColumn('productCycleActive', f.when(f.lower(f.col(productActivationCycleCol)).contains('yes'), True).otherwise(False)) \
    .select('productCode', 'productBusinessChannelRelevant', 'productCycleActive') \
    .dropDuplicates(['productCode']) \
    .distinct()

  # Recipe season details 
  recipeDetailsManuallyEnrichedFiltered = recipeDetailsManuallyEnriched \
    .withColumn('recipeId', f.upper(f.col('recipeId'))) \
    .withColumn('recipeSeasonRelevant', f.when(f.lower(f.col('relevantSeason')).contains(season.lower()), True).otherwise(False)) \
    .withColumn('recipeCycleActive', f.when(f.lower(f.col(recipeActivationCycleCol)).contains('yes'), True).otherwise(False)) \
    .select('recipeId', 'recipeSeasonRelevant', 'recipeCycleActive') \
    .dropDuplicates(['recipeId']) \
    .distinct()

  # All products, possible with recipes linked
  productsWithRecipes = sifuProductDetails \
    .join(sifuRecipeDetailsLongFull, ['language', 'productCode'], 'left') \
    .join(productDetailsManuallyEnrichedT, ['productCode'], 'left') \
    .join(recipeDetailsManuallyEnrichedFiltered, ['recipeId'], 'left') \
    .select('cuEanCodes', 'language', 'productCode', 'productCodeLocal', 'productName', 'productUrl', 'productImageUrl', 'recipeId', 'recipeName', 'recipeUrl', 'recipeImageUrl', 'manualMapping', 'productBusinessChannelRelevant', 'recipeSeasonRelevant', 'productCycleActive', 'recipeCycleActive') 
else:
  # Only keep recipes with a product linked to it
  # Deduplicate on recipeId x productCode
  sifuRecipeDetailsLongFull = sifuRecipeDetailsLong \
    .where(f.col('productCode').isNotNull()) \
    .join(sifuRecipeDetails.withColumnRenamed('previewImageUrl', 'recipeImageUrl').select('recipeId', 'language', 'recipeImageUrl').distinct(), ['recipeId', 'language'], 'inner') \
    .withColumn('recipeRank1', f.row_number().over(wR)) \
    .where(f.col('recipeRank1') == 1) \
    .drop('recipeRank1') \
    .distinct()

  # Join the operators with all products with a recipe linked to it
  productsWithRecipes = sifuProductDetails \
    .join(sifuRecipeDetailsLongFull, ['language', 'productCode'], 'left') \
    .withColumn('productBusinessChannelRelevant', f.lit(None).cast('string')) \
    .withColumn('recipeSeasonRelevant', f.lit(None).cast('boolean')) \
    .withColumn('productCycleActive', f.lit(None).cast('boolean')) \
    .withColumn('recipeCycleActive', f.lit(None).cast('boolean')) \
    .select('cuEanCodes', 'language', 'productCode', 'productCodeLocal', 'productName', 'productUrl', 'productImageUrl', 'recipeId', 'recipeName', 'recipeUrl', 'recipeImageUrl', 'manualMapping', 'productBusinessChannelRelevant', 'recipeSeasonRelevant', 'productCycleActive', 'recipeCycleActive')

# COMMAND ----------

# DBTITLE 1,Create Operator set to create recommendations for
# Operator set contains
# 1) Operators with active sales
# 2) Operators with contact information
# 3) Operators with menu data
# 4) Operators with a trackingId
# 5) Users with a trackingId
   
# Operators from Ohub
dfOperatorsOhubWithActiveSales = dfOperatorsWithActiveSales \
  .select('operatorOhubId') \
  .distinct()
dfOperatorsOhubWithMenu = onMenuTable \
  .select('operatorOhubId') \
  .distinct()
dfOperatorsOhubWithContact = dfOperatorsWithContact \
  .select('operatorOhubId') \
  .distinct()
dfOperatorsOhubContacts = dfContactsIds \
  .select('operatorOhubId') \
  .distinct()

# Create operatorOhubId to localChannel mapping with indicator
if countryCodeU in countriesBusinessRuling:
  channelsWithManualRecos = productDetailsManuallyEnrichedT \
    .select('productBusinessChannelRelevant') \
    .withColumn('operatorHasChannelBased', f.lit(1)) \
    .distinct()
  dfOperatorsChannelsHasReco = dfOperatorsChannels \
    .join(channelsWithManualRecos, f.lower(f.col('productBusinessChannelRelevant')).contains(f.lower(f.col('localChannel'))), 'left') \
    .select('operatorOhubId', 'localChannel', 'operatorHasChannelBased')
else:
  dfOperatorsChannelsHasReco = dfOperatorsChannels \
    .withColumn('operatorHasChannelBased', f.lit(0).cast('integer')) \
    .select('operatorOhubId', 'localChannel', 'operatorHasChannelBased')

# Window to deduplicate on operatorOhubId
wOper = w.Window \
  .partitionBy('operatorOhubId') \
  .orderBy(f.col('operatorHasChannelBased').desc(), f.col('localChannel').desc())
          
# Combine all relevant operators from ohub and add localChannel
dfOhubOperatorSet = dfOperatorsOhubWithActiveSales \
  .union(dfOperatorsOhubWithMenu) \
  .union(dfOperatorsOhubWithContact) \
  .union(dfOperatorsOhubContacts) \
  .where(f.col('operatorOhubId').isNotNull()) \
  .distinct() \
  .join(dfOperatorsChannelsHasReco, on='operatorOhubId', how='left') \
  .withColumn('channelRank', f.row_number().over(wOper)) \
  .where(f.col('channelRank') == 1) \
  .drop('channelRank')
   
# Active online users
dfHitsPageMaxCorrectedFiltered = dfHitsPageMaxCorrected \
  .where(f.to_date(f.col('visitStartTimeConverted')) >= f.col('maxDateMin6')) \
  .where(f.to_date(f.col('visitStartTimeConverted')) <= f.col('maxDate'))
# Filter the users based on the ones with a trackingId and with a hit in ga within the timeframe 
dfUsersActive = dfUsers \
  .select('trackingId') \
  .distinct() \
  .join(dfHitsPageMaxCorrectedFiltered.select('trackingid').distinct(), on='trackingId', how='left_semi') \
  .select('trackingId', f.lit(1).alias('onlineActive')) \
  .distinct()
  
wUser = w.Window \
  .partitionBy('operatorOhubId', 'trackingId') \
  .orderBy(f.col('userId').desc())
  
# Select 1 userId per trackingId
dfTrackingUsers = dfContactsIds \
  .select('operatorOhubId', 'trackingid', 'userId') \
  .withColumn('userRank', f.row_number().over(wUser)) \
  .where(f.col('userRank') == 1) \
  .drop('userRank') \
  .join(dfUsersActive, on='trackingId', how='left') \
  .distinct()

# full join to add users based on trackingids (with and without operatorOhubId)
dfOperatorSet = dfOhubOperatorSet \
  .join(dfTrackingUsers, on='operatorOhubId', how='full') \
  .where(f.col('operatorOhubId').isNotNull() | (f.col('trackingId').isNotNull() & (f.col('onlineActive') == 1))) \
  .drop('onlineActive') \
  .withColumn('operatorHasChannelBased', f.when(f.col('operatorHasChannelBased').isNotNull(), f.col('operatorHasChannelBased')).otherwise(0)) \
  .distinct() 

# operatorRecoIdOhubLevel:     recommendations are on operator level
# operatorRecoIdTrackingLevel: recommendations are on operator x trackingid level (it is possible one operator has multiple trackingids linked)

# operatorRecoIdOhubLevel     =   operatorOhubId --> trackingId
# operatorRecoIdTrackingLevel = ((operatorOhubId --> '') + / + (trackingId --> '')) --> trackingId 

if countryCodeU in ['PL']:
  dfOperatorSet = dfOperatorSet.withColumn("localChannel", f.regexp_replace(f.col("localChannel"), "\*", "\\\*")).distinct()

dfOperatorSetId = dfOperatorSet \
  .withColumn('trackingidNotNull', f.when(f.col('trackingid').isNotNull(), f.col('trackingid')).otherwise('')) \
  .withColumn('operatorOhubIdNotNull', f.when(f.col('operatorOhubId').isNotNull(), f.col('operatorOhubId')).otherwise('')) \
  .withColumn('operatorRecoIdOhubLevel', f.when(f.col('operatorOhubId').isNotNull(), f.col('operatorOhubId')).otherwise(f.col('trackingid'))) \
  .withColumn('operatorRecoIdTrackingLevel', f.when(f.col('operatorOhubId').isNotNull(), f.concat(f.concat(f.col('operatorOhubIdNotNull'), f.lit(' / ')), f.col('trackingidNotNull'))).otherwise(f.col('trackingid'))) \
  .join(dfValidationGroups, ['trackingId'], 'left') \
  .cache()

# COMMAND ----------

productsWithRecipes.count()

# COMMAND ----------

dfOperatorSetId.count()

# COMMAND ----------

dfOperatorSetId.where(f.col('trackingId').isNull()).count()

# COMMAND ----------

dfOperatorSetId.where(f.col('operatorOhubId').isNull()).count()

# COMMAND ----------

# DBTITLE 1,Create recommendation set for operators and trackable entities
recommendationSetFinal = dfOperatorSetId.repartition(1000, 'operatorRecoIdTrackingLevel') \
  .crossJoin(f.broadcast(productsWithRecipes)) \
  .withColumn('channelBased', f
              .when((f.col('localChannel').isNull() | f.col('productBusinessChannelRelevant').isNull()), 0)
              .when(f.lower(f.col('productBusinessChannelRelevant')).contains(f.lower(f.col('localChannel'))), 1)
              .otherwise(0)) \
  .select('operatorRecoIdOhubLevel', 'operatorRecoIdTrackingLevel', 'operatorOhubId', 'trackingid', 'userid', 'localChannel', 'validationGroup', 'productCode', 'cuEanCodes', 'productCodeLocal', 'recipeId', 'language', 'productName', 'productUrl', 'productImageUrl', 'recipeName', 'recipeUrl', 'recipeImageUrl', 'manualMapping', 'channelBased', 'operatorHasChannelBased', 'productBusinessChannelRelevant', 'recipeSeasonRelevant', 'productCycleActive', 'recipeCycleActive') \
  .cache()

# COMMAND ----------

# DBTITLE 1,Load id x product scores
if scoresGaAvailable:
  scoresLogitGa = scoresLogitGa \
    .withColumn('label', f.regexp_replace(f.col('label'), 'dep_dum_ppv_', '')) \
    .withColumn('label', f.regexp_replace(f.col('label'),'_', '-')) \
    .withColumnRenamed('label', 'productCode') \
    .withColumnRenamed('score', 'affinity') \
    .select('trackingid', 'productCode', 'affinity')
else:
  schema = t.StructType([
    t.StructField("trackingid", t.StringType(), True),
    t.StructField("productCode", t.StringType(), True),
    t.StructField("affinity", t.FloatType(), True)
  ])
  scoresLogitGa = spark.createDataFrame(sc.emptyRDD(), schema)
  
if (countryCodeU not in countriesNoSalesScores) and scoresSalesAvailable:
  scoresLogitSales = scoresLogitSales \
    .withColumn('label', f.regexp_replace(f.col('label'), 'dep_dum_prs_', '')) \
    .withColumnRenamed('label', 'cuEanCodes') \
    .withColumnRenamed('score', 'propensity') \
    .select('operatorOhubId', 'cuEanCodes', 'propensity')
else:
  schema = t.StructType([
    t.StructField("operatorOhubId", t.StringType(), True),
    t.StructField("cuEanCodes", t.StringType(), True),
    t.StructField("propensity", t.FloatType(), True)
  ])
  scoresLogitSales = spark.createDataFrame(sc.emptyRDD(), schema)

scoresLogitGa = scoresLogitGa \
  .withColumn('affinityAverage', f.avg('affinity').over(w.Window.partitionBy('trackingId'))) \
  .withColumn('affinityAverageNtile', f.ntile(10).over(w.Window.partitionBy().orderBy(f.col('affinityAverage').asc()))) \
  .withColumn('affinityOverwritten', f.when(f.col('affinityAverageNtile') > 5, f.col('affinity')).otherwise(0))
scoresLogitSales = scoresLogitSales \
  .withColumn('propensityAverage', f.avg('propensity').over(w.Window.partitionBy('operatorOhubId'))) \
  .withColumn('propensityAverageNtile', f.ntile(10).over(w.Window.partitionBy().orderBy(f.col('propensityAverage').asc()))) \
  .withColumn('propensityOverwritten', f.when(f.col('propensityAverageNtile') > 5, f.col('propensity')).otherwise(0))

# All countries use recipenet code in url, except for the DACH countries. DACH uses PANEER instead of Recipenet and therefor includes the cueancode within the url
if countryCodeU in ['AT', 'CH', 'DE']:
  scoresLogitGa = scoresLogitGa.withColumnRenamed('productCode', 'cuEanCodes')
  productCodeToJoinGaScores = 'cuEanCodes'
else:
  productCodeToJoinGaScores = 'productCode'

# COMMAND ----------

# DBTITLE 1,Determine Whitespots
# Determine Digital Whitespots
# Create a set of trackingid with the products they already viewed
productsViewedGa = dfGaFactHits \
  .where(f.to_date(f.col('visitStartTimeConverted')) > f.col('maxDateMin6')) \
  .withColumn('digitalWhitespot', f.lit(0).cast('int')) \
  .select('trackingid', 'productCode', 'digitalWhitespot') \
  .distinct()

# Determine Sales Whitespots
# Create a set of operatorOhubIds with the products they already bought
productsBoughtSales = dfSales \
  .where(f.date_format(f.col('transactionDate'),'yyyyMM') > f.col('maxYearMonthMin6')) \
  .withColumn('salesWhitespot', f.lit(0).cast('int')) \
  .withColumnRenamed('cuEanCode', 'cuEanCodes') \
  .select('operatorOhubId', 'cuEanCodes', 'salesWhitespot') \
  .distinct()

# COMMAND ----------

# DBTITLE 1,Create the recommendation set and add whitespot indicator (DEPRICATED)
# THIS JOIN IS NOT SPLIT UP. THERE ARE MANY NULL VALUES FOR BOTH operatorOhubId AND trackingId. HENCE, THESE JOINS WILL BE SLOW AND STUCK AT THE LAST TASK OF THE JOB

# dfRecommendationSetJoined = recommendationSetFinal \
#   .join(scoresLogitGa, ['trackingid', productCodeToJoinGaScores], 'left') \
#   .join(scoresLogitSales, ['operatorOhubId', 'cuEanCodes'], 'left') \
#   .join(f.broadcast(productsViewedGa), ['trackingid', 'productCode'], 'left') \
#   .join(f.broadcast(productsBoughtSales), ['operatorOhubId', 'cuEanCodes'], 'left') \
#   .join(f.broadcast(onMenuTable), ['operatorOhubId', 'productCode'], 'left') \
#   .join(f.broadcast(dfProductViews), ['productCode'], 'left') \
#   .join(f.broadcast(dfRecipeViews), ['recipeId'], 'left') \
#   .join(f.broadcast(dfProductSales), ['cuEanCodes'], 'left') \
#   .withColumn('productViews', f.when(f.col('productViews').isNotNull(), f.col('productViews')).otherwise(0)) \
#   .withColumn('recipeViews', f.when(f.col('recipeViews').isNotNull(), f.col('recipeViews')).otherwise(0)) \
#   .withColumn('productSales', f.when(f.col('productSales').isNotNull(), f.col('productSales')).otherwise(0)) \
#   .withColumn('productViewsNtile', f.when(f.col('productViewsNtile').isNotNull(), f.col('productViewsNtile')).otherwise(0)) \
#   .withColumn('recipeViewsNtile', f.when(f.col('recipeViewsNtile').isNotNull(), f.col('recipeViewsNtile')).otherwise(0)) \
#   .withColumn('productSalesNtile', f.when(f.col('productSalesNtile').isNotNull(), f.col('productSalesNtile')).otherwise(0)) \
#   .withColumn('digitalWhitespot', f.when(f.col('digitalWhitespot').isNotNull(), f.col('digitalWhitespot')).otherwise(1).cast('int')) \
#   .withColumn('salesWhitespot', f.when(f.col('salesWhitespot').isNotNull(), f.col('salesWhitespot')).otherwise(1).cast('int')) \
#   .withColumn('whitespot', f.least(f.col('salesWhitespot'), f.col('digitalWhitespot'))) \
#   .withColumn('affinity', f.when(f.col('affinity').isNotNull(), f.col('affinity')).otherwise(0)) \
#   .withColumn('affinityOverwritten', f.when(f.col('affinityOverwritten').isNotNull(), f.col('affinityOverwritten')).otherwise(0)) \
#   .withColumn('propensity', f.when(f.col('propensity').isNotNull(), f.col('propensity')).otherwise(0)) \
#   .withColumn('propensityOverwritten', f.when(f.col('propensityOverwritten').isNotNull(), f.col('propensityOverwritten')).otherwise(0)) \
#   .withColumn('onMenuCard', f.when(f.col('onMenuCard').isNotNull(), f.col('onMenuCard')).otherwise(0)) \
#   .withColumn('personalized', f.when((f.col('propensity') != 0) | (f.col('affinity') != 0) | (f.col('onMenuCard') != 0), 1).otherwise(0))  \
#   .withColumn('countryCode', f.lit(countryCodeU)) \
#   .select('operatorRecoIdTrackingLevel',
#           'operatorRecoIdOhubLevel', 
#           'operatorOhubId', 
#           'localChannel',
#           'userId',
#           'trackingId',
#           'language', 
#           'productCode', 
#           'cuEanCodes', 
#           'productCodeLocal', 
#           'productName', 
#           'productUrl', 
#           'recipeId', 
#           'recipeName', 
#           'recipeUrl', 
#           'personalized', 
#           'affinity', 
#           'affinityAverage', 
#           'affinityAverageNtile',
#           'affinityOverwritten',
#           'propensity', 
#           'propensityAverage', 
#           'propensityAverageNtile',
#           'propensityOverwritten',
#           'onMenuCard', 
#           'salesWhitespot', 
#           'digitalWhitespot',
#           'whitespot',
#           'productViews',
#           'productViewsNtile',
#           'recipeViews',
#           'recipeViewsNtile', 
#           'productSales', 
#           'productSalesNtile',
#           'validationGroup',
#           'manualMapping', 
#           'channelBased', 
#           'operatorHasChannelBased',
#           'productBusinessChannelRelevant', 
#           'recipeSeasonRelevant',
#           'countryCode')

# COMMAND ----------

# DBTITLE 1,Create the recommendation set (part 1)
recommendationSetFinalNotNull1 = recommendationSetFinal \
  .where((f.col('trackingId').isNotNull() & f.col(productCodeToJoinGaScores).isNotNull())) \
  .join(scoresLogitGa, ['trackingid', productCodeToJoinGaScores], 'left') \
  .select("operatorRecoIdOhubLevel", "operatorRecoIdTrackingLevel", "operatorOhubId", "trackingid", "userid", "localChannel", "validationGroup", "productCode", "cuEanCodes", "productCodeLocal", "recipeId", "language", "productName", "productUrl", 'productImageUrl', "recipeName", "recipeUrl", 'recipeImageUrl', "manualMapping", "channelBased", "operatorHasChannelBased", "productBusinessChannelRelevant", "recipeSeasonRelevant", "productCycleActive", "recipeCycleActive", "affinity", "affinityAverage", "affinityAverageNtile", "affinityOverwritten")

recommendationSetFinalNull1 = recommendationSetFinal \
  .where(~(f.col('trackingId').isNotNull() & f.col(productCodeToJoinGaScores).isNotNull())) \
  .withColumn('affinity', f.lit(None).cast('float')) \
  .withColumn('affinityAverage', f.lit(None).cast('double')) \
  .withColumn('affinityAverageNtile', f.lit(None).cast('integer')) \
  .withColumn('affinityOverwritten', f.lit(None).cast('float')) \
  .select("operatorRecoIdOhubLevel", "operatorRecoIdTrackingLevel", "operatorOhubId", "trackingid", "userid", "localChannel", "validationGroup", "productCode", "cuEanCodes", "productCodeLocal", "recipeId", "language", "productName", "productUrl", 'productImageUrl', "recipeName", "recipeUrl", 'recipeImageUrl', "manualMapping", "channelBased", "operatorHasChannelBased", "productBusinessChannelRelevant", "recipeSeasonRelevant", "productCycleActive", "recipeCycleActive", "affinity", "affinityAverage", "affinityAverageNtile", "affinityOverwritten")

dfRecommendationSetJoinedScore1 = recommendationSetFinalNotNull1 \
  .union(recommendationSetFinalNull1) \
  .cache()
dfRecommendationSetJoinedScore1.show()

# COMMAND ----------

# DBTITLE 1,Create the recommendation set (part 2)
recommendationSetFinalNotNull2 = dfRecommendationSetJoinedScore1 \
  .where((f.col('operatorOhubId').isNotNull() & f.col('cuEanCodes').isNotNull())) \
  .join(scoresLogitSales, ['operatorOhubId', 'cuEanCodes'], 'left') \
  .select("operatorRecoIdOhubLevel", "operatorRecoIdTrackingLevel", "operatorOhubId", "trackingid", "userid", "localChannel", "validationGroup", "productCode", "cuEanCodes", "productCodeLocal", "recipeId", "language", "productName", "productUrl", 'productImageUrl', "recipeName", "recipeUrl", 'recipeImageUrl', "manualMapping", "channelBased", "operatorHasChannelBased", "productBusinessChannelRelevant", "recipeSeasonRelevant", "productCycleActive", "recipeCycleActive", "affinity", "affinityAverage", "affinityAverageNtile", "affinityOverwritten", "propensity", "propensityAverage", "propensityAverageNtile", "propensityOverwritten")

recommendationSetFinalNull2 = dfRecommendationSetJoinedScore1 \
  .where(~(f.col('operatorOhubId').isNotNull() & f.col('cuEanCodes').isNotNull())) \
  .withColumn('propensity', f.lit(None).cast('float')) \
  .withColumn('propensityAverage', f.lit(None).cast('double')) \
  .withColumn('propensityAverageNtile', f.lit(None).cast('integer')) \
  .withColumn('propensityOverwritten', f.lit(None).cast('float')) \
  .select("operatorRecoIdOhubLevel", "operatorRecoIdTrackingLevel", "operatorOhubId", "trackingid", "userid", "localChannel", "validationGroup", "productCode", "cuEanCodes", "productCodeLocal", "recipeId", "language", "productName", "productUrl", 'productImageUrl', "recipeName", "recipeUrl", 'recipeImageUrl', "manualMapping", "channelBased", "operatorHasChannelBased", "productBusinessChannelRelevant", "recipeSeasonRelevant", "productCycleActive", "recipeCycleActive", "affinity", "affinityAverage", "affinityAverageNtile", "affinityOverwritten", "propensity", "propensityAverage", "propensityAverageNtile", "propensityOverwritten")

dfRecommendationSetJoinedScore2 = recommendationSetFinalNotNull2 \
  .union(recommendationSetFinalNull2) \
  .cache()
dfRecommendationSetJoinedScore2.show()

# COMMAND ----------

# DBTITLE 1,Create the recommendation set (part 3)
dfRecommendationSetJoined = dfRecommendationSetJoinedScore2 \
  .join(f.broadcast(productsViewedGa), ['trackingid', 'productCode'], 'left') \
  .join(f.broadcast(productsBoughtSales), ['operatorOhubId', 'cuEanCodes'], 'left') \
  .join(f.broadcast(onMenuTable), ['operatorOhubId', 'productCode'], 'left') \
  .join(f.broadcast(dfProductViews), ['productCode'], 'left') \
  .join(f.broadcast(dfRecipeViews), ['recipeId'], 'left') \
  .join(f.broadcast(dfProductSales), ['cuEanCodes'], 'left') \
  .withColumn('productViews', f.when(f.col('productViews').isNotNull(), f.col('productViews')).otherwise(0)) \
  .withColumn('recipeViews', f.when(f.col('recipeViews').isNotNull(), f.col('recipeViews')).otherwise(0)) \
  .withColumn('productSales', f.when(f.col('productSales').isNotNull(), f.col('productSales')).otherwise(0)) \
  .withColumn('productViewsNtile', f.when(f.col('productViewsNtile').isNotNull(), f.col('productViewsNtile')).otherwise(0)) \
  .withColumn('recipeViewsNtile', f.when(f.col('recipeViewsNtile').isNotNull(), f.col('recipeViewsNtile')).otherwise(0)) \
  .withColumn('productSalesNtile', f.when(f.col('productSalesNtile').isNotNull(), f.col('productSalesNtile')).otherwise(0)) \
  .withColumn('digitalWhitespot', f.when(f.col('digitalWhitespot').isNotNull(), f.col('digitalWhitespot')).otherwise(1).cast('int')) \
  .withColumn('salesWhitespot', f.when(f.col('salesWhitespot').isNotNull(), f.col('salesWhitespot')).otherwise(1).cast('int')) \
  .withColumn('whitespot', f.least(f.col('salesWhitespot'), f.col('digitalWhitespot'))) \
  .withColumn('affinity', f.when(f.col('affinity').isNotNull(), f.col('affinity')).otherwise(0)) \
  .withColumn('affinityOverwritten', f.when(f.col('affinityOverwritten').isNotNull(), f.col('affinityOverwritten')).otherwise(0)) \
  .withColumn('propensity', f.when(f.col('propensity').isNotNull(), f.col('propensity')).otherwise(0)) \
  .withColumn('propensityOverwritten', f.when(f.col('propensityOverwritten').isNotNull(), f.col('propensityOverwritten')).otherwise(0)) \
  .withColumn('onMenuCard', f.when(f.col('onMenuCard').isNotNull(), f.col('onMenuCard')).otherwise(0)) \
  .withColumn('personalized', f.when((f.col('propensity') != 0) | (f.col('affinity') != 0) | (f.col('onMenuCard') != 0), 1).otherwise(0))  \
  .withColumn('personalizedOverwritten', f.when((f.col('propensityOverwritten') != 0) | (f.col('affinityOverwritten') != 0) | (f.col('onMenuCard') != 0), 1).otherwise(0))  \
  .withColumn('countryCode', f.lit(countryCodeU)) \
  .select('operatorRecoIdTrackingLevel',
          'operatorRecoIdOhubLevel', 
          'operatorOhubId', 
          'trackingId',
          'userId',
          'localChannel',
          'language', 
          'productCode', 
          'cuEanCodes', 
          'productCodeLocal', 
          'productName', 
          'productUrl', 
          'productImageUrl',
          'recipeId', 
          'recipeName', 
          'recipeUrl', 
          'recipeImageUrl',
          'personalized', 
          'personalizedOverwritten', 
          'affinity', 
          'affinityAverage', 
          'affinityAverageNtile',
          'affinityOverwritten',
          'propensity', 
          'propensityAverage', 
          'propensityAverageNtile',
          'propensityOverwritten',
          'onMenuCard', 
          'salesWhitespot', 
          'digitalWhitespot',
          'whitespot',
          'productViews',
          'productViewsNtile',
          'recipeViews',
          'recipeViewsNtile', 
          'productSales', 
          'productSalesNtile',
          'validationGroup',
          'manualMapping', 
          'channelBased', 
          'operatorHasChannelBased',
          'productBusinessChannelRelevant', 
          'recipeSeasonRelevant', 
          'productCycleActive', 
          'recipeCycleActive',
          'countryCode')

# COMMAND ----------

# display(dfRecommendationSetJoined)

# COMMAND ----------

# display(dfRecommendationSetJoined.select('localChannel').distinct())

# COMMAND ----------

# # Check if recommendations unique on operatorTrackingLevel x productCodeToJoinOn x recipeId x language
# display(dfRecommendationSetJoined.groupBy('operatorRecoIdTrackingLevel', productCodeToJoinOn, 'recipeId', 'language').count().where(f.col('count')>1))

# COMMAND ----------

# # Count unique trackingId and the number of trackingIds with at least one personalized reco
# display(dfRecommendationSetJoined \
#   .where(f.col('trackingId').isNotNull()) \
#   .select('trackingId', productCodeToJoinOn, 'personalized') \
#   .distinct() \
#   .groupBy('trackingId') \
#   .agg(f.sum('personalized').alias('nrProductsPersonalized')) \
#   .groupBy('nrProductsPersonalized') \
#   .count())

# COMMAND ----------

hiveTable = "dev_derived_reco.joined_recommendation_set"
deltaTable = "/mnt/datamodel/dev/derived/reco/joined/recommendation_set"

dfRecommendationSetJoined.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .option("mergeSchema", "true") \
  .option("replaceWhere", "countryCode = '" + countryCodeU + "'") \
  .partitionBy("countryCode") \
  .save(deltaTable)

sqlQuery1 = "drop table if exists " + hiveTable
sqlQuery2 = "create table " + hiveTable + " using delta location '" + deltaTable + "'"
sqlQuery3 = "ALTER TABLE " + hiveTable + " SET TBLPROPERTIES ('delta.logRetentionDuration'='interval 30 days', 'delta.deletedFileRetentionDuration'='interval 30 days')"
spark.sql(sqlQuery1)
spark.sql(sqlQuery2)
spark.sql(sqlQuery3)
