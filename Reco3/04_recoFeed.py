# Databricks notebook source
from pyspark.sql import functions as f
from pyspark.sql import window as w
from pyspark.sql import types as t
from delta import tables
from datetime import datetime, timedelta
from pytz import timezone

# COMMAND ----------

countryCodeU = getArgument("countryCode").upper()
mcoU = getArgument("mco").upper()
countriesBusinessRuling = getArgument("countriesBusinessRuling").split(',')

# countryCodeU = "ZA"
# mcoU = "ZA"
# countriesBusinessRuling = []

countryCodeL = countryCodeU.lower()

# COMMAND ----------

print(countryCodeU)

# COMMAND ----------

tableNameInputReco      = 'dev_derived_reco.joined_recommendation_set' 

hiveTableNameRecAcm     = 'dev_derived_reco.output_recommendations_acm'
deltaTableNameRecAcm    = '/mnt/datamodel/dev/derived/reco/output/recommendations_acm'
hiveTableNameRecUfs     = 'dev_derived_reco.output_recommendations_ufs_com'
deltaTableNameRecUfs    = '/mnt/datamodel/dev/derived/reco/output/recommendations_ufs_com'
hiveTableNameRecOview   = 'dev_derived_reco.output_recommendations_oview'
deltaTableNameRecOview  = '/mnt/datamodel/dev/derived/reco/output/recommendations_oview'
hiveTableNameRecMarketo = 'dev_derived_reco.output_recommendations_marketo'
deltaTableNameRecMarketo= '/mnt/datamodel/dev/derived/reco/output/recommendations_marketo'
hiveTableNameRecLDA     = 'dev_derived_reco.output_recommendations_lda'
deltaTableNameRecLDA    = '/mnt/datamodel/dev/derived/reco/output/recommendations_lda'

# COMMAND ----------

dfInputReco = spark.table(tableNameInputReco)

# COMMAND ----------

# To do: clean product and recipe column from acm export 

def get_old_recos(primaryKey, daysOld, countryCodeU, system, recoType):
  deltaTable = '/mnt/datamodel/dev/derived/reco/output/recommendations_' + system
  
  today = datetime.now(timezone('Europe/Amsterdam'))
  today = today.strftime("%Y%m%d")
  todayDatetimeObject = datetime.strptime(today, '%Y%m%d')
  print('Today\'s date is: ' + today)
  recoDateOld = datetime.strftime(todayDatetimeObject - timedelta(days=daysOld-1), '%Y%m%d')

  excludeOnColumns = primaryKey + [recoType]
  # Try to load the old recommendations
  try:
    # Obtain the version of the delta table that is older than 10 days
    fullHistoryDF = tables.DeltaTable.forPath(spark, deltaTable).history()
    version = fullHistoryDF \
      .where(f.date_format(f.to_date(f.col('timestamp')), "YYYYMMdd") < recoDateOld) \
      .agg(f.max('version')) \
      .collect()[0][0]
    print('Loading older version: ' + str(version))
    df = spark.read.format("delta") \
      .option("versionAsOf", version) \
      .load(deltaTable) \
      .where(f.col('countryCode') == countryCodeU)
    
    # For each output system, transform the output back to the form such that it can be removed from the possible product / recipe reco's
    if system.lower() == 'acm':
      oldRecommendations = df.select(f.col('opr_lnkd_integration_id').alias('operatorOhubId'), f.lower(f.col('language')).alias('language'), f.col('ws_sku1_prd_cod').alias(recoType)) \
        .union(df.select(f.col('opr_lnkd_integration_id').alias('operatorOhubId'), f.lower(f.col('language')).alias('language'), f.col('ws_sku2_prd_cod').alias(recoType))) \
        .union(df.select(f.col('opr_lnkd_integration_id').alias('operatorOhubId'), f.lower(f.col('language')).alias('language'), f.col('ws_sku3_prd_cod').alias(recoType))) \
        .withColumn(recoType, f.regexp_replace(recoType, '.*\_', ''))
    elif system.lower() == 'ufs_com':
      oldRecommendations = df.select('trackingId', f.col('languageCode').alias('language'), f.col('recommendation').alias(recoType))
    elif system.lower() == 'oview':
      oldRecommendations = df.select(f.col('opr_lnkd_integration_id').alias('operatorOhubId'), f.col('recommendation_id').alias(recoType))
    elif system.lower() == 'marketo':
      oldRecommendations = df.select('operatorOhubId', f.col('recommendationCode').alias(recoType))
    else:
      raise ValueError()
    for k in excludeOnColumns: 
      oldRecommendations = oldRecommendations.where(f.col(k).isNotNull())
    
    # The select statement here makes sure that it was possible to select the right columns
    oldRecommendations = oldRecommendations.select(excludeOnColumns).distinct()
    print('Old recommendations available for ' + str(system))
  except Exception as e:
    print(e)
    print('No old recommendations available for ' + str(system))
    structColumns = [t.StructField(k, t.StringType(), True) for k in excludeOnColumns]
    schema = t.StructType(structColumns)
    oldRecommendations = spark.createDataFrame(sc.emptyRDD(), schema)
  return oldRecommendations

# COMMAND ----------

def createRecoFeed(df, countryCode, primaryKey, numberOfRecoPerCustomer, numberOfRecoStrict, productCodeType, system, productRankOn, personalized, personalizedOverwritten, personalizedOrChannelBased, personalizedOverwrittenOrChannelBased, whitespot, productRecipeLink, recipeRankOn, excludeOldRecommendations, excludeOldRecommendationsDays, businessRuling):  
  countryCodeU = countryCode.upper()
  
  # Filter on countryCode
  df = df.where(f.col('countryCode') == countryCodeU)
  
  # Make sure primary keys are not null
  for k in primaryKey:
    df = df.where(f.col(k).isNotNull())
  
  # Filter on whitespot
  if whitespot == 'full':
    df = df.where(f.col('whitespot') == 1)
  elif whitespot == 'digital':
    df = df.where(f.col('digitalWhitespot') == 1)
  elif whitespot == 'sales':
    df = df.where(f.col('salesWhitespot') == 1)
  elif whitespot == None:
    pass
  else:
    raise ValueError("Incorrect whitespot parameter: choose from ['full', 'digital', 'sales', None]")

  if personalized:
    df = df.where(f.col('personalized') == 1)
  if personalizedOrChannelBased:
    df = df.where((f.col('personalized') == 1) | (f.col('channelBased') == 1))
  if personalizedOverwritten:
    df = df.where(f.col('personalizedOverwritten') == 1)
  if personalizedOverwrittenOrChannelBased:
    df = df.where((f.col('personalizedOverwritten') == 1) | (f.col('channelBased') == 1))
  
  # Select the reco lines per operator that are channel based if the operator has channelbased recommendations.
  # For the operators withouth hasChannelBased, select all products
  # Select only recipes relevant to the season
  if businessRuling:
    df = df \
      .where(((f.col('operatorHasChannelBased') == 1) & (f.col('channelBased') == 1)) | (f.col('operatorHasChannelBased') == 0)) \
      .where(f.col('recipeSeasonRelevant') == True)
    
  # Add random columns
  df = df \
    .withColumn('random1', f.rand()) \
    .withColumn('random2', f.rand())

  # This script is for if products need to be tight to recipes (as for in the email)
  if productRecipeLink:
    df = df.where(f.col('recipeId').isNotNull())
    # If business ruling applies and products should be linked to recipes, only take products/recipe combinations from the manual mapping
    if businessRuling:
      df = df.where(f.col('manualMapping') == 1)
    
    # Window to deduplicate recipes and drop duplicates per primary key x product
    primaryKeyProduct = primaryKey + [productCodeType]
    w0 = w.Window.partitionBy(primaryKeyProduct).orderBy(f.col('recipeViews').desc(), f.col('random1').desc())

    # One recipe per product, recipe with most online views is chosen
    df = df \
      .withColumn('recipeRank', f.row_number().over(w0)) \
      .where(f.col('recipeRank') == 1)
    
    if excludeOldRecommendations:
      try:
        dfOld = get_old_recos(primaryKey, excludeOldRecommendationsDays, countryCodeU, system, productCodeType)
        df = df.join(dfOld, on=primaryKeyProduct, how='left_anti')
      except:
        print('Error getting old recos')
        print("Error:", sys.exc_info()[0])
        
    # To Do: Build check for "rankOn" to see if contains propensity, affinity, or onMenuCard  
    # Window to rank products
    productRankOn += ['random2']
    productRankOnCol = []
    for r in productRankOn:
      productRankOnCol += [f.col(r).desc()]
    w1 = w.Window.partitionBy(primaryKey).orderBy(productRankOnCol)

    # Rank product x recipe combinations
    df = df \
      .withColumn('recoRank', f.row_number().over(w1)) \
      .where(f.col('recoRank') <= numberOfRecoPerCustomer)

    if numberOfRecoStrict:
      dfExact = df \
        .groupBy(primaryKey) \
        .agg(f.countDistinct('recoRank').alias('cntRecos')) \
        .where(f.col('cntRecos') == numberOfRecoPerCustomer) \
        .select(primaryKey)
      df = df.join(dfExact, on=primaryKey, how='left_semi')
    # The resulting df has as primary key: primaryKey x productCodeType
  else:
    df = df \
      .withColumn('random3', f.rand()) \
      .withColumn('random4', f.rand())

    # Windows to deduplicate product and recipes, drop duplicates per primary key x product or primary key x recipe
    primaryKeyProduct = primaryKey + [productCodeType]
    primaryKeyRecipe = primaryKey + ['recipeId']
    productDedupOn = productRankOn + ['random1']
    recipeDedupOn = recipeRankOn + ['random2']
    productDedupOnCol = []
    recipeDedupOnCol = []
    for rp in productDedupOn:
      productDedupOnCol += [f.col(rp).desc()]
    for rr in recipeDedupOn:
      recipeDedupOnCol += [f.col(rr).desc()]
    w0P = w.Window.partitionBy(primaryKeyProduct).orderBy(productDedupOnCol)
    w0R = w.Window.partitionBy(primaryKeyRecipe).orderBy(recipeDedupOnCol)
    
    dfP = df.dropDuplicates(primaryKeyProduct) 
    dfR = df.dropDuplicates(primaryKeyRecipe)

    if excludeOldRecommendations:
      try:
        dfOldP = get_old_recos(primaryKey, excludeOldRecommendationsDays, countryCodeU, system, productCodeType)
        dfOldR = get_old_recos(primaryKey, excludeOldRecommendationsDays, countryCodeU, system, 'recipeId')
        dfP = dfP.join(dfOldP, on=primaryKeyProduct, how='left_anti')
        dfR = dfR.join(dfOldR, on=primaryKeyRecipe, how='left_anti')
      except:
        print('Error getting old recos')
        print("Error:", sys.exc_info()[0])
        
    # To Do: Build check for "rankOn" to see if contains propensity, affinity, or onMenuCard  
    # Window to rank products
    productRankOn += ['random3']
    recipeRankOn += ['random4']
    productRankOnCol = []
    recipeRankOnCol = []
    for rp in productRankOn:
      productRankOnCol += [f.col(rp).desc()]
    for rr in recipeRankOn:
      recipeRankOnCol += [f.col(rr).desc()]
    w1P = w.Window.partitionBy(primaryKey).orderBy(productRankOnCol)
    w1R = w.Window.partitionBy(primaryKey).orderBy(recipeRankOnCol)

    # Rank product combinations
    dfP = dfP \
      .withColumn('recoRank', f.row_number().over(w1P)) \
      .where(f.col('recoRank') <= numberOfRecoPerCustomer) \
      .withColumn('recoType', f.lit('product'))
    # Rank product combinations
    dfR = dfR \
      .withColumn('recoRank', f.row_number().over(w1R)) \
      .where(f.col('recoRank') <= numberOfRecoPerCustomer) \
      .withColumn('recoType', f.lit('recipe'))
    
    if numberOfRecoStrict:
      dfPExact = dfP \
        .groupBy(primaryKey) \
        .agg(f.countDistinct('recoRank').alias('cntRecos')) \
        .where(f.col('cntRecos') == numberOfRecoPerCustomer) \
        .select(primaryKey) 
      dfP = dfP.join(dfPExact, on=primaryKey, how='left_semi')
      dfRExact = dfR \
        .groupBy(primaryKey) \
        .agg(f.countDistinct('recoRank').alias('cntRecos')) \
        .where(f.col('cntRecos') == numberOfRecoPerCustomer) \
        .select(primaryKey)
      dfR = dfR.join(dfPExact, on=primaryKey, how='left_semi')
    
    colOrder = dfP.columns
    df = dfP.select(colOrder).union(dfR.select(colOrder))    
  return df

# COMMAND ----------

def createRecoFeedValidation(df, countryCode, primaryKey, numberOfRecoPerCustomer, numberOfRecoStrict, productCodeType, system, validationGroupList, productRankOnList, personalizedList, personalizedOverwrittenList, personalizedOrChannelBasedList, personalizedOverwrittenOrChannelBasedList, whitespotList, productRecipeLinkList, recipeRankOnList, excludeOldRecommendationsList, excludeOldRecommendationsDaysList, businessRulingList): 
  # Add some len values based on arguments
  if not len(validationGroupList) == len(productRankOnList) == len(personalizedList) == len(personalizedOrChannelBasedList) == len(whitespotList) == len(productRecipeLinkList) == len(recipeRankOnList) == len(excludeOldRecommendationsList) == len(excludeOldRecommendationsDaysList) == len(businessRulingList):
    raise ValueError("Make sure input lists are of same length.")
  i = 0 
  dfGroup = df.where(f.col('validationGroup') == validationGroupList[i])
  dfReco = createRecoFeed(dfGroup, countryCodeU, primaryKey, numberOfRecoPerCustomer, numberOfRecoStrict, productCodeType, system, productRankOn=productRankOnList[i], personalized=personalizedList[i], personalizedOverwritten=personalizedOverwrittenList[i], personalizedOrChannelBased=personalizedOrChannelBasedList[i], personalizedOverwrittenOrChannelBased=personalizedOverwrittenOrChannelBasedList[i], whitespot=whitespotList[i], productRecipeLink=productRecipeLinkList[i], recipeRankOn=recipeRankOnList[i], excludeOldRecommendations=excludeOldRecommendationsList[i], excludeOldRecommendationsDays=excludeOldRecommendationsDaysList[i], businessRuling=businessRulingList[i])
  colOrder = dfReco.columns
  for i in range(1, len(validationGroupList)):
    dfGroupNext = df.where(f.col('validationGroup') == validationGroupList[i])
    dfRecoNext = createRecoFeed(dfGroupNext, countryCodeU, primaryKey, numberOfRecoPerCustomer, numberOfRecoStrict, productCodeType, system, productRankOn=productRankOnList[i], personalized=personalizedList[i], personalizedOverwritten=personalizedOverwrittenList[i], personalizedOrChannelBased=personalizedOrChannelBasedList[i], personalizedOverwrittenOrChannelBased=personalizedOverwrittenOrChannelBasedList[i], whitespot=whitespotList[i], productRecipeLink=productRecipeLinkList[i], recipeRankOn=recipeRankOnList[i], excludeOldRecommendations=excludeOldRecommendationsList[i], excludeOldRecommendationsDays=excludeOldRecommendationsDaysList[i], businessRuling=businessRulingList[i])
    dfReco = dfReco.select(colOrder).union(dfRecoNext.select(colOrder))
  return dfReco

# COMMAND ----------

if countryCodeU not in ['AT', 'CH', 'DE']:
  productCodeType = 'productCode'
else:
  productCodeType = 'cuEanCodes'

if countryCodeU in countriesBusinessRuling:
  businessRulingPerCountry = True
else:
  businessRulingPerCountry = False

# COMMAND ----------

# DBTITLE 1,Create reco feeds, the tuning on what type of recommendations are generated in each feed are created here
# ACM
dfRecoAcm = createRecoFeed(dfInputReco, countryCodeU, ['operatorOhubId', 'language'], 3, True, 'cuEanCodes', 'acm', productRankOn=['propensityOverwritten', 'affinityOverwritten', 'onMenuCard', 'productSalesNtile'], personalized=False, personalizedOverwritten=False, personalizedOrChannelBased=False, personalizedOverwrittenOrChannelBased=False, whitespot='sales', productRecipeLink=True, recipeRankOn=None, excludeOldRecommendations=True, excludeOldRecommendationsDays=14, businessRuling=businessRulingPerCountry)

# Ufs.com Validation
dfRecoUfs = createRecoFeedValidation(dfInputReco, countryCodeU, ['trackingId', 'language'], 3, True, productCodeType, 'ufs_com',
                                     validationGroupList=['t','c1', 'c2'],
                                     productRankOnList=[['affinityOverwritten', 'propensityOverwritten', 'onMenuCard', 'productViewsNtile'], ['productViewsNtile'], []],
                                     personalizedList=[False, False, False],
                                     personalizedOverwrittenList=[False, False, False],
                                     personalizedOrChannelBasedList=[False, False, False],
                                     personalizedOverwrittenOrChannelBasedList=[False, False, False],
                                     whitespotList=['digital', None, None],
                                     productRecipeLinkList=[False, False, False],
                                     recipeRankOnList=[['affinityOverwritten', 'propensityOverwritten', 'onMenuCard', 'recipeViewsNtile'], ['recipeViewsNtile'], []], 
                                     excludeOldRecommendationsList=[True, False, False], 
                                     excludeOldRecommendationsDaysList=[7, None, None], 
                                     businessRulingList=[False, False, False])

# Dispatcher (oview)
dfRecoOview = createRecoFeed(dfInputReco, countryCodeU, ['operatorOhubId'], 3, True, 'cuEanCodes', 'oview', productRankOn=['propensityOverwritten', 'affinityOverwritten', 'onMenuCard', 'productSalesNtile'], personalized=False, personalizedOverwritten=False, personalizedOrChannelBased=False, personalizedOverwrittenOrChannelBased=True, whitespot='sales', productRecipeLink=False, recipeRankOn=['affinityOverwritten', 'propensityOverwritten', 'recipeViewsNtile'], excludeOldRecommendations=True, excludeOldRecommendationsDays=14, businessRuling=businessRulingPerCountry)

# Marketo
dfRecoMarketo = createRecoFeed(dfInputReco, countryCodeU, ['operatorOhubId'], 3, True, 'cuEanCodes', 'marketo', productRankOn=['propensityOverwritten', 'affinityOverwritten', 'onMenuCard', 'productSalesNtile'], personalized=False, personalizedOverwritten=False, personalizedOrChannelBased=False, personalizedOverwrittenOrChannelBased=False, whitespot='sales', productRecipeLink=False, recipeRankOn=['affinityOverwritten', 'propensityOverwritten', 'recipeViewsNtile'], excludeOldRecommendations=True, excludeOldRecommendationsDays=14, businessRuling=businessRulingPerCountry)

# COMMAND ----------

# DBTITLE 1,ACM reco output transform
dfRecoAcmTransformed = dfRecoAcm \
  .withColumn('language', f.upper(f.col('language'))) \
  .groupBy('countryCode', 'operatorOhubId', 'language') \
  .agg(
    f.lit('NLA').cast('string').alias('ws_sku1_prd_cat'),
    f.max(f.when(f.col('recoRank') == 1, f.concat(f.concat(f.concat(f.lit(mcoU + '_' + countryCodeU + '_'), f.col('language')), f.lit('_')), f.col('cuEanCodes')))).alias('ws_sku1_prd_cod'),
    f.max(f.when(f.col('recoRank') == 1, f.col('productName'))).alias('ws_sku1_prd_nme'),
    f.max(f.when(f.col('recoRank') == 1, f.col('propensity'))).alias('ws_sku1_prd_scr'),
    f.max(f.when(f.col('recoRank') == 1, f.col('productUrl'))).alias('ws_sku1_prd_ldp'),
    f.lit('NLA').cast('string').alias('ws_sku2_prd_cat'),
    f.max(f.when(f.col('recoRank') == 2, f.concat(f.concat(f.concat(f.lit(mcoU + '_' + countryCodeU + '_'), f.col('language')), f.lit('_')), f.col('cuEanCodes')))).alias('ws_sku2_prd_cod'),
    f.max(f.when(f.col('recoRank') == 2, f.col('productName'))).alias('ws_sku2_prd_nme'),
    f.max(f.when(f.col('recoRank') == 2, f.col('propensity'))).alias('ws_sku2_prd_scr'),
    f.max(f.when(f.col('recoRank') == 2, f.col('productUrl'))).alias('ws_sku2_prd_ldp'),
    f.lit('NLA').cast('string').alias('ws_sku3_prd_cat'),
    f.max(f.when(f.col('recoRank') == 3, f.concat(f.concat(f.concat(f.lit(mcoU + '_' + countryCodeU + '_'), f.col('language')), f.lit('_')), f.col('cuEanCodes')))).alias('ws_sku3_prd_cod'),
    f.max(f.when(f.col('recoRank') == 3, f.col('productName'))).alias('ws_sku3_prd_nme'),
    f.max(f.when(f.col('recoRank') == 3, f.col('propensity'))).alias('ws_sku3_prd_scr'),
    f.max(f.when(f.col('recoRank') == 3, f.col('productUrl'))).alias('ws_sku3_prd_ldp'),
    f.lit('').cast('string').alias('dps_sku1_prd_cat'),
    f.lit('').cast('string').alias('dps_sku1_prd_cod'),
    f.lit('').cast('string').alias('dps_sku1_prd_nme'),
    f.lit('').cast('string').alias('dps_sku1_prd_scr'),
    f.lit('').cast('string').alias('dps_sku1_prd_ldp'),
    f.lit('').cast('string').alias('dps_sku2_prd_cat'),
    f.lit('').cast('string').alias('dps_sku2_prd_cod'),
    f.lit('').cast('string').alias('dps_sku2_prd_nme'),
    f.lit('').cast('string').alias('dps_sku2_prd_scr'),
    f.lit('').cast('string').alias('dps_sku2_prd_ldp'),
    f.lit('').cast('string').alias('dps_sku3_prd_cat'),
    f.lit('').cast('string').alias('dps_sku3_prd_cod'),
    f.lit('').cast('string').alias('dps_sku3_prd_nme'),
    f.lit('').cast('string').alias('dps_sku3_prd_scr'),
    f.lit('').cast('string').alias('dps_sku3_prd_ldp'),
    f.lit('').cast('string').alias('ws_smp_sku1_prd_cat'),
    f.lit('').cast('string').alias('ws_smp_sku1_prd_cod'),
    f.lit('').cast('string').alias('ws_smp_sku1_prd_nme'),
    f.lit('').cast('string').alias('ws_smp_sku1_prd_scr'),
    f.lit('').cast('string').alias('ws_smp_sku1_prd_ldp'),
    f.lit('').cast('string').alias('ws_smp_sku1_prd_smp_fwu'),
    f.lit('').cast('string').alias('ws_smp_sku2_prd_cat'),
    f.lit('').cast('string').alias('ws_smp_sku2_prd_cod'),
    f.lit('').cast('string').alias('ws_smp_sku2_prd_nme'),
    f.lit('').cast('string').alias('ws_smp_sku2_prd_scr'),
    f.lit('').cast('string').alias('ws_smp_sku2_prd_ldp'),
    f.lit('').cast('string').alias('ws_smp_sku2_prd_smp_fwu'),
    f.lit('').cast('string').alias('ws_smp_sku3_prd_cat'),
    f.lit('').cast('string').alias('ws_smp_sku3_prd_cod'),
    f.lit('').cast('string').alias('ws_smp_sku3_prd_nme'),
    f.lit('').cast('string').alias('ws_smp_sku3_prd_scr'),
    f.lit('').cast('string').alias('ws_smp_sku3_prd_ldp'),
    f.lit('').cast('string').alias('ws_smp_sku3_prd_smp_fwu'),
    f.lit('NLA').cast('string').alias('rcr_sku1_prd_cat'),
    f.max(f.when(f.col('recoRank') == 1, f.concat(f.concat(f.concat(f.lit(mcoU + '_' + countryCodeU + '_'), f.col('language')), f.lit('_')), f.col('recipeId')))).alias('rcr_sku1_prd_cod'),
    f.max(f.when(f.col('recoRank') == 1, f.col('recipeName'))).alias('rcr_sku1_prd_nme'),
    f.max(f.when(f.col('recoRank') == 1, f.col('propensity'))).alias('rcr_sku1_prd_scr'),
    f.max(f.when(f.col('recoRank') == 1, f.col('recipeUrl'))).alias('rcr_sku1_prd_ldp'),
    f.lit('NLA').cast('string').alias('rcr_sku2_prd_cat'),
    f.max(f.when(f.col('recoRank') == 2, f.concat(f.concat(f.concat(f.lit(mcoU + '_' + countryCodeU + '_'), f.col('language')), f.lit('_')), f.col('recipeId')))).alias('rcr_sku2_prd_cod'),
    f.max(f.when(f.col('recoRank') == 2, f.col('recipeName'))).alias('rcr_sku2_prd_nme'),
    f.max(f.when(f.col('recoRank') == 2, f.col('propensity'))).alias('rcr_sku2_prd_scr'),
    f.max(f.when(f.col('recoRank') == 2, f.col('recipeUrl'))).alias('rcr_sku2_prd_ldp'),
    f.lit('NLA').cast('string').alias('rcr_sku3_prd_cat'),
    f.max(f.when(f.col('recoRank') == 3, f.concat(f.concat(f.concat(f.lit(mcoU + '_' + countryCodeU + '_'), f.col('language')), f.lit('_')), f.col('recipeId')))).alias('rcr_sku3_prd_cod'),
    f.max(f.when(f.col('recoRank') == 3, f.col('recipeName'))).alias('rcr_sku3_prd_nme'),
    f.max(f.when(f.col('recoRank') == 3, f.col('propensity'))).alias('rcr_sku3_prd_scr'),
    f.max(f.when(f.col('recoRank') == 3, f.col('recipeUrl'))).alias('rcr_sku3_prd_ldp')) \
  .withColumnRenamed('operatorOhubId', 'opr_lnkd_integration_id')

# COMMAND ----------

dfRecoAcmTransformed \
  .write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .option("replaceWhere", "countryCode = '" + countryCodeU + "'") \
  .partitionBy("countryCode") \
  .save(deltaTableNameRecAcm)
  
sqlQuery1 = "drop table if exists " + hiveTableNameRecAcm
sqlQuery2 = "create table " + hiveTableNameRecAcm + " using delta location '" + deltaTableNameRecAcm + "'"
sqlQuery3 = "ALTER TABLE " + hiveTableNameRecAcm + " SET TBLPROPERTIES ('delta.logRetentionDuration'='interval 30 days', 'delta.deletedFileRetentionDuration'='interval 30 days')"
spark.sql(sqlQuery1)
spark.sql(sqlQuery2)
spark.sql(sqlQuery3)

# COMMAND ----------

# DBTITLE 1,Ufs.com reco output transform
dfRecoUfsTransformed = dfRecoUfs \
  .select('userId',
          'trackingId',
          f.when(f.col('recoType') == 'product', 'PRODUCT').when(f.col('recoType') == 'recipe', 'RECIPE').cast('string').alias('recommendationType'),
          'countryCode',
          f.col('language').alias('languageCode'),
          f.when(f.col('recoType') == 'product', f.col(productCodeType)).when(f.col('recoType') == 'recipe', f.col('recipeId')).alias('recommendation'),
          f.col('recoRank').alias('rank')) \

# COMMAND ----------

dfRecoUfsTransformed \
  .write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .option("replaceWhere", "countryCode = '" + countryCodeU + "'") \
  .partitionBy("countryCode") \
  .save(deltaTableNameRecUfs)
  
sqlQuery1 = "drop table if exists " + hiveTableNameRecUfs
sqlQuery2 = "create table " + hiveTableNameRecUfs + " using delta location '" + deltaTableNameRecUfs + "'"
sqlQuery3 = "ALTER TABLE " + hiveTableNameRecUfs + " SET TBLPROPERTIES ('delta.logRetentionDuration'='interval 30 days', 'delta.deletedFileRetentionDuration'='interval 30 days')"

spark.sql(sqlQuery1)
spark.sql(sqlQuery2)
spark.sql(sqlQuery3)

# COMMAND ----------

# DBTITLE 1,Dispatcher (oview) reco output transform ZA exclude webshop operators
if countryCodeU == 'ZA':
  dfWebshopBuyersZA = spark.table('dev_derived_reco.sales_max_corrected') \
    .where(f.col('countryCode') == 'ZA') \
    .where(f.lower(f.col('orderType')).contains('web') | f.lower(f.col('orderType')).contains('app')) \
    .where(f.date_format('transactionDate', 'yyyyMM') > f.col('maxYearMonthMin18')) \
    .where(f.date_format('transactionDate', 'yyyyMM') <= f.col('maxYearMonth')) \
    .select('operatorOhubId') \
    .distinct()
  dfRecoOview = dfRecoOview.join(dfWebshopBuyersZA, on='operatorOhubId', how='left_anti')
  print("Check - Excluded operators with webshop sales for ZA")

# COMMAND ----------

# DBTITLE 1,Dispatcher (oview) reco output transform
# If there are personalized rows, check if there are sets of 3 products and recipes available for any operator, if not, do not create oview reco table
if not dfRecoOview.cache().rdd.isEmpty():
  hasCompletePersonalizedRecos = True  
  print(countryCodeU + ' has personalized recos')
  dfRecoOviewTransformed = dfRecoOview \
    .select(
      'countryCode',
      f.col('operatorOhubId').alias('opr_lnkd_integration_id'),
      f.lit('REC-O').alias('source'),
      f.lit(None).cast('double').alias('loyalty_status'),
      f.when(f.col('recoType') == 'product', 'Whitespot Product Recommendation').when(f.col('recoType') == 'recipe', 'Whitespot Recipe Recommendation').alias('recommendation_type'),
      f.when(f.col('recoType') == 'product', f.col('cuEanCodes')).when(f.col('recoType') == 'recipe', f.col('recipeId')).cast('string').alias('recommendation_id'),
      f.when(f.col('recoType') == 'product', f.col('productName')).when(f.col('recoType') == 'recipe', f.col('recipeName')).cast('string').alias('recommendation_name'),
      f.when(f.col('recoRank') == 1, 'High').when(f.col('recoRank') == 2, 'Medium').otherwise('Low').alias('recommendation_level'),
      f.col('recoRank').alias('recommendation_rank'))
else:
  hasCompletePersonalizedRecos = False
  print(countryCodeU + ' does not have full personalized recommendation rows')

# COMMAND ----------

if hasCompletePersonalizedRecos:
  dfRecoOviewTransformed \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("replaceWhere", "countryCode = '" + countryCodeU + "'") \
    .partitionBy("countryCode") \
    .save(deltaTableNameRecOview)

  sqlQuery1 = "drop table if exists " + hiveTableNameRecOview
  sqlQuery2 = "create table " + hiveTableNameRecOview + " using delta location '" + deltaTableNameRecOview + "'"
  sqlQuery3 = "ALTER TABLE " + hiveTableNameRecOview + " SET TBLPROPERTIES ('delta.logRetentionDuration'='interval 30 days', 'delta.deletedFileRetentionDuration'='interval 30 days')"

  spark.sql(sqlQuery1)
  spark.sql(sqlQuery2)
  spark.sql(sqlQuery3)

# COMMAND ----------

# DBTITLE 1,Marketo reco output transform
# Marketo start and end date of the recommendation 
today = datetime.now(timezone('Europe/Amsterdam')).date()
lastSunday = today - timedelta(days=((today.weekday() + 1) % 7))
lastSundayString = lastSunday.strftime('%Y%m%d')

recoDateStart = datetime.strftime(datetime.strptime(lastSundayString, '%Y%m%d') + timedelta(days=8), "%Y%m%d %H:%M:%S")
recoDateEnd = datetime.strftime(datetime.strptime(lastSundayString, '%Y%m%d') + timedelta(days=15), "%Y%m%d %H:%M:%S")

print(recoDateStart)
print(recoDateEnd)

dfRecoMarketoTransformed = dfRecoMarketo \
  .select(
    'countryCode',
    'operatorOhubId',
    f.when(f.col('recoType') == 'product', f.col('cuEanCodes')).when(f.col('recoType') == 'recipe', f.col('recipeId')).cast('string').alias('recommendationCode'),
    f.when(f.col('recoType') == 'product', 'product').when(f.col('recoType') == 'recipe', 'recipe').alias('recommendationCodeType'),
    f.when(f.col('recoType') == 'product', f.col('productName')).when(f.col('recoType') == 'recipe', f.col('recipeName')).alias('recommendationName'),
    f.when(f.col('personalizedOverwritten') == 1, 'personalized whitespot').otherwise('random whitespot').alias('recommendationType'),
    f.col('recoRank').alias('recommendationRank'),
    f.when(f.col('recoType') == 'product', f.col('productUrl')).when(f.col('recoType') == 'recipe', f.col('recipeUrl')).alias('recommendationUrl'),
    f.when(f.col('recoType') == 'product', f.col('productImageUrl')).when(f.col('recoType') == 'recipe', f.col('recipeImageUrl')).alias('recommendationImageUrl'),
    f.lit(recoDateStart).alias('recommendationDateStart'),
    f.lit(recoDateEnd).alias('recommendationDateEnd'))

# COMMAND ----------

dfRecoMarketoTransformed \
  .write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .option("mergeSchema", "true") \
  .option("replaceWhere", "countryCode = '" + countryCodeU + "'") \
  .partitionBy("countryCode") \
  .save(deltaTableNameRecMarketo)

sqlQuery1 = "drop table if exists " + hiveTableNameRecMarketo
sqlQuery2 = "create table " + hiveTableNameRecMarketo + " using delta location '" + deltaTableNameRecMarketo + "'"
sqlQuery3 = "ALTER TABLE " + hiveTableNameRecMarketo + " SET TBLPROPERTIES ('delta.logRetentionDuration'='interval 30 days', 'delta.deletedFileRetentionDuration'='interval 30 days')"

spark.sql(sqlQuery1)
spark.sql(sqlQuery2)
spark.sql(sqlQuery3)

# COMMAND ----------

# countryCodeU = 'PL'
dfRecoLDA = spark.table('dev_derived_reco.output_recommendations_oview') \
  .where(f.col('countryCode') == countryCodeU) \
  .where(f.lower(f.col('recommendation_type')).contains('product')) \
  .select(
    'countryCode',
    f.col('opr_lnkd_integration_id').alias('operatorOhubId'),
    f.col('recommendation_id').alias('cuEanCode'),
    f.col('recommendation_rank').alias('recommendationRank'),
    f.col('recommendation_name').alias('recommendationName')
  )

# COMMAND ----------

dfRecoLDA \
  .write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .option("mergeSchema", "true") \
  .option("replaceWhere", "countryCode = '" + countryCodeU + "'") \
  .partitionBy("countryCode") \
  .save(deltaTableNameRecLDA)

sqlQuery1 = "drop table if exists " + hiveTableNameRecLDA
sqlQuery2 = "create table " + hiveTableNameRecLDA + " using delta location '" + deltaTableNameRecLDA + "'"
sqlQuery3 = "ALTER TABLE " + hiveTableNameRecLDA + " SET TBLPROPERTIES ('delta.logRetentionDuration'='interval 30 days', 'delta.deletedFileRetentionDuration'='interval 30 days')"

spark.sql(sqlQuery1)
spark.sql(sqlQuery2)
spark.sql(sqlQuery3)
