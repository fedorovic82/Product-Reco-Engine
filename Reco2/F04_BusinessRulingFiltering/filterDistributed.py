# Databricks notebook source
from pyspark.sql import functions as f
from filterRank.filterItems.addRecos import df_add_recos

countryCodeL = getArgument("runCountryCode").lower()
countryCodeU = countryCodeL.upper()

# COMMAND ----------

spark.sql("refresh table data_reco_input.ods_reco_operators_withcontact")

# COMMAND ----------

dfOperators = spark.table('data_reco_input.ods_reco_operators_withcontact') \
  .where(f.col('countryCode') == countryCodeU)

if countryCodeU == 'PL':
  dfOperators = dfOperators.withColumn("LOCAL_CHANNEL", f.regexp_replace(f.col("LOCAL_CHANNEL"), "\*", "\\\*"))

# COMMAND ----------

tableNameProductDetails        = 'data_config.' + countryCodeL + '_product_details'
tableNameRecipeDetails         = 'data_config.' + countryCodeL + '_recipe_details'
tableNameRecipeProductMapping  = 'data_config.' + countryCodeL + '_recipe_product_mapping'

dfpProduct = spark.table(tableNameProductDetails) \
  .fillna('') \
  .toPandas()

dfpRecipe = spark.table(tableNameRecipeDetails) \
  .fillna('') \
  .toPandas()

dfpRecipeProductMapping = spark.table(tableNameRecipeProductMapping) \
  .fillna('') \
  .toPandas()

# COMMAND ----------

dfOperatorsRecoSet = df_add_recos(dfpProduct, dfpRecipe, dfpRecipeProductMapping, dfOperators, 'operatorOhubId', 'LOCAL_CHANNEL') \
  .withColumn('channelBased', f.when(f.col('productCode') != '', 1).otherwise(0))

# COMMAND ----------

dbutils.fs.rm('dbfs:/user/hive/warehouse/data_reco_input.db/' + countryCodeL + '_recommendation_set', True)

# COMMAND ----------

dfOperatorsRecoSet.write \
  .mode("overwrite") \
  .saveAsTable('data_reco_input.' + countryCodeL + '_recommendation_set')

# COMMAND ----------

# -- select  count(*),
# --         count(distinct countryCode || ' / ' || operatorOhubId)
# -- from data_reco_input.ods_reco_operators_withcontact;

# from filterRank.filterItems.filterProductRecipeCombinations import getFilteredProductRecipeCombos
# from filterRank.filterItems.filterRecipes import getRecipesCurrentSeason
# from filterRank.filterItems.currentSeason import getSeasonfromDate
# from filterRank.filterItems.filterProducts import getProductsperBusinessChannel
# getFilteredProductRecipeCombos(dfProduct, dfRecipe, dfRecipeProductMapping, 'restaurant')
# dfRecipe[dfRecipe['relevantSeason'].str.contains(str('Summer'))]
# getProductsperBusinessChannel(dfProduct, 'Education')
# getRecipesCurrentSeason(dfRecipe, getSeasonfromDate())
