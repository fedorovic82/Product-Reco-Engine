# Databricks notebook source
# MAGIC %md
# MAGIC configure country code

# COMMAND ----------

#var_runCountryCode = getArgument("runCountryCode")
var_runCountryCode = 'PL'

# COMMAND ----------

#var_runAreaCode = getArgument("runAreaCode")
var_runAreaCode = 'emea'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC refresh table data_reco_input.ods_reco_operators_withcontact;
# MAGIC 
# MAGIC Select  count(*),
# MAGIC         count(distinct countryCode || ' / ' || operatorOhubId)
# MAGIC From data_reco_input.ods_reco_operators_withcontact
# MAGIC Where 1=1
# MAGIC ;

# COMMAND ----------


df_operators = spark.table('data_reco_input.ods_reco_operators_withcontact')
df_db_operators = df_operators.where(df_operators.countryCode == var_runCountryCode)

# COMMAND ----------

from filterRank.filterItems.filterProductRecipeCombinations import getFilteredProductRecipeCombos
from filterRank.filterItems.filterRecipes import getRecipesCurrentSeason
from filterRank.filterItems.currentSeason import getSeasonfromDate
from filterRank.filterItems.filterProducts import getProductsperBusinessChannel
from filterRank.filterItems.filterProductRecipeCombinations import getFilteredProductRecipeCombos
from filterRank.filterItems.addRecos import df_add_recos

# COMMAND ----------

# MAGIC %md
# MAGIC Load Configuration Files

# COMMAND ----------

table_name_product_details = 'data_config_'+str(var_runAreaCode)+'.'+str(var_runCountryCode).lower()+'_product_details'
df_db_product = spark.table(table_name_product_details)
df_db_product = df_db_product.fillna('')
df_product = df_db_product.toPandas()

table_name_recipe_details = 'data_config_'+str(var_runAreaCode)+'.'+str(var_runCountryCode).lower()+'_recipe_details'
df_db_recipe = spark.table(table_name_recipe_details)
df_db_recipe = df_db_recipe.fillna('')
df_recipe = df_db_recipe.toPandas()

table_name_recipe_product_mapping  = 'data_config_'+str(var_runAreaCode)+'.'+str(var_runCountryCode).lower()+'_recipe_product_mapping'
df_db_recipe_product_mapping = spark.table(table_name_recipe_product_mapping)
df_db_recipe_product_mapping = df_db_recipe_product_mapping.fillna('')
df_recipe_product_mapping = df_db_recipe_product_mapping.toPandas()

#getFilteredProductRecipeCombos(df_product, df_recipe, df_recipe_product_mapping, 'restaurant')

# COMMAND ----------

# MAGIC %md
# MAGIC Test Egg function

# COMMAND ----------

#print(df_recipe['relevantSeason'])

# COMMAND ----------

from pyspark.sql.functions import *

df_db_operators = df_db_operators.withColumn("LOCAL_CHANNEL", regexp_replace(col("LOCAL_CHANNEL"), "\*", "\\\*"))
df_db_operators.show()

# COMMAND ----------

df_db_operators_reco_set = df_add_recos(df_product, df_recipe, df_recipe_product_mapping, df_db_operators, 'operatorOhubId','LOCAL_CHANNEL')
table_name_recommendation_set = 'data_reco_input.'+str(var_runCountryCode).lower()+'_recommendation_set'
df_db_operators_reco_set = df_db_operators_reco_set.withColumn("LOCAL_CHANNEL", regexp_replace(col("LOCAL_CHANNEL"), "(\\\*)", "\*"))
# df_db_operators_reco_set.write.mode("Overwrite").saveAsTable(table_name_recommendation_set)
df_db_operators_reco_set.show()

# COMMAND ----------

#%fs rm -r dbfs:/user/hive/warehouse/data_reco_input.db/pt_recommendation_set

# COMMAND ----------

getProductsperBusinessChannel(df_product, operatorgroup)
