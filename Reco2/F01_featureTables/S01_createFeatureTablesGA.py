# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

varCountryCode = getArgument("varCountryCode")
largeCountries = getArgument("largeCountries").split(',')

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table data_googleanalytics.ods_hits_page;
# MAGIC refresh table data_googleanalytics.ods_hits_page_pdp;
# MAGIC refresh table data_googleanalytics.ods_hits_page_rcp;

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

# dfPageviews = spark.read.format("delta").load("/delta/ods_hits_page")
# dfProductPageviews = spark.read.format("delta").load("/delta/ods_hits_page_pdp")
# dfUsers = spark.read.format("delta").load("/delta/ods_users")

dfPageviews = spark.table("data_googleanalytics.ods_hits_page")
dfProductPageviews = spark.table("data_googleanalytics.ods_hits_page_pdp")
dfRecipePageviews = spark.table("data_googleanalytics.ods_hits_page_rcp")
dfUsers = spark.table("data_googleanalytics.ods_users")

# COMMAND ----------

# Filter data
dfUsers = dfUsers.where(F.col('country_code') == varCountryCode)
dfProductPageviews = dfProductPageviews.where(F.col('country_code') == varCountryCode)
dfRecipePageviews = dfRecipePageviews.where(F.col('country_code') == varCountryCode)  
 
if varCountryCode.upper() in largeCountries:
  dfPageviewsIds = dfPageviews \
    .withColumn('date_dateformat', F.to_date(F.col('date'),'yyyyMMdd')) \
    .where(F.col('date_dateformat') >= F.col('max_date_min3')).select('trackingid').distinct()
  dfUsers = dfUsers.join(dfPageviewsIds, ['trackingid'], 'inner')

# COMMAND ----------

if varCountryCode.upper() in largeCountries:
  varTimeframeEnd = 3
if varCountryCode.upper() not in largeCountries:
  varTimeframeEnd = 6  

# COMMAND ----------

# Create Dependent Variables for Product Pageviews 
dfProductPageviewsDepDummies = createBehaviorFeatures(dataframe_ids = dfUsers,
                                        dataframe_feature = dfProductPageviews,    #
                                        feature = "productCode_Corrected",            # group by id, feature
                                        ID = "trackingid",                            # operator id
                                        metric = "",                                  # not relevant for feature dummies
                                        feature_type = 'dummy',                       # occurrence or not
                                        date_column = 'ga_date',                      #
                                        lower_bound = 80,                             # minimum number of occurrences of feature
                                        timeframe_start = 0,                          # months back in time from latest date
                                        timeframe_end = varTimeframeEnd,              # months back in time from latest date
                                        prefix = 'dep_dum_ppv_',                      # prefix for each dummy
                                        denominator_metric = '',                      # not relevant for feature dummies
                                        denominator_type = '',                        # not relevant for feature dummies
                                        indicator_unixtime = 0)                       # 1 if date is unix time, 0 if not

# transform df to long format
dfProductPageviewsDepDummiesLong = unpivotDataframe(df = dfProductPageviewsDepDummies,
                                  id_vars = ["trackingid"],
                                  value_vars = dfProductPageviewsDepDummies.columns[1:],
                                  var_name = "FEATURE",
                                  value_name = "VALUE")

# Add country_code
dfProductPageviewsDepDummiesLong = dfProductPageviewsDepDummiesLong \
  .withColumn('country_code', F.lit(varCountryCode)) \
  .withColumn('featureType', F.lit('productPageview'))

# Save Dataframe  
dfProductPageviewsDepDummiesLong.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .option("replaceWhere", "country_code = '" + varCountryCode + "'") \
  .partitionBy("country_code", "featureType") \
  .save("/delta/ods_features_pageviews")

# COMMAND ----------

# Create Independent Variables for Product Pageviews 
dfProductPageviewsIndDummies = createBehaviorFeatures(dataframe_ids = dfUsers,
                                        dataframe_feature = dfProductPageviews,    #
                                        feature = "productCode_Corrected",            # group by id, feature
                                        ID = "trackingid",                            # operator id
                                        metric = "",                                  # not relevant for feature dummies
                                        feature_type = 'dummy',                       # occurrence or not
                                        date_column = 'ga_date',                      #
                                        lower_bound = 30,                             # minimum number of occurrences of feature
                                        timeframe_start = 0,                          # months back in time from latest date
                                        timeframe_end = varTimeframeEnd,              # months back in time from latest date
                                        prefix = 'ind_dum_ppv_',                      # prefix for each dummy
                                        denominator_metric = '',                      # not relevant for feature dummies
                                        denominator_type = '',                        # not relevant for feature dummies
                                        indicator_unixtime = 0)                       # 1 if date is unix time, 0 if not

# transform df to long format
dfProductPageviewsIndDummiesLong = unpivotDataframe(df = dfProductPageviewsIndDummies,
                                  id_vars = ["trackingid"],
                                  value_vars = dfProductPageviewsIndDummies.columns[1:],
                                  var_name = "FEATURE",
                                  value_name = "VALUE")

# transform df to long format
dfProductPageviewsIndDummiesLong = dfProductPageviewsIndDummiesLong \
  .withColumn('country_code', F.lit(varCountryCode)) \
  .withColumn('featureType', F.lit('productPageview'))

dfProductPageviewsIndDummiesLong.write \
  .format("delta") \
  .mode("append") \
  .option("overwriteSchema", "true") \
  .partitionBy("country_code","featureType") \
  .save("/delta/ods_features_pageviews")

# COMMAND ----------

# Create Dependent Variables for Product Pageviews 
dfProductPageviewsDepDummies = createBehaviorFeatures(dataframe_ids = dfUsers,
                                        dataframe_feature = dfRecipePageviews,
                                        feature = "recipeCode_Corrected",             # group by id, feature
                                        ID = "trackingid",                            # operator id
                                        metric = "",                                  # not relevant for feature dummies
                                        feature_type = 'dummy',                       # occurrence or not
                                        date_column = 'ga_date',
                                        lower_bound = 80,                             # minimum number of occurrences of feature
                                        timeframe_start = 0,                          # months back in time from latest date
                                        timeframe_end = varTimeframeEnd,              # months back in time from latest date
                                        prefix = 'dep_dum_rpv_',                      # prefix for each dummy
                                        denominator_metric = '',                      # not relevant for feature dummies
                                        denominator_type = '',                        # not relevant for feature dummies
                                        indicator_unixtime = 0)                       # 1 if date is unix time, 0 if not

# COMMAND ----------

dfProductPageviewsDepDummies.show()

# COMMAND ----------

# transform df to long format
dfRecipePageviewsDepDummiesLong = unpivotDataframe(df = dfProductPageviewsDepDummies,
                                  id_vars = ["trackingid"],
                                  value_vars = dfProductPageviewsDepDummies.columns[1:],
                                  var_name = "FEATURE",
                                  value_name = "VALUE")

# Add country_code
dfRecipePageviewsDepDummiesLong = dfRecipePageviewsDepDummiesLong \
  .withColumn('country_code', F.lit(varCountryCode)) \
  .withColumn('featureType', F.lit('recipePageview'))

# Save Dataframe  
dfRecipePageviewsDepDummiesLong.write \
  .format("delta") \
  .mode("append") \
  .option("overwriteSchema", "true") \
  .partitionBy("country_code", "featureType") \
  .save("/delta/ods_features_pageviews")

# COMMAND ----------

# Create Independent Variables for Product Pageviews 
dfRecipePageviewsIndDummies = createBehaviorFeatures(dataframe_ids = dfUsers,
                                        dataframe_feature = dfRecipePageviews,
                                        feature = "recipeCode_Corrected",             # group by id, feature
                                        ID = "trackingid",                            # operator id
                                        metric = "",                                  # not relevant for feature dummies
                                        feature_type = 'dummy',                       # occurrence or not
                                        date_column = 'ga_date',
                                        lower_bound = 30,                             # minimum number of occurrences of feature
                                        timeframe_start = 0,                          # months back in time from latest date
                                        timeframe_end = varTimeframeEnd,              # months back in time from latest date
                                        prefix = 'ind_dum_rpv_',                      # prefix for each dummy
                                        denominator_metric = '',                      # not relevant for feature dummies
                                        denominator_type = '',                        # not relevant for feature dummies
                                        indicator_unixtime = 0)                       # 1 if date is unix time, 0 if not

# transform df to long format
dfRecipePageviewsIndDummiesLong = unpivotDataframe(df = dfRecipePageviewsIndDummies,
                                  id_vars = ["trackingid"],
                                  value_vars = dfRecipePageviewsIndDummies.columns[1:],
                                  var_name = "FEATURE",
                                  value_name = "VALUE")

# transform df to long format
dfRecipePageviewsIndDummiesLong = dfRecipePageviewsIndDummiesLong \
  .withColumn('country_code', F.lit(varCountryCode)) \
  .withColumn('featureType', F.lit('recipePageview'))

# Save Dataframe  
dfRecipePageviewsIndDummiesLong.write \
  .format("delta") \
  .mode("append") \
  .option("overwriteSchema", "true") \
  .partitionBy("country_code","featureType") \
  .save("/delta/ods_features_pageviews")

# COMMAND ----------

# MAGIC %md
# MAGIC Create Visit Frequency Features

# COMMAND ----------

# Create Independent Variables for Product Pageviews 
dfUsersIndFrqDummies = createScaleDummies(dfUsers,
                                          "cnt_visits",        # group by id, feature
                                          "trackingid",        # operator id
                                          0,                   # minimum number of occurrences of feature
                                          "ind_dum_frq_"       # prefix for each dummy
                                          )

# transform df to long format
dfUsersIndFrqDummiesLong = unpivotDataframe(df = dfUsersIndFrqDummies,
                                  id_vars = ["trackingid"],
                                  value_vars = dfUsersIndFrqDummies.columns[1:],
                                  var_name = "FEATURE",
                                  value_name = "VALUE")

# transform df to long format
dfUsersIndFrqDummiesLong = dfUsersIndFrqDummiesLong \
  .withColumn('country_code', F.lit(varCountryCode)) \
  .withColumn('featureType', F.lit('userAttribute'))

# Save Dataframe  
dfUsersIndFrqDummiesLong.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .option("replaceWhere", "country_code = '" + varCountryCode + "'") \
  .partitionBy("country_code", "featureType") \
  .save("/delta/ods_features_users")

# COMMAND ----------

# MAGIC %md
# MAGIC Create Visit Recency Features

# COMMAND ----------

# Create Feature for Product Pageviews 
dfUsersIndRecDummies = createScaleDummies(dfUsers,   
                                          "weeks_since_last_view",      # group by id, feature
                                          "trackingid",                 # operator id
                                          0,                            # minimum number of occurrences of feature
                                          'ind_dum_rec_'                # prefix for each dummy
                                          )

# transform df to long format
dfUsersIndRecDummiesLong = unpivotDataframe(df = dfUsersIndRecDummies,
                                  id_vars = ["trackingid"],
                                  value_vars = dfUsersIndRecDummies.columns[1:],
                                  var_name = "FEATURE",
                                  value_name = "VALUE")

# transform df to long format
dfUsersIndRecDummiesLong = dfUsersIndRecDummiesLong \
  .withColumn('country_code', F.lit(varCountryCode)) \
  .withColumn('featureType', F.lit('userAttribute'))

# Save Dataframe    
dfUsersIndRecDummiesLong.write \
  .format("delta") \
  .mode("append") \
  .option("overwriteSchema", "true") \
  .partitionBy("country_code", "featureType") \
  .save("/delta/ods_features_users")

# COMMAND ----------

# MAGIC %md
# MAGIC Create Visit Average Time On Site Features

# COMMAND ----------

# Create Independent Variables for Product Pageviews 
dfUsersIndDurDummies = createScaleDummies(dfUsers,   
                                          "avg_time_on_site",           # group by id, feature
                                          "trackingid",                 # operator id
                                          0,                            # minimum number of occurrences of feature
                                          'ind_dum_dur_'                # prefix for each dummy
                                          )

# transform df to long format
dfUsersIndDurDummiesLong = unpivotDataframe(df = dfUsersIndDurDummies,
                                  id_vars = ["trackingid"],
                                  value_vars = dfUsersIndDurDummies.columns[1:],
                                  var_name = "FEATURE",
                                  value_name = "VALUE")

# transform df to long format
dfUsersIndDurDummiesLong = dfUsersIndDurDummiesLong \
  .withColumn('country_code', F.lit(varCountryCode)) \
  .withColumn('featureType', F.lit('userAttribute'))

# Save Dataframe    
dfUsersIndDurDummiesLong.write \
  .format("delta") \
  .mode("append") \
  .option("overwriteSchema", "true") \
  .partitionBy("country_code", "featureType") \
  .save("/delta/ods_features_users")

# COMMAND ----------

# MAGIC %md
# MAGIC Create Average Page Depth Features

# COMMAND ----------

# Create Independent Variables for Product Pageviews 
# dfUsersIndPgdDummies = createScaleDummies(dfUsers,   
#                                           "avg_page_depth",   # group by id, feature
#                                           "trackingid",       # operator id
#                                           0,                  # minimum number of occurrences of feature
#                                           'ind_dum_pgd_'      # prefix for each dummy
#                                           )
# 
# # transform df to long format
# dfUsersIndPgdDummiesLong = unpivotDataframe(df = dfUsersIndPgdDummies,
#                                  id_vars = ["trackingid"],
#                                  value_vars = dfUsersIndPgdDummies.columns[1:],
#                                  var_name = "FEATURE",
#                                  value_name = "VALUE")
#
# # transform df to long format
# dfUsersIndPgdDummiesLong = dfUsersIndPgdDummiesLong.withColumn('country_code', F.lit(varCountryCode))
# 
# # Save Dataframe    
# dfUsersIndPgdDummiesLong.write.mode("Append").saveAsTable('data_reco_feature_tables.ods_features_users')
