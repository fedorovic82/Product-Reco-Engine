# Databricks notebook source
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql import window as w

# COMMAND ----------

dfHitsPage = spark.table('dev_derived_integration_ga.output_ga_fact_hits')
dfSessions = spark.table('dev_derived_integration_ga.output_ga_fact_sessions')

# COMMAND ----------

# DBTITLE 1,Calculate user level metrics
dfTimeFrames = dfSessions \
  .groupBy('countryCode') \
  .agg(f.max('visitStartTimeConverted').alias('maxVisitStartTimeConverted')) \
  .withColumn('maxDate', f.to_date('maxVisitStartTimeConverted')) \
  .withColumn('maxDateMin3', f.add_months(f.col('maxDate'), -3)) \
  .withColumn('maxDateMin6', f.add_months(f.col('maxDate'), -6))

# COMMAND ----------

hiveTable = "dev_derived_reco.ga_time_frames"
deltaTable = '/mnt/datamodel/dev/derived/reco/ga/time_frames'

dfTimeFrames \
  .write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .save(deltaTable)

sqlQuery1 = "drop table if exists " + hiveTable
sqlQuery2 = "create table " + hiveTable + " using delta location '" + deltaTable + "'"
spark.sql(sqlQuery1)
spark.sql(sqlQuery2)

# COMMAND ----------

# Add trackingId and columns to filter on date
dfHitsPageMaxCorrected = dfHitsPage \
  .withColumnRenamed('date', 'gaDate') \
  .join(dfTimeFrames, on='countryCode', how='inner')

# COMMAND ----------

hiveTable = "dev_derived_reco.ga_fact_hits_max_corrected"
deltaTable = '/mnt/datamodel/dev/derived/reco/ga/fact_hits_max_corrected'

dfHitsPageMaxCorrected \
  .write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .save(deltaTable)

sqlQuery1 = "drop table if exists " + hiveTable
sqlQuery2 = "create table " + hiveTable + " using delta location '" + deltaTable + "'"
spark.sql(sqlQuery1)
spark.sql(sqlQuery2)

# COMMAND ----------

# Join the hits page info and create metrics (avgPageDept, avgTimeOnSite, etc..)
dfUsers = dfHitsPageMaxCorrected \
  .groupBy("countryCode", "trackingId") \
  .agg(
    f.countDistinct(f.concat(f.col('fullvisitorId'), f.col('visitId'))).alias('cntVisits'),
    f.min(f.col('visitStartTimeConverted')).alias('firstVisit'),
    f.max(f.col('visitStartTimeConverted')).alias('lastVisit'),
    f.countDistinct(f.concat(f.col('fullvisitorId'), f.col('visitId'), f.col('pagePath'))).alias('cntPages'),
    f.sum(f.col('time')/1000).alias('timeOnSite')
  ) \
  .withColumn('weeksSinceLastView', f.round(f.datediff(f.current_date(), f.to_date(f.col('lastVisit'),'yyyy-MM-dd'))/7,0).cast(t.IntegerType())) \
  .withColumn('avgTimeOnSite', f.round((f.col('timeOnSite')/f.col('cntVisits')) / 7, 0).cast(t.IntegerType())) \
  .withColumn('avgPageDepth', (f.col('cntPages')/f.col('cntVisits')).cast(t.DoubleType()))

# COMMAND ----------

hiveTable = "dev_derived_reco.ga_users"
deltaTable = '/mnt/datamodel/dev/derived/reco/ga/users'

dfUsers \
  .write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .save(deltaTable)

sqlQuery1 = "drop table if exists " + hiveTable
sqlQuery2 = "create table " + hiveTable + " using delta location '" + deltaTable + "'"
spark.sql(sqlQuery1)
spark.sql(sqlQuery2)

# COMMAND ----------

# DBTITLE 1,Create product views
dfProductViews = dfHitsPageMaxCorrected \
  .where(f.col('productCode').isNotNull() & (f.col('productCode') != '')) \
  .where(f.to_date(f.col('visitStartTimeConverted')) >= f.col('maxDateMin' + str(3))) \
  .where(f.to_date(f.col('visitStartTimeConverted')) <= f.col('maxDate')) \
  .groupBy('countryCode', 'productCode') \
  .agg(f.countDistinct(f.concat('fullVisitorId', 'visitId')).alias('productViews')) \
  .withColumn('productViewsNtile', f.ntile(10).over(w.Window.partitionBy('countryCode').orderBy(f.col('productViews').asc())))

# COMMAND ----------

hiveTable = "dev_derived_reco.ga_product_views"
deltaTable = '/mnt/datamodel/dev/derived/reco/ga/product_views'

dfProductViews \
  .write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .save(deltaTable)

sqlQuery1 = "drop table if exists " + hiveTable
sqlQuery2 = "create table " + hiveTable + " using delta location '" + deltaTable + "'"
spark.sql(sqlQuery1)
spark.sql(sqlQuery2)

# COMMAND ----------

# DBTITLE 1,Create recipes views
dfRecipeViews = dfHitsPageMaxCorrected \
  .where(f.col('recipeId').isNotNull() & (f.col('recipeId') != '')) \
  .where(f.to_date(f.col('visitStartTimeConverted')) >= f.col('maxDateMin' + str(3))) \
  .where(f.to_date(f.col('visitStartTimeConverted')) <= f.col('maxDate')) \
  .groupBy('countryCode', 'recipeId') \
  .agg(f.countDistinct(f.concat('fullVisitorId', 'visitId')).alias('recipeViews')) \
  .withColumn('recipeViewsNtile', f.ntile(10).over(w.Window.partitionBy('countryCode').orderBy(f.col('recipeViews').asc())))

# COMMAND ----------

hiveTable = "dev_derived_reco.ga_recipe_views"
deltaTable = '/mnt/datamodel/dev/derived/reco/ga/recipe_views'

dfRecipeViews \
  .write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .save(deltaTable)

sqlQuery1 = "drop table if exists " + hiveTable
sqlQuery2 = "create table " + hiveTable + " using delta location '" + deltaTable + "'"
spark.sql(sqlQuery1)
spark.sql(sqlQuery2)

# COMMAND ----------

# DBTITLE 1,Create controle and treatment group
hiveTable = "dev_derived_reco.ga_reco_validation_groups"
deltaTable = '/mnt/datamodel/dev/derived/reco/ga/reco_validation_groups'

dfAllTrackingIds = dfHitsPage \
  .select('trackingId') \
  .distinct() \
  .where(f.col('trackingId').isNotNull())

try:
  dfTrackingIdsAssigned = spark.table(hiveTable)
  oldGroupingAvailable = True
  writeMode = 'append'
except:
  print("No old split available")
  oldGroupingAvailable = False
  writeMode = 'overwrite'
  
if oldGroupingAvailable:
  dfRecoValidationGroupsToBeAssigned = dfAllTrackingIds.join(dfTrackingIdsAssigned, on='trackingId', how='left_anti')
else:
  dfRecoValidationGroupsToBeAssigned = dfAllTrackingIds
  
dfRecoValidationGroupsToBeAssigned.count()

# COMMAND ----------

from uuid import uuid4
uuid_udf= f.udf(lambda : str(uuid4()), t.StringType())

dfRecoValidationGroupsNewlyAssigned = dfRecoValidationGroupsToBeAssigned \
  .withColumn("id", uuid_udf()) \
  .withColumn("randomRank", f.ntile(10).over(w.Window.partitionBy().orderBy('id'))) \
  .withColumn("group", f.when(f.col('randomRank') == 1, "c1").when(f.col('randomRank') == 2, 'c2').otherwise("t"))

display(dfRecoValidationGroupsNewlyAssigned)

# COMMAND ----------

dfRecoValidationGroupsNewlyAssigned \
  .write \
  .format("delta") \
  .mode(writeMode) \
  .save(deltaTable)

sqlQuery1 = "drop table if exists " + hiveTable
sqlQuery2 = "create table " + hiveTable + " using delta location '" + deltaTable + "'"
spark.sql(sqlQuery1)
spark.sql(sqlQuery2)
