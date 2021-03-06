# Databricks notebook source
from pyspark.sql import functions as f

countryCodeL = getArgument("countryCode").lower()
# countryCodeL = 'za'
countryCodeU = countryCodeL.upper()
recoDate = getArgument("date")
# recoDate = '20200415'

# COMMAND ----------

dfAnalyticalGa = spark.table("dev_derived_reco.analytical_ga_" + countryCodeL)

# COMMAND ----------

from scaleai.py_econometrics.generic_functions.pandas_udf import pandas_udf_transform
from scaleai.py_econometrics.glm.pool.pool_logit import fit_logit_group

experiment = 'logit_ga_' + countryCodeL
idVar = 'trackingId'
groupNameCol = 'FEATURE'
experimentName = '/Users/guus.verstegen@unilever.com/Experiments/' + experiment + '_' + recoDate
artifactLocation = 'dbfs:/user/guus/mlflow/' + experiment + '_' + recoDate
outputSchema = idVar + ' string, label string, score float'

fit_ols_group_udf = pandas_udf_transform(
  inputFunction = fit_logit_group, 
  outputSchema = outputSchema, 
  idVar = idVar, 
  depVar = 'VALUE', 
  label = 'logit', 
  groupNameCol = groupNameCol, 
  patternDep = 'dep_', 
  patternInd = 'ind_', 
  maxIter = 10000,
  experimentName = experimentName, 
  artifactLocation = artifactLocation,
  saveModel = False,
  saveScores = True,
  saveEstimates = True,
  method='bfgs'
)

deltaTableScore = "/mnt/datamodel/dev/derived/reco/scores/ga/logit"
hiveTableScore = "dev_derived_reco.scores_ga_logit" 

dfAnalyticalGa \
  .groupBy(groupNameCol) \
  .apply(fit_ols_group_udf) \
  .withColumn("countryCode", f.lit(countryCodeU)) \
  .write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .option("replaceWhere", "countryCode = '" + countryCodeU + "'") \
  .partitionBy("countryCode") \
  .save(deltaTableScore)

# COMMAND ----------

sqlQuery1 = "drop table if exists " + hiveTableScore
sqlQuery2 = "create table " + hiveTableScore + " using delta location '" + deltaTableScore + "'"
spark.sql(sqlQuery1)
spark.sql(sqlQuery2)
