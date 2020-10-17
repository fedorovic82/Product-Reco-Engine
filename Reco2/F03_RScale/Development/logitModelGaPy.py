# Databricks notebook source
# MAGIC %run "/Shared/dataScience/Reco2/F02_analyticalTables/readFeatureMatrixFunction"

# COMMAND ----------

countryCodeL = getArgument("countryCode").lower()
recoDate = getArgument("date")

# COMMAND ----------

hiveTable = "data_reco_analytical_tables." + countryCodeL + "_analytical_table_ga"
dfAnalyticalGa = spark.table(hiveTable)

# COMMAND ----------

# -- method = 'newton'
# iter10 = 11 min
# iter100 = 48 min
# -- method = 'bfgs'
# iter10 = 9 min
# iter100 = 12 min
# -- method = 'lbfgs'
# iter10 = 7 min
# iter100 = 9 min

# COMMAND ----------

# import scaleai
from scaleai.py_econometrics.generic_functions.pandas_udf import pandas_udf_transform
from scaleai.py_econometrics.glm.pool.pool_logit import fit_logit_group

experiment = 'logit_ga_' + countryCodeL
idVar = 'trackingid'
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

dfAnalyticalGa \
  .groupBy(groupNameCol) \
  .apply(fit_ols_group_udf) \
  .write \
  .mode('overwrite') \
  .saveAsTable('data_reco_output.' + countryCodeL + '_scores_logit_ga_' + recoDate)
