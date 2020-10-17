# Databricks notebook source
# MAGIC %run "/Shared/dataScience/Reco2/F02_analyticalTables/readFeatureMatrixFunction"

# COMMAND ----------

from pyspark.sql import functions as f
countryCodeL = getArgument("countryCode").lower()
recoDate = getArgument("date")
hiveTablePrd = "data_reco_analytical_tables." + countryCodeL + "_ods_analytical_table_sales_prd"
hiveTableEst = "data_reco_analytical_tables." + countryCodeL + "_ods_analytical_table_sales_est"
dfAnalyticalSalesPrd = spark.table(hiveTablePrd).withColumn('splitOn', f.lit('train'))
dfAnalyticalSalesEst = spark.table(hiveTableEst).withColumn('splitOn', f.lit('test'))
dfAnalyticalSales = dfAnalyticalSalesEst.union(dfAnalyticalSalesPrd)

# COMMAND ----------

# import scaleai
from scaleai.py_econometrics.generic_functions.pandas_udf import pandas_udf_transform
from scaleai.py_econometrics.glm.pool.pool_logit import fit_logit_group

experiment = 'logit_sales_' + countryCodeL
idVar = 'operatorOhubId'
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
  splitColumn = 'splitOn',
  patternDep = 'dep_', 
  patternInd = 'ind_', 
  maxIter = 10000,
  experimentName = experimentName, 
  artifactLocation = artifactLocation,
  saveModel = False,
  saveScores = True,
  saveEstimates = True,
  method = 'bfgs'
)

dfAnalyticalSales \
  .groupBy(groupNameCol) \
  .apply(fit_ols_group_udf) \
  .write \
  .mode('overwrite') \
  .saveAsTable('data_reco_output.' + countryCodeL + '_scores_logit_sales_' + recoDate)
