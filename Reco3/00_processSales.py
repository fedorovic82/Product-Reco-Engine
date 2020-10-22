# Databricks notebook source
from pyspark.sql import functions as f
from pyspark.sql import window as w

# COMMAND ----------

dfSales = spark.table('dev_sources_ohub.cleaned_operator_sales').alias('dfSales')

# COMMAND ----------

# DBTITLE 1,Create Sales Timeframes per Country
w1 = w.Window.partitionBy('countryCode').orderBy('yearMonth')
w2 = w.Window.partitionBy('countryCode')

# Create timeframes currentYear --> max van afgelopen 12 maanden

dfSalesTimeFrames = dfSales \
  .select(
    'countryCode',
    f.date_format('transactionDate', 'yyyyMM').alias('yearMonth'),
    f.date_format('transactionDate', 'yyyy').alias('year'),
    'operatorOhubId',
    f.date_format(f.current_date(), 'yyyy').alias('currentYear')) \
  .groupBy('countryCode', 'yearMonth', 'year', 'currentYear') \
  .agg(f.countDistinct('operatorOhubId').alias('countOperators')) \
  .groupBy('countryCode', 'yearMonth', 'year', 'currentYear', 'countOperators') \
  .agg(
    f.lag('yearMonth').over(w1).alias('yearMonthMin1'),
    f.lag(f.col('countOperators')).over(w1).alias('countOperatorsMin1'),
    f.max(f.when(f.col('year') == f.col('currentYear'), f.col('countOperators'))).over(w2).alias('yearMonthMax')) \
  .withColumn('percOps1', f.col('countOperators')/f.col('yearMonthMax')) \
  .withColumn('percOps2', f.col('countOperators')/f.col('countOperatorsMin1')) \
  .where((f.col('yearMonth') <= f.date_format(f.add_months(f.current_date(), 1), 'yyyyMM')) & 
         (f.col('percOps1') >= 0.6) & 
         (f.col('percOps2') >= 0.6)) \
  .groupBy('countryCode') \
  .agg(f.max('yearMonth').alias('maxYearMonth'),
       f.date_format(f.add_months(f.to_date(f.concat(f.substring(f.max('yearMonth'),1,4), f.lit('-'), f.substring(f.max('yearMonth'),5,2), f.lit('-01'))), -3), 'yyyyMM').alias('maxYearMonthMin3'),
       f.date_format(f.add_months(f.to_date(f.concat(f.substring(f.max('yearMonth'),1,4), f.lit('-'), f.substring(f.max('yearMonth'),5,2), f.lit('-01'))), -6), 'yyyyMM').alias('maxYearMonthMin6'),
       f.date_format(f.add_months(f.to_date(f.concat(f.substring(f.max('yearMonth'),1,4), f.lit('-'), f.substring(f.max('yearMonth'),5,2), f.lit('-01'))), -9), 'yyyyMM').alias('maxYearMonthMin9'),
       f.date_format(f.add_months(f.to_date(f.concat(f.substring(f.max('yearMonth'),1,4), f.lit('-'), f.substring(f.max('yearMonth'),5,2), f.lit('-01'))), -12), 'yyyyMM').alias('maxYearMonthMin12'),
       f.date_format(f.add_months(f.to_date(f.concat(f.substring(f.max('yearMonth'),1,4), f.lit('-'), f.substring(f.max('yearMonth'),5,2), f.lit('-01'))), -15), 'yyyyMM').alias('maxYearMonthMin15'),
       f.date_format(f.add_months(f.to_date(f.concat(f.substring(f.max('yearMonth'),1,4), f.lit('-'), f.substring(f.max('yearMonth'),5,2), f.lit('-01'))), -18), 'yyyyMM').alias('maxYearMonthMin18')) 

# COMMAND ----------

deltaTable = '/mnt/datamodel/dev/derived/reco/sales/time_frames'
hiveTable = 'dev_derived_reco.sales_time_frames'

dfSalesTimeFrames.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .partitionBy("countryCode") \
  .save(deltaTable)

sqlQuery1 = 'drop table if exists ' + hiveTable
sqlQuery2 = "create table " + hiveTable + " using delta location " + "'" + deltaTable + "'"
spark.sql(sqlQuery1)
spark.sql(sqlQuery2)

# COMMAND ----------

dfSalesMaxCorrected = dfSales \
  .join(dfSalesTimeFrames, on='countryCode', how="inner") \
  .where(f.col('cuEanCode').isNotNull() & f.col('transactionDate').isNotNull())

# COMMAND ----------

deltaTable = '/mnt/datamodel/dev/derived/reco/sales/max_corrected'
hiveTable = 'dev_derived_reco.sales_max_corrected'

dfSalesMaxCorrected.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .partitionBy("countryCode") \
  .save(deltaTable)

sqlQuery1 = 'drop table if exists ' + hiveTable
sqlQuery2 = "create table " + hiveTable + " using delta location " + "'" + deltaTable + "'"
spark.sql(sqlQuery1)
spark.sql(sqlQuery2)

# COMMAND ----------

# DBTITLE 1,Filter the Sales Data (not older than 18 months and not more recent than today)
dfSalesMaxCorrectedFiltered = dfSalesMaxCorrected \
  .where(
    (f.date_format('transactionDate', 'yyyyMM') > f.col('maxYearMonthMin18')) &
    (f.date_format('transactionDate', 'yyyyMM') <= f.col('maxYearMonth'))) \
  .select(
    "dfSales.*", 
    f.date_format('transactionDate', 'yyyyMMdd').cast('string').alias('transactionDateString'),
    'maxYearMonth',
    'maxYearMonthMin3',
    'maxYearMonthMin6',
    'maxYearMonthMin9',
    'maxYearMonthMin12',
    'maxYearMonthMin15',
    'maxYearMonthMin18' 
  )  

# COMMAND ----------

deltaTable = '/mnt/datamodel/dev/derived/reco/sales/max_corrected_filtered'
hiveTable = 'dev_derived_reco.sales_max_corrected_filtered'

dfSalesMaxCorrectedFiltered.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .partitionBy("countryCode") \
  .save(deltaTable)

sqlQuery1 = 'drop table if exists ' + hiveTable
sqlQuery2 = "create table " + hiveTable + " using delta location " + "'" + deltaTable + "'"
spark.sql(sqlQuery1)
spark.sql(sqlQuery2)

# COMMAND ----------

# DBTITLE 1,Select the operators that have bought something in last 6 months of the timeframe per country: these are the active operators
dfOperators = dfSales \
  .join(dfSalesTimeFrames, on='countryCode', how='inner') \
  .where(f.date_format(f.col('transactionDate'), 'yyyyMM') > f.col('maxYearMonthMin6')) \
  .select('countryCode', 'operatorOhubId') \
  .distinct()

# COMMAND ----------

hiveTable = 'dev_derived_reco.sales_operators_active'
deltaTable = '/mnt/datamodel/dev/derived/reco/sales/operators_active'

dfOperators \
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

# DBTITLE 1,Product sales count
dfProductSales = dfSalesMaxCorrectedFiltered \
  .where(f.col('cuEanCode').isNotNull() & (f.col('cuEanCode') != '')) \
  .where(f.date_format('transactionDate', 'yyyyMM') > f.col('maxYearMonthMin' + str(3))) \
  .groupBy('countryCode', 'cuEanCode') \
  .agg(f.countDistinct(f.concat('operatorOhubId')).alias('productSales')) \
  .withColumn('productSalesNtile', f.ntile(10).over(w.Window.partitionBy('countryCode').orderBy(f.col('productSales').asc())))

# COMMAND ----------

hiveTable = "dev_derived_reco.sales_product_sales"
deltaTable = '/mnt/datamodel/dev/derived/reco/sales/product_sales'

dfProductSales \
  .write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .save(deltaTable)

sqlQuery1 = "drop table if exists " + hiveTable
sqlQuery2 = "create table " + hiveTable + " using delta location '" + deltaTable + "'"
spark.sql(sqlQuery1)
spark.sql(sqlQuery2)
