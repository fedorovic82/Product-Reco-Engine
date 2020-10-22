# Databricks notebook source
from pyspark.sql import functions as f
from pyspark.sql import window as w

countryCodeU = 'DE'

# COMMAND ----------

# After which months to consider sales of 3 months old
month = spark.table('dev_derived_reco.sales_time_frames') \
  .where(f.col('countryCode') == countryCodeU) \
  .select('maxYearMonthMin6').collect()[0][0]

print(month)

# Load sales data of the last 6 months of sufficiently filled sales data
dfSales = spark.table('dev_sources_ohub.cleaned_operator_sales') \
  .where(f.col('countryCode') == countryCodeU) \
  .where(f.date_format(f.col('transactionDate'), 'yyyyMM') > month) \
  .where(f.date_format(f.col('transactionDate'), 'yyyyMM') <= f.date_format(f.current_date(), 'yyyyMM')) \
  .where(f.col('cuEanCode').isNotNull() & f.col('operatorOhubId').isNotNull()) \
  .select(
    f.col('operatorOhubId').alias('operatorOhubId'),
    f.col('cuEanCode').alias('cuEanCodeSales'),
    f.col('productName').alias('productNameSales')) \
  .distinct()

dfProductName = spark.table("data_user_guus.de_visualize_ranked_recommendations") \
  .where(f.col('countryCode') == countryCodeU) \
  .select(
    f.col('cuEanCodes').alias('cuEanCodeReco'),
    f.col('productName').alias('productNameReco')) \
  .dropDuplicates(['cuEanCodeReco'])

# Load recommendations (all reco's)
dfReco = spark.table("data_user_guus.de_visualize_ranked_recommendations") \
  .where(f.col('countryCode') == countryCodeU) \
  .where(f.col('recommendation_id_acm') == 1) \
  .where(f.col('personalized') == 1) \
  .select(
    f.col('operatorOhubId').alias('operatorOhubId'),
    f.col('cuEanCodes').alias('cuEanCodeReco')) \
  .distinct() \
  .join(dfProductName, on = 'cuEanCodeReco', how='inner') \
  .withColumn('codeNameReco', f.concat(f.col('cuEanCodeReco'), f.lit("_"), f.regexp_replace(f.col('productNameReco'), " ", "_")))

# COMMAND ----------

'''
# Create sales features for operators
'dev_derived_reco.sales_operators_active' # Active operators used to create the personalized recommendations
'dev_derived_integration_ids.output_operators' # Full operator base used to filter
'dev_derived_integration_ids.output_contacts_ids' # Full contacts base used to base the ranking
'''

# COMMAND ----------

dfJoin = dfSales.join(dfReco, on='operatorOhubId', how='inner')

# COMMAND ----------

dfJoin.agg(f.countDistinct('operatorOhubId')).show()

# COMMAND ----------

dfPivot = dfJoin \
  .groupBy('cuEanCodeSales', 'productNameSales') \
  .pivot('codeNameReco') \
  .agg(f.count('operatorOhubId'))

# COMMAND ----------

w1 = w.Window.partitionBy().orderBy(f.col('totalNumberOfBuyers').desc())

dfProductBuyers = dfJoin \
  .groupBy('cuEanCodeSales') \
  .agg(f.countDistinct('operatorOhubId').alias('totalNumberOfBuyers')) \
  .withColumn('volumeRank', f.row_number().over(w1))

# COMMAND ----------

dfSummary = dfPivot.join(dfProductBuyers, on='cuEanCodeSales', how='inner').na.fill(0)
dfSummary = dfSummary.withColumn('totalNumberOfBuyersCheck', sum(dfSummary[c] for c in [i for i in dfSummary.columns if i[0].isdigit()]))

# Order column names based on volumes of highest volume product
d = dfSummary.where(f.col('volumeRank') == 1).select([i for i in dfSummary.columns if i[0].isdigit()]).collect()[0].asDict()
dSorted = {k: v for k, v in sorted(d.items(), key=lambda item: item[1], reverse=True)}
sortedCols = list(dSorted.keys())

dfSummary = dfSummary.select(['volumeRank', 'cuEanCodeSales', 'productNameSales', 'totalNumberOfBuyers', 'totalNumberOfBuyersCheck'] + sortedCols)

for c in [i for i in dfSummary.columns if i[0].isdigit()]:
  dfSummary = dfSummary.withColumn(c + '_perc', f.round(f.col(c) / f.col('totalNumberOfBuyers'), 3))
  
dfSummary = dfSummary.cache()

# COMMAND ----------

percCols = [i for i in dfSummary.columns if (i[0].isdigit() and i[-1] == 'c' and i[-2] == 'r')]
display(dfSummary.orderBy(f.col('volumeRank').asc()).select(['volumeRank', 'cuEanCodeSales', 'productNameSales', 'totalNumberOfBuyers', 'totalNumberOfBuyersCheck'] + percCols))

# COMMAND ----------

# aantal kopers met complete profielen --> bij ons bekend business channel en een contact persoon
