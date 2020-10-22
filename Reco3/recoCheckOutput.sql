-- Databricks notebook source
-- MAGIC %python 
-- MAGIC tableNameInputReco      = 'dev_derived_reco.joined_recommendation_set' 
-- MAGIC 
-- MAGIC hiveTableNameRecAcm     = 'dev_derived_reco.output_recommendations_acm'
-- MAGIC deltaTableNameRecAcm    = '/mnt/datamodel/dev/derived/reco/output/recommendations_acm'
-- MAGIC hiveTableNameRecUfs     = 'dev_derived_reco.output_recommendations_ufs_com'
-- MAGIC deltaTableNameRecUfs    = '/mnt/datamodel/dev/derived/reco/output/recommendations_ufs_com'
-- MAGIC hiveTableNameRecOview   = 'dev_derived_reco.output_recommendations_oview'
-- MAGIC deltaTableNameRecOview  = '/mnt/datamodel/dev/derived/reco/output/recommendations_oview'
-- MAGIC hiveTableNameRecMarketo = 'dev_derived_reco.output_recommendations_marketo'
-- MAGIC deltaTableNameRecMarketo= '/mnt/datamodel/dev/derived/reco/output/recommendations_marketo'

-- COMMAND ----------

select *
from dev_derived_reco.joined_recommendation_set

-- COMMAND ----------

select *
from dev_derived_reco.output_recommendations_acm
where countryCode is null

-- COMMAND ----------

select countryCode, count(distinct opr_lnkd_integration_id)
from dev_derived_reco.output_recommendations_acm
group by countryCode
-- DK, FI, LK, SE

-- COMMAND ----------

select countryCode, count(*) as count
from dev_derived_reco.operator_ids
where trackingId is not null
group by countryCode
order by count desc

-- COMMAND ----------

select *
from dev_derived_reco.output_recommendations_ufs_com
where countryCode = 'FR'

-- COMMAND ----------

select *
from dev_derived_reco.output_recommendations_ufs_com
where countryCode = 'FR'
and trackingId = '09564aab-e460-2b35-08af-ba71f4755ab1'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import functions as f
-- MAGIC df = spark.read.format("delta").option("timestampAsOf", "2020-08-06 11:58:45.0").load('/mnt/datamodel/dev/derived/reco/output/recommendations_ufs_com')
-- MAGIC display(df.where(f.col('countryCode') == 'FR').where(f.col('trackingId') == '09564aab-e460-2b35-08af-ba71f4755ab1'))

-- COMMAND ----------

select countryCode, recommendationType, count(distinct trackingId)
from dev_derived_reco.output_recommendations_ufs_com
group by countryCode, recommendationType

-- COMMAND ----------

select *
from dev_derived_reco.output_recommendations_marketo

-- COMMAND ----------

select countryCode, recommendationCodeType, count(distinct operatorOhubId)
from dev_derived_reco.output_recommendations_marketo
group by countryCode, recommendationCodeType

-- COMMAND ----------

select *
from dev_derived_reco.output_recommendations_oview

-- COMMAND ----------

select countryCode, recommendation_type, count(distinct opr_lnkd_integration_id)
from dev_derived_reco.output_recommendations_oview
group by countryCode, recommendation_type
