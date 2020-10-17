# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC Drop Table if exists data_reco_input.ods_reco_operators_reachable;
# MAGIC Create Table data_reco_input.ods_reco_operators_reachable
# MAGIC as
# MAGIC Select distinct countryCode,
# MAGIC        dateCreated,
# MAGIC        dateUpdated,
# MAGIC        isActive,
# MAGIC        isGoldenRecord,
# MAGIC        concatId,
# MAGIC        ohubId,
# MAGIC        channel,
# MAGIC        LOCAL_CHANNEL as LOCAL_CHANNEL,
# MAGIC        CHANNEL_USAGE,
# MAGIC        CHANNEL_REFERENCE_FK
# MAGIC From
# MAGIC (
# MAGIC Select  ops.countryCode,
# MAGIC         ops.dateCreated,
# MAGIC         ops.dateUpdated,
# MAGIC         ops.isActive,
# MAGIC         ops.isGoldenRecord,
# MAGIC         ops.concatId,
# MAGIC         ops.ohubId,
# MAGIC         ops.channel,
# MAGIC         (Case When chn.LOCAL_CHANNEL_RECO is not Null and length(chn.LOCAL_CHANNEL_RECO) > 4 Then initcap(chn.LOCAL_CHANNEL_RECO)
# MAGIC               When chn.LOCAL_CHANNEL_RECO is not Null and length(chn.LOCAL_CHANNEL_RECO) <= 4 Then chn.LOCAL_CHANNEL_RECO
# MAGIC         Else 'Other' END) as LOCAL_CHANNEL,
# MAGIC         chn.CHANNEL_USAGE,
# MAGIC         chn.CHANNEL_REFERENCE_FK,
# MAGIC         dense_rank() over(partition by ops.countryCode, ops.ohubId order by ops.dateCreated asc, ops.dateUpdated desc, ops.concatId desc,  initcap(LOCAL_CHANNEL_RECO) desc, chn.CHANNEL_USAGE asc, chn.CHANNEL_REFERENCE_FK desc) as rnk_ops
# MAGIC From data_datascience_prod.operators ops
# MAGIC         left outer join
# MAGIC      data_config_all.channel_mapping chn on lower(ops.countryCode) = lower(chn.country_code) and lower(ops.channel) = lower(chn.ORIGINAL_CHANNEL)
# MAGIC Where 1=1
# MAGIC And   upper(isGoldenRecord) = 'TRUE'
# MAGIC --And   ops.concatId in
# MAGIC --(
# MAGIC --Select op_concatId
# MAGIC --From data_fpo.reachable_fpo_20190321 
# MAGIC --Where 1=1
# MAGIC --And upper(reachableEmail) = 'Y'
# MAGIC --)
# MAGIC )
# MAGIC Where 1=1
# MAGIC And rnk_ops = 1
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select * from data_reco_input.datascience_sales_recent_ops

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select *
# MAGIC From data_fpo.reachable_fpo_20190321 
# MAGIC Where 1=1
# MAGIC And upper(reachableEmail) = 'Y'
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select * From data_reco_input.datascience_sales_recent_ops

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select count(*), count(distinct operatorOhubId)
# MAGIC From data_reco_input.datascience_sales_recent_ops
# MAGIC Where countryCode = 'ES'
# MAGIC --And operatorOhubId in (
# MAGIC --Select op_ohubid
# MAGIC --From data_fpo.reachable_fpo_20190321 
# MAGIC --Where 1=1
# MAGIC --And upper(reachableEmail) = 'Y'
# MAGIC --)
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select count(distinct op_ohubid)
# MAGIC From data_fpo.reachable_fpo_20190321 
# MAGIC Where 1=1
# MAGIC And upper(reachableEmail) = 'Y'
# MAGIC And op_countryCode = 'ES'
# MAGIC And op_ohubid in
# MAGIC (
# MAGIC Select operatorOhubId
# MAGIC From data_reco_input.datascience_sales_recent_ops
# MAGIC )
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select  count(*),
# MAGIC         count(distinct countryCode || ' / ' || ohubid)
# MAGIC From data_reco_input.ods_reco_operators_reachable
# MAGIC Where 1=1
# MAGIC And countryCode = 'US' 
# MAGIC ;
