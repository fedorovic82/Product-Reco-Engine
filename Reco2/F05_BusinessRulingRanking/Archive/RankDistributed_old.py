# Databricks notebook source
var_country_code = getArgument("runCountryCode")
#var_country_code = 'AT'

# COMMAND ----------

reco_date = getArgument("recoDate")
#reco_date = '20190602'

# COMMAND ----------

sifu_product_details = spark.table('data_sifu.sifu_product_details')
sifu_recipe_details = spark.table('data_sifu.sifu_recipe_details')

# COMMAND ----------

recommendation_set = spark.table('data_reco_input.'+str(var_country_code)+'_recommendation_set')
ods_reco_contacts_IDS = spark.table('data_reco_input.ods_reco_contacts_ids')
available_product_images_acm = spark.table('data_config_all.available_product_images_acm')

# COMMAND ----------

from pyspark.sql.functions import *

ods_reco_contacts_IDS_select = ods_reco_contacts_IDS.where(ods_reco_contacts_IDS.operatorOhubId.isNull())

ods_reco_contacts_IDS_select = ods_reco_contacts_IDS.select('countryCode','integrated_id','trackingid').distinct()

recommendation_set_select = recommendation_set

recommendation_set_select = recommendation_set_select.withColumn('operatorOhubId', lit(None).cast('string'))
recommendation_set_select = recommendation_set_select.withColumn('Local_channel', lit(None).cast('string'))
recommendation_set_select = recommendation_set_select.withColumn('recoids', lit(None).cast('string'))
recommendation_set_select = recommendation_set_select.withColumn('contactpersonOhubId', lit(None).cast('string'))
recommendation_set_select = recommendation_set_select.withColumn('emailAddress', lit(None).cast('string'))
recommendation_set_select = recommendation_set_select.withColumn('emailId', lit(None).cast('string'))
recommendation_set_select = recommendation_set_select.withColumn('userid', lit(None).cast('string'))

recommendation_set_select = recommendation_set_select.select('operatorOhubId', 'Local_channel', 'recoids', 'productCode', 'cuEanCodes', 'productCodeLocal', 'recipeId', 'contactpersonOhubId', \
                                                                 'emailAddress', 'emailId', 'userid').distinct()

ods_reco_contacts_IDS_select = ods_reco_contacts_IDS_select.where(ods_reco_contacts_IDS_select.countryCode == var_country_code)


tmp0a_ods_recommendations = recommendation_set_select.crossJoin(ods_reco_contacts_IDS_select)

tmp0a_ods_recommendations = tmp0a_ods_recommendations.select('integrated_id', 'operatorOhubId', 'Local_channel', 'recoids', 'productCode', 'cuEanCodes', 'productCodeLocal', 'recipeId', 'contactpersonOhubId', \
                                                                 'emailAddress', 'trackingid', 'emailId', 'userid')

tmp0a_ods_recommendations = tmp0a_ods_recommendations.distinct()

tmp0a_ods_recommendations.show()

#tmp0_ods_recommendations.write.mode("Overwrite").saveAsTable('data_reco_output.tmp0a_ods_'+str(var_country_code).lower()+'_recommendations_check1')


# COMMAND ----------

#tmp0a_ods_recommendations.withColumn('integrated_id',when(tmp0a_ods_recommendations.integrated_id.isNotNull(), tmp0a_ods_recommendations.integrated_id).otherwise(' ')).withColumn('cuEanCodes',when(tmp0a_ods_recommendations.cuEanCodes.isNotNull(), tmp0a_ods_recommendations.cuEanCodes).otherwise(' ')).withColumn('recipeId',when(tmp0a_ods_recommendations.recipeId.isNotNull(), tmp0a_ods_recommendations.recipeId).otherwise(' ')).agg(countDistinct(concat(concat(col('operatorOhubId'),lit(' / '),col('cuEanCodes')),lit(' / '),col('recipeId')))).show()
#tmp0a_ods_recommendations.count()

# COMMAND ----------

ods_features_product_whitespots_ga = spark.table('data_reco_feature_tables.ods_features_pageviews')

ods_features_product_whitespots_ga = ods_features_product_whitespots_ga.where(ods_features_product_whitespots_ga.country_code == var_country_code)

ods_features_product_whitespots_ga = ods_features_product_whitespots_ga.where("lower(feature) like '%dep_dum_ppv%'")

ods_features_product_whitespots_ga = ods_features_product_whitespots_ga.select(col('country_code'),
                                              col('trackingid'),
                                              col('feature').alias('productCode'),
                                              col('value').alias('viewed'),
                                              col('value').alias('digital_whitespot')
                                              )

ods_features_product_whitespots_ga = ods_features_product_whitespots_ga.withColumn('productCode', regexp_replace('productCode', 'dep_dum_ppv_', ''))
ods_features_product_whitespots_ga = ods_features_product_whitespots_ga.withColumn('digital_whitespot', 1-col('digital_whitespot'))

ods_features_product_whitespots_ga.show()


# COMMAND ----------

ods_features_product_whitespots_sales = spark.table('data_reco_input.datascience_sales_max_corr')

ods_features_product_whitespots_sales = ods_features_product_whitespots_sales.where(col('countryCode') == var_country_code)

ods_features_product_whitespots_sales = ods_features_product_whitespots_sales.where(date_format(col('transactionDate'),'yyyyMM') > col('max_yearmonth_min6'))

ods_features_product_whitespots_sales = ods_features_product_whitespots_sales.select(ods_features_product_whitespots_sales.countryCode,
                                              ods_features_product_whitespots_sales.operatorOhubId,
                                              ods_features_product_whitespots_sales.cuEanCode.alias('cuEanCodes'),
                                              lit(1).alias('ordered'),
                                              lit(0).alias('whitespot')
                                              )
ods_features_product_whitespots_sales.show()


# COMMAND ----------

from pyspark.sql.functions import *

recommendation_set = recommendation_set.select('operatorOhubId', 'Local_channel', 'recoids', 'productCode', 'cuEanCodes', 'productCodeLocal', 'recipeId').distinct()

ods_reco_contacts_IDS = ods_reco_contacts_IDS.select('countryCode', 'operatorOhubId', 'integrated_id', 'contactpersonOhubId', 'emailAddress', 'trackingid', 'emailId', 'userid').distinct()
ods_reco_contacts_IDS = ods_reco_contacts_IDS.where(ods_reco_contacts_IDS.countryCode == var_country_code)

tmp0b_ods_recommendations = recommendation_set.join(ods_reco_contacts_IDS, ['operatorOhubId'], 'inner')

tmp0a_ods_recommendations = tmp0a_ods_recommendations.select('integrated_id', 'operatorOhubId', 'Local_channel', 'recoids', 'productCode', 'cuEanCodes', 'productCodeLocal', 'recipeId', 'contactpersonOhubId', 'emailAddress', 'trackingid', 'emailId', 'userid')
tmp0b_ods_recommendations = tmp0b_ods_recommendations.select('integrated_id', 'operatorOhubId', 'Local_channel', 'recoids', 'productCode', 'cuEanCodes', 'productCodeLocal', 'recipeId', 'contactpersonOhubId', 'emailAddress', 'trackingid', 'emailId', 'userid')

tmp0b_ods_recommendations = tmp0a_ods_recommendations.unionAll(tmp0b_ods_recommendations)

available_product_images_acm = available_product_images_acm.where(available_product_images_acm.countryCode == var_country_code)
available_product_images_acm = available_product_images_acm.where(available_product_images_acm.cuEanCodes.isNotNull())

lst_available_images = available_product_images_acm.select('cuEanCodes').distinct().rdd.map(lambda r: r[0]).collect()

tmp0b_ods_recommendations = tmp0b_ods_recommendations.where(col("cuEanCodes").isin(lst_available_images))

tmp0b_ods_recommendations = tmp0b_ods_recommendations.distinct()

tmp0b_ods_recommendations.show()

#tmp0b_ods_at_recommendations.write.mode("Overwrite").saveAsTable('data_reco_output.tmp0b_ods_'+str(var_country_code)+'_recommendations_check1')


# COMMAND ----------

#tmp0b_ods_recommendations.withColumn('integrated_id',when(tmp0b_ods_recommendations.integrated_id.isNotNull(), tmp0b_ods_recommendations.integrated_id).otherwise(' ')).withColumn('cuEanCodes',when(tmp0b_ods_recommendations.cuEanCodes.isNotNull(), tmp0b_ods_recommendations.cuEanCodes).otherwise(' ')).withColumn('recipeId',when(tmp0b_ods_recommendations.recipeId.isNotNull(), tmp0b_ods_recommendations.recipeId).otherwise(' ')).agg(countDistinct(concat(concat(col('operatorOhubId'),lit(' / '),col('cuEanCodes')),lit(' / '),col('recipeId')))).show()
#tmp0b_ods_recommendations.count()

# COMMAND ----------

from pyspark.sql.functions import *

tmp0b_ods_recommendations = tmp0b_ods_recommendations.select('integrated_id', 'operatorOhubId', 'Local_channel', 'recoids', 'productCode', 'cuEanCodes', 'productCodeLocal', 'recipeId', 'contactpersonOhubId', 'emailAddress', 'trackingid', 'emailId', 'userid')
ods_features_product_whitespots_sales = ods_features_product_whitespots_sales.where(col('countryCode') == var_country_code).select('operatorOhubId', 'cuEanCodes', 'ordered', 'whitespot').distinct()
tmp1_ods_recommendations = tmp0b_ods_recommendations.join(ods_features_product_whitespots_sales, ['operatorOhubId', 'cuEanCodes'], 'left')
tmp1_ods_recommendations = tmp1_ods_recommendations.withColumn('ordered', when(tmp1_ods_recommendations.ordered.isNotNull(), tmp1_ods_recommendations.ordered).otherwise(0))
tmp1_ods_recommendations = tmp1_ods_recommendations.withColumn('whitespot',when(tmp1_ods_recommendations.whitespot.isNotNull(), tmp1_ods_recommendations.whitespot).otherwise(1))

ods_features_product_whitespots_ga = ods_features_product_whitespots_ga.where(col('country_code') == var_country_code).select('trackingid', 'productCode', 'viewed', 'digital_whitespot').distinct()
tmp1_ods_recommendations = tmp1_ods_recommendations.join(ods_features_product_whitespots_ga, ['trackingid', 'productCode'], 'left')
tmp1_ods_recommendations = tmp1_ods_recommendations.withColumn('digital_whitespot',when(tmp1_ods_recommendations.digital_whitespot.isNotNull(),tmp1_ods_recommendations.digital_whitespot).otherwise(1))

tmp1_ods_recommendations = tmp1_ods_recommendations.select('integrated_id', 'operatorOhubId', 'Local_channel', 'recoids', 'productCode', 'cuEanCodes', 'productCodeLocal', 'recipeId', 'contactpersonOhubId', 'emailAddress', 'trackingid', 'emailId', 'userid', 'viewed', 'ordered', 'whitespot', 'digital_whitespot')
tmp1_ods_recommendations = tmp1_ods_recommendations.distinct()

#tmp1_ods_at_recommendations.write.mode("Overwrite").saveAsTable('data_reco_output.tmp1_ods_'+str(var_country_code)+'_recommendations_check')

# ods_features_product_whitespots_ga 
# ods_features_product_whitespots_sales

# COMMAND ----------

#recommendation_set.select('operatorOhubId').distinct().count()

# COMMAND ----------

#ods_reco_contacts_IDS.select('operatorOhubId').distinct().count()

# COMMAND ----------

#tmp0b_ods_recommendations.select('operatorOhubId').distinct().count()

# COMMAND ----------

#tmp1_ods_recommendations.select('operatorOhubId').distinct().count()

# COMMAND ----------


#tmp1_ods_recommendations.select('emailAddress').distinct().count()

# COMMAND ----------

from pyspark.sql.functions import *

if var_country_code not in ['CH']:
  sifu_product_details = sifu_product_details.where(col('countryCode') == var_country_code)
  sifu_product_details = sifu_product_details.select('productCode', 'language', 'productName','url')
  sifu_product_details = sifu_product_details.withColumnRenamed('url', 'productUrl')

if var_country_code in ['CH']:
  sifu_product_details = sifu_product_details.where(col('countryCode') == var_country_code)
  sifu_product_details = sifu_product_details.select('cuEanCodes', 'language', 'productName','url')
  sifu_product_details = sifu_product_details.withColumnRenamed('url', 'productUrl')

sifu_recipe_details = sifu_recipe_details.where(col('countryCode') == var_country_code)
sifu_recipe_details = sifu_recipe_details.withColumnRenamed('url', 'recipeUrl')

tmp1_ods_recommendations = tmp1_ods_recommendations.where(tmp1_ods_recommendations.productCodeLocal.isNotNull())

scores_logit_ga = spark.table('data_reco_output.'+var_country_code.lower()+'_scores_logit_ga_'+str(reco_date))
scores_logit_sales = spark.table('data_reco_output.'+var_country_code.lower()+'_scores_logit_sales_'+str(reco_date))

scores_logit_ga = scores_logit_ga.withColumn('product_code', regexp_replace(col('product_code'),'_', '-'))
scores_logit_ga = scores_logit_ga.withColumnRenamed('product_code', 'productCode')
scores_logit_ga = scores_logit_ga.withColumnRenamed('value', 'affinity')
scores_logit_ga = scores_logit_ga.select('trackingid','productCode','affinity') 

scores_logit_sales = scores_logit_sales.withColumnRenamed('product_code', 'cuEanCodes')
scores_logit_sales = scores_logit_sales.withColumnRenamed('value', 'propensity')
scores_logit_sales = scores_logit_sales.select('operatorOhubId','cuEanCodes','propensity')

if var_country_code in ['AT', 'CH', 'DE']:
  scores_logit_ga = scores_logit_ga.withColumnRenamed('productCode', 'cuEanCodes')
  tmp2_ods_recommendations = tmp1_ods_recommendations.join(scores_logit_ga, ['trackingid', 'cuEanCodes'], 'left')

if var_country_code not in ['AT', 'CH', 'DE']:
  tmp2_ods_recommendations = tmp1_ods_recommendations.join(scores_logit_ga, ['trackingid', 'productCode'], 'left')
    
tmp2_ods_recommendations = tmp2_ods_recommendations.join(scores_logit_sales, ['operatorOhubId', 'cuEanCodes'], 'left')

if var_country_code not in ['CH']:
  tmp2_ods_recommendations = tmp2_ods_recommendations.join(sifu_product_details, ['productCode'], 'inner')

if var_country_code in ['CH']:
  tmp2_ods_recommendations = tmp2_ods_recommendations.join(sifu_product_details, ['cuEanCodes'], 'inner')

tmp2_ods_recommendations = tmp2_ods_recommendations.withColumn('step1_integrated_reco_id',when((col('emailAddress').isNotNull() & (col('emailAddress') != '')),tmp2_ods_recommendations.emailAddress).otherwise(tmp2_ods_recommendations.operatorOhubId))
tmp2_ods_recommendations = tmp2_ods_recommendations.withColumn('integrated_reco_id',when(tmp2_ods_recommendations.operatorOhubId.isNotNull(),tmp2_ods_recommendations.step1_integrated_reco_id).otherwise(tmp2_ods_recommendations.trackingid))

tmp2_ods_recommendations = tmp2_ods_recommendations.withColumn('affinity',when(tmp2_ods_recommendations.affinity.isNotNull(),tmp2_ods_recommendations.affinity).otherwise(0))
tmp2_ods_recommendations = tmp2_ods_recommendations.withColumn('propensity',when(tmp2_ods_recommendations.propensity.isNotNull(),tmp2_ods_recommendations.propensity).otherwise(0))
tmp2_ods_recommendations = tmp2_ods_recommendations.withColumn('personalized',when(tmp2_ods_recommendations.trackingid.isNotNull(),1).otherwise(0))

tmp2_ods_recommendations = tmp2_ods_recommendations.withColumn('random_recipe_affinity',rand())
tmp2_ods_recommendations = tmp2_ods_recommendations.withColumn('random_product_affinity',rand())

tmp2_ods_recommendations = tmp2_ods_recommendations.join(sifu_recipe_details, ['recipeId', 'language'], 'inner')

tmp2_ods_recommendations = tmp2_ods_recommendations.select('operatorOhubId', 'contactpersonOhubId', 'emailAddress', 'integrated_reco_id', 'local_channel', 'productCode', 'language', 'productName', \
                                                                 'productUrl', 'recipeName', 'recipeUrl', 'cuEanCodes', 'productCodeLocal', 'recipeId', 'userid', 'trackingid', 'emailid', 'personalized', \
                                                                 'affinity', 'propensity', 'random_recipe_affinity', 'random_product_affinity', 'whitespot', 'digital_whitespot')

tmp2_ods_recommendations = tmp2_ods_recommendations.withColumn('whitespot', when(tmp2_ods_recommendations.whitespot.isNotNull(), tmp2_ods_recommendations.whitespot).otherwise(1))
tmp2_ods_recommendations = tmp2_ods_recommendations.withColumn('digital_whitespot', when(tmp2_ods_recommendations.digital_whitespot.isNotNull(), tmp2_ods_recommendations.digital_whitespot).otherwise(1))


# COMMAND ----------

#tmp2_ods_recommendations.count()

# COMMAND ----------

#tmp2_ods_recommendations.select('operatorOhubId').distinct().count()

# COMMAND ----------

#tmp2_ods_recommendations.select('emailAddress').distinct().count()

# COMMAND ----------

from pyspark.sql.window import *
from pyspark.sql.functions import *


w1 =  Window.partitionBy(tmp2_ods_recommendations.integrated_reco_id,tmp2_ods_recommendations.language,tmp2_ods_recommendations.productCode).orderBy(tmp2_ods_recommendations.propensity.desc(), tmp2_ods_recommendations.affinity.desc(),tmp2_ods_recommendations.random_recipe_affinity.desc())


tmp2_ods_recommendations = tmp2_ods_recommendations.withColumn('rcp_rnk', dense_rank().over(w1))
tmp2_ods_recommendations = tmp2_ods_recommendations.where(tmp2_ods_recommendations.rcp_rnk == 1)
tmp2_ods_recommendations = tmp2_ods_recommendations.where(tmp2_ods_recommendations.whitespot == 1)

tmp2_ods_recommendations = tmp2_ods_recommendations.select('operatorOhubId', 'contactpersonOhubId', 'emailAddress', 'integrated_reco_id', 'local_channel', 'productCode', 'language', 'productName', \
                                                                 'productUrl', 'recipeName', 'recipeUrl', 'cuEanCodes', 'productCodeLocal', 'recipeId', 'userid', 'trackingid', 'emailid', 'personalized', \
                                                                 'affinity', 'propensity', 'random_recipe_affinity', 'random_product_affinity', 'whitespot', 'digital_whitespot')


w2 =  Window.partitionBy(tmp2_ods_recommendations.integrated_reco_id, tmp2_ods_recommendations.language).orderBy(tmp2_ods_recommendations.propensity.desc(), tmp2_ods_recommendations.affinity.desc(), tmp2_ods_recommendations.productCode.desc(),tmp2_ods_recommendations.random_recipe_affinity.desc())

tmp2_ods_recommendations = tmp2_ods_recommendations.withColumn('recommendation_id', dense_rank().over(w2))


# COMMAND ----------

#tmp2_ods_recommendations.count()

# COMMAND ----------

tmp3_ods_recommendations = tmp2_ods_recommendations
tmp3_ods_recommendations = tmp3_ods_recommendations.withColumn('step1_integrated_reco_id_acm', when(tmp3_ods_recommendations.trackingid.isNotNull(), tmp3_ods_recommendations.trackingid).otherwise(''))

tmp3_ods_recommendations = tmp3_ods_recommendations.withColumn('integrated_reco_id_acm', when(tmp3_ods_recommendations.operatorOhubId.isNotNull(), tmp2_ods_recommendations.operatorOhubId).otherwise(tmp3_ods_recommendations.step1_integrated_reco_id_acm))

tmp3_ods_recommendations = tmp3_ods_recommendations.withColumn('integrated_reco_id_acm', when(tmp3_ods_recommendations.integrated_reco_id_acm.isNotNull(), tmp3_ods_recommendations.integrated_reco_id_acm).otherwise(''))

tmp3_ods_recommendations = tmp3_ods_recommendations.withColumn('step1_integrated_reco_id_ufs_com', when(tmp3_ods_recommendations.emailAddress.isNotNull(), tmp3_ods_recommendations.emailAddress).otherwise(tmp3_ods_recommendations.operatorOhubId))
tmp3_ods_recommendations = tmp3_ods_recommendations.withColumn('step1_integrated_reco_id_ufs_com', when(tmp3_ods_recommendations.step1_integrated_reco_id_ufs_com.isNotNull(), tmp3_ods_recommendations.step1_integrated_reco_id_ufs_com).otherwise(''))

tmp3_ods_recommendations = tmp3_ods_recommendations.withColumn('step2_integrated_reco_id_ufs_com', when(tmp3_ods_recommendations.trackingid.isNotNull(), tmp3_ods_recommendations.trackingid).otherwise(''))
tmp3_ods_recommendations = tmp3_ods_recommendations.withColumn('integrated_reco_id_ufs_com', when(tmp3_ods_recommendations.operatorOhubId.isNotNull(), concat(concat(tmp3_ods_recommendations.step1_integrated_reco_id_ufs_com, lit(' / ')),tmp3_ods_recommendations.step2_integrated_reco_id_ufs_com)).otherwise(tmp3_ods_recommendations.trackingid))

w1 =  Window.partitionBy(tmp3_ods_recommendations.integrated_reco_id_acm, tmp3_ods_recommendations.language, tmp3_ods_recommendations.productCode).orderBy(tmp3_ods_recommendations.affinity.desc(), tmp3_ods_recommendations.random_recipe_affinity.desc(), tmp3_ods_recommendations.random_product_affinity.desc())
tmp3_ods_recommendations = tmp3_ods_recommendations.withColumn('rnk_rec_opr', dense_rank().over(w1))

tmp3_ods_recommendations = tmp3_ods_recommendations.where(tmp3_ods_recommendations.rnk_rec_opr == 1)

w2 =  Window.partitionBy(tmp3_ods_recommendations.integrated_reco_id_acm,tmp3_ods_recommendations.language).orderBy(tmp3_ods_recommendations.propensity.desc(), tmp3_ods_recommendations.affinity.desc(), tmp3_ods_recommendations.random_product_affinity.desc(), tmp3_ods_recommendations.productCode.desc())
w3 =  Window.partitionBy(tmp3_ods_recommendations.integrated_reco_id_ufs_com,tmp3_ods_recommendations.language).orderBy(tmp3_ods_recommendations.propensity.desc(), tmp3_ods_recommendations.affinity.desc(), tmp3_ods_recommendations.random_product_affinity.desc(), tmp3_ods_recommendations.productCode.desc())

tmp3_ods_recommendations = tmp3_ods_recommendations.withColumn('recommendation_id_acm', dense_rank().over(w2))
tmp3_ods_recommendations = tmp3_ods_recommendations.withColumn('recommendation_id_ufs_com', dense_rank().over(w3))

tmp3_ods_recommendations = tmp3_ods_recommendations.select('integrated_reco_id_acm', 'integrated_reco_id_ufs_com', 'operatorOhubId', 'contactpersonOhubId', 'language', 'emailAddress', 'local_channel', 'productCode',  \
'cuEanCodes', 'productCodeLocal', 'recipeId', 'productName', 'productUrl', 'recipeName', 'recipeUrl', 'userid', 'trackingid', 'emailid', 'personalized', 'affinity', 'propensity', 'random_recipe_affinity', 'random_product_affinity', 'whitespot', 'digital_whitespot','recommendation_id_acm', 'recommendation_id_ufs_com')

#tmp3_ods_recommendations.write.mode("Overwrite").saveAsTable('data_reco_output.tmp3_ods_at_recommendations_check')


# COMMAND ----------

#tmp3_ods_recommendations.select('operatorOhubId').distinct().count()

# COMMAND ----------

#tmp3_ods_recommendations.select('emailAddress').distinct().count()

# COMMAND ----------

from pyspark.sql.functions import *

tmp3_ods_recommendations_complete_ops_prd_acm = tmp3_ods_recommendations
tmp3_ods_recommendations_complete_ops_prd_acm = tmp3_ods_recommendations_complete_ops_prd_acm.where(col('recommendation_id_acm') <= 3)
tmp3_ods_recommendations_complete_ops_prd_acm = tmp3_ods_recommendations_complete_ops_prd_acm.groupBy("integrated_reco_id_acm", "language").agg(countDistinct("cuEanCodes").alias('cnt_products')).where(col('cnt_products') == 3)
tmp3_ods_recommendations_complete_ops_prd_acm = tmp3_ods_recommendations_complete_ops_prd_acm.select('integrated_reco_id_acm', 'language').distinct()

# COMMAND ----------

ods_recommendations = tmp3_ods_recommendations.join(tmp3_ods_recommendations_complete_ops_prd_acm, ['integrated_reco_id_acm', 'language'], 'inner')

# COMMAND ----------

from pyspark.sql.functions import *

tmp3_ods_recommendations_complete_ops_rcp_acm = ods_recommendations
tmp3_ods_recommendations_complete_ops_rcp_acm = tmp3_ods_recommendations_complete_ops_rcp_acm.where(col('recommendation_id_acm') <= 3)
tmp3_ods_recommendations_complete_ops_rcp_acm = tmp3_ods_recommendations_complete_ops_rcp_acm.groupBy("integrated_reco_id_acm", "language").agg(countDistinct("recipeId").alias('cnt_recipes')).where(col('cnt_recipes') == 3)
tmp3_ods_recommendations_complete_ops_rcp_acm = tmp3_ods_recommendations_complete_ops_rcp_acm.select('integrated_reco_id_acm', 'language').distinct()

# COMMAND ----------

ods_recommendations = ods_recommendations.join(tmp3_ods_recommendations_complete_ops_rcp_acm, ['integrated_reco_id_acm', 'language'], 'inner')

# COMMAND ----------

ods_recommendations = ods_recommendations.distinct()

# COMMAND ----------

ods_recommendations.write.mode("Overwrite").saveAsTable('data_reco_output.ods_'+str(var_country_code).lower()+'_recommendations_'+str(reco_date))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select *
# MAGIC From
# MAGIC (
# MAGIC Select  cast('Operators' as string) as entity,
# MAGIC         countryCode,
# MAGIC         count(*), 
# MAGIC         count(distinct ohubId) as cnt_ops,
# MAGIC         cast(Null as integer) as cnt_email
# MAGIC From data_reco_input.ods_reco_operators
# MAGIC Group by countryCode
# MAGIC UNION ALL
# MAGIC Select  cast('Operators with contactpersons' as string) as entity,
# MAGIC         countryCode,
# MAGIC         count(*), 
# MAGIC         count(distinct operatorOhubId) as cnt_ops,
# MAGIC         cast(Null as integer) as cnt_email
# MAGIC From data_reco_input.ods_reco_operators_withcontact
# MAGIC Group by countryCode
# MAGIC UNION ALL
# MAGIC Select  cast('Contactpersons' as string) as entity,
# MAGIC         countryCode,
# MAGIC         count(*), 
# MAGIC         count(distinct operatorOhubId) as cnt_ops,
# MAGIC         cast(Null as integer) as cnt_email
# MAGIC From data_reco_input.ods_reco_contacts
# MAGIC Group by countryCode
# MAGIC UNION ALL
# MAGIC Select  cast('IDS' as string) as entity,
# MAGIC         countryCode,
# MAGIC         count(*), 
# MAGIC         count(distinct operatorOhubId) as cnt_ops,
# MAGIC         count(distinct emailAddress) as cnt_email
# MAGIC From data_reco_input.ods_reco_contacts_IDS
# MAGIC Group by countryCode
# MAGIC )
# MAGIC Where countryCode = 'CH'
# MAGIC UNION ALL
# MAGIC Select  cast('Reco sets' as string) as entity,
# MAGIC         cast('CH' as string) as countryCode,
# MAGIC         count(*), 
# MAGIC         count(distinct operatorOhubId) as cnt_ops,
# MAGIC         cast(Null as integer) as cnt_email
# MAGIC From data_reco_input.ch_recommendation_set
# MAGIC UNION ALL
# MAGIC Select  cast('Reco sets' as string) as entity,
# MAGIC         cast('CH' as string) as countryCode,
# MAGIC         count(*), 
# MAGIC         count(distinct operatorOhubId) as cnt_ops,
# MAGIC         cast(Null as integer) as cnt_email
# MAGIC From data_reco_input.ch_recommendation_set
# MAGIC Where productCode <> ''
# MAGIC UNION ALL
# MAGIC Select  cast('Recos' as string) as entity,
# MAGIC         cast('CH' as string) as countryCode,
# MAGIC         count(*), 
# MAGIC         count(distinct operatorOhubId) as cnt_ops,
# MAGIC         count(distinct emailAddress) as cnt_email
# MAGIC From data_reco_output.ods_ch_recommendations_20190602
# MAGIC ;
