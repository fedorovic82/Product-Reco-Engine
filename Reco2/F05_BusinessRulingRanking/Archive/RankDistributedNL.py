# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC Drop table if exists data_reco_feature_tables.ods_features_product_whitespots_ga;
# MAGIC Create table data_reco_feature_tables.ods_features_product_whitespots_ga
# MAGIC as
# MAGIC Select country_code, trackingid, regexp_replace(feature, 'dep_dum_ppv_', '') as productCode, value as viewed, 1-value as whitespot
# MAGIC From data_reco_feature_tables.ods_features_pageviews
# MAGIC Where 1=1
# MAGIC and lower(feature) like '%dep_dum_ppv%' 
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select *
# MAGIC From data_reco_output.at_model_logit_sales
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select * From data_reco_input.nl_recommendation_set 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC refresh table data_reco_input.ods_reco_contacts_IDS;
# MAGIC 
# MAGIC Drop table if exists data_reco_output.tmp0_ods_nl_recommendations;
# MAGIC Create table data_reco_output.tmp0_ods_nl_recommendations
# MAGIC as
# MAGIC Select distinct 
# MAGIC ids.integrated_id,
# MAGIC cast(Null as string) as operatorOhubId, 
# MAGIC cast(Null as string) as Local_channel, 
# MAGIC cast(Null as string) as recoids, 
# MAGIC productCode, 
# MAGIC cuEanCodes, 
# MAGIC productCodeLocal, 
# MAGIC recipeId,
# MAGIC cast(Null as string) as contactpersonOhubId, 
# MAGIC cast(Null as string) as emailAddress, 
# MAGIC ids.trackingid, 
# MAGIC cast(Null as string) as emailId,
# MAGIC cast(Null as string) as userid
# MAGIC From data_reco_input.nl_recommendation_set rcs
# MAGIC             cross join
# MAGIC      (Select * from data_reco_input.ods_reco_contacts_IDS where operatorOhubId is Null) ids
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC refresh table data_reco_input.ods_reco_contacts_IDS;
# MAGIC 
# MAGIC Drop table if exists data_reco_output.tmp1_ods_nl_recommendations;
# MAGIC Create table data_reco_output.tmp1_ods_nl_recommendations
# MAGIC as
# MAGIC Select distinct t.*, viewed, whitespot
# MAGIC From
# MAGIC (Select ids.integrated_id, rcs.operatorOhubId, rcs.Local_channel, rcs.recoids, rcs.productCode, rcs.cuEanCodes, rcs.productCodeLocal, rcs.recipeId, ids.contactpersonOhubId, ids.emailAddress, ids.trackingid, ids.emailId, ids.userid
# MAGIC From data_reco_input.nl_recommendation_set rcs
# MAGIC        inner join 
# MAGIC      data_reco_input.ods_reco_contacts_IDS ids on rcs.operatorOhubId = ids.operatorOhubId
# MAGIC         Union ALL
# MAGIC Select * 
# MAGIC From data_reco_output.tmp0_ods_nl_recommendations
# MAGIC ) t
# MAGIC   left join
# MAGIC data_reco_feature_tables.ods_features_product_whitespots_ga ws on t.trackingid = ws.trackingid and t.productCode = ws.productCode
# MAGIC Where 1=1
# MAGIC And t.cuEanCodes in (Select cuEanCodes From data_config_all.available_product_images_acm Where upper(countryCode) = 'NL' and cuEanCodes is not Null)
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select  count(*), 
# MAGIC         count(distinct integrated_id  || ' / ' || productCode || ' / ' || (Case When recipeId is not Null Then recipeId else '' END)), 
# MAGIC         count(distinct productCode),
# MAGIC         count(distinct operatorOhubId),
# MAGIC         count(distinct emailAddress)
# MAGIC From data_reco_output.tmp1_ods_nl_recommendations
# MAGIC Where 1=1
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select *
# MAGIC From data_reco_output.tmp1_ods_nl_recommendations
# MAGIC Where 1=1
# MAGIC And operatorOhubId is Null
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Drop table if exists data_reco_output.tmp2_ods_nl_recommendations;
# MAGIC Create table data_reco_output.tmp2_ods_nl_recommendations
# MAGIC as
# MAGIC Select distinct r.*, 
# MAGIC        dense_rank() over(partition by r.integrated_reco_id order by affinity desc, r.productCode, random_product_affinity) as recommendation_id
# MAGIC From
# MAGIC (
# MAGIC Select t.operatorOhubId, 
# MAGIC        t.contactpersonOhubId,
# MAGIC        t.emailAddress,
# MAGIC        t.integrated_reco_id,
# MAGIC        t.local_channel, 
# MAGIC        t.productCode, 
# MAGIC        t.language,
# MAGIC        t.productName,
# MAGIC        t.productUrl,
# MAGIC        t.recipeName,
# MAGIC        t.recipeUrl,       
# MAGIC        t.cuEanCodes, 
# MAGIC        t.productCodeLocal, 
# MAGIC        t.recipeId, 
# MAGIC        t.userid, 
# MAGIC        t.trackingid, 
# MAGIC        t.emailid,
# MAGIC        t.personalized,
# MAGIC        t.affinity,
# MAGIC        random_recipe_affinity,
# MAGIC        random_product_affinity,       
# MAGIC        (Case when t.whitespot is not Null then t.whitespot else 1 END) as whitespot,
# MAGIC        (Case when t.viewed is not Null then t.viewed else 0 END) as viewed,       
# MAGIC        dense_rank() over(partition by t.integrated_reco_id, t.productCode order by affinity desc, random_recipe_affinity desc) as rnk_rcp
# MAGIC From
# MAGIC (
# MAGIC Select p.*,
# MAGIC        rcp.recipeName,
# MAGIC        rcp.url as recipeUrl
# MAGIC From
# MAGIC (
# MAGIC Select  distinct 
# MAGIC         (Case When operatorOhubId is not Null Then (Case When (emailAddress is not Null and emailAddress <> '') then emailAddress 
# MAGIC                                                          When (emailaddress is Null or emailaddress = '') then operatorOhubId END)
# MAGIC               When operatorOhubId is null     Then s.trackingid end) as integrated_reco_id,
# MAGIC         s.*,
# MAGIC         prd.language,
# MAGIC         prd.productName,
# MAGIC         prd.url as productUrl,
# MAGIC         (case when aff.value is not Null then aff.value else 0 end) as affinity,
# MAGIC         rand() as random_recipe_affinity,
# MAGIC         rand() as random_product_affinity,
# MAGIC         (Case When aff.trackingid is not Null then 1 else 0 END) as personalized
# MAGIC From data_reco_output.tmp1_ods_nl_recommendations s
# MAGIC   left outer join
# MAGIC data_reco_analytical_tables.nl_scores_logit_ga aff on s.trackingid = aff.trackingid and s.productCode = replace(aff.product_code, '_', '-')
# MAGIC inner join
# MAGIC   data_sifu.sifu_product_details prd on s.productCode = prd.productCode and lower(prd.countryCode) = 'nl' 
# MAGIC Where 1=1
# MAGIC and s.productCodeLocal is not Null
# MAGIC ) p
# MAGIC inner join
# MAGIC   data_sifu.sifu_recipe_details rcp on p.recipeId = rcp.recipeId and lower(p.language) = lower(rcp.language) and lower(rcp.countryCode) = 'nl'
# MAGIC ) t
# MAGIC ) r
# MAGIC Where r.rnk_rcp = 1 and whitespot = 1
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select  count(*), 
# MAGIC         count(distinct operatorOhubId), 
# MAGIC         count(distinct integrated_reco_id || ' / ' || productCode || ' / ' ||  recipeId)
# MAGIC From data_reco_output.tmp2_ods_nl_recommendations
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select *
# MAGIC From data_reco_output.tmp2_ods_nl_recommendations_complete_ops
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Drop table if exists data_reco_output.tmp3_ods_nl_recommendations;
# MAGIC Create table data_reco_output.tmp3_ods_nl_recommendations
# MAGIC as
# MAGIC Select *
# MAGIC From
# MAGIC (
# MAGIC Select r.*,
# MAGIC         dense_rank() over(partition by integrated_reco_id order by affinity desc, random_product_affinity desc, productCode desc) as recommendation_id
# MAGIC From
# MAGIC (
# MAGIC Select (Case When rec.operatorOhubId is not Null Then (Case When (emailAddress is not Null and emailAddress <> '') then emailAddress 
# MAGIC                                                             When (emailaddress is Null or emailaddress = '') then operatorOhubId END) 
# MAGIC              When rec.operatorOhubId is Null Then rec.trackingid END) as integrated_reco_id,
# MAGIC        rec.operatorOhubId, 
# MAGIC        rec.contactpersonOhubId,
# MAGIC        rec.language,
# MAGIC        rec.emailAddress,
# MAGIC        rec.local_channel, 
# MAGIC        rec.productCode, 
# MAGIC        rec.cuEanCodes, 
# MAGIC        rec.productCodeLocal, 
# MAGIC        rec.recipeId, 
# MAGIC        rec.productName,
# MAGIC        rec.productUrl,
# MAGIC        rec.recipeName,
# MAGIC        rec.recipeUrl,   
# MAGIC        rec.userid,
# MAGIC        rec.trackingid, 
# MAGIC        rec.emailid,
# MAGIC        rec.personalized,
# MAGIC        rec.affinity,
# MAGIC        random_recipe_affinity,
# MAGIC        random_product_affinity,       
# MAGIC        whitespot,
# MAGIC        viewed,       
# MAGIC        dense_rank() over(partition by 
# MAGIC        (Case When rec.operatorOhubId is not Null Then rec.operatorOhubId 
# MAGIC              When rec.operatorOhubId is Null Then rec.trackingid END), 
# MAGIC              productCode order by affinity desc, 
# MAGIC              random_recipe_affinity desc, 
# MAGIC              random_product_affinity desc) as rnk_rec_opr
# MAGIC From data_reco_output.tmp2_ods_nl_recommendations rec
# MAGIC Where 1=1
# MAGIC ) r
# MAGIC Where 1=1
# MAGIC And rnk_rec_opr = 1
# MAGIC )
# MAGIC Where 1=1
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select *
# MAGIC From data_reco_output.tmp3_ods_nl_recommendations
# MAGIC Where recommendation_id is Null

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Drop table if exists data_reco_output.tmp3_ods_nl_recommendations_complete_ops;
# MAGIC Create table data_reco_output.tmp3_ods_nl_recommendations_complete_ops
# MAGIC as
# MAGIC Select distinct integrated_reco_id
# MAGIC From
# MAGIC (
# MAGIC Select integrated_reco_id,
# MAGIC        max(recommendation_id) as max_recommendation_id
# MAGIC From data_reco_output.tmp3_ods_nl_recommendations
# MAGIC Where 1=1
# MAGIC And recommendation_id <= 3
# MAGIC Group by integrated_reco_id
# MAGIC )
# MAGIC Where 1=1
# MAGIC And max_recommendation_id = 3
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Drop Table if exists data_reco_output.ods_nl_recommendations;
# MAGIC Create Table data_reco_output.ods_nl_recommendations
# MAGIC as
# MAGIC Select *
# MAGIC From data_reco_output.tmp3_ods_nl_recommendations
# MAGIC Where 1=1
# MAGIC And integrated_reco_id in (select integrated_reco_id from data_reco_output.tmp3_ods_nl_recommendations_complete_ops where integrated_reco_id is not Null)
# MAGIC And recommendation_id <= 3
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select  count(*), 
# MAGIC         count(distinct operatorOhubId || ' / ' || productCode),
# MAGIC         count(distinct operatorOhubId)
# MAGIC From data_reco_output.ods_nl_recommendations
# MAGIC Where 1=1 
# MAGIC And operatorOhubId is not Null
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select  count(*), 
# MAGIC         count(distinct trackingid || ' / ' || productCode),
# MAGIC         count(distinct trackingid)
# MAGIC From data_reco_output.ods_nl_recommendations
# MAGIC Where 1=1 
# MAGIC And operatorOhubId is  Null
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select  count(*), 
# MAGIC         count(distinct trackingid || ' / ' || productCode),
# MAGIC         count(distinct trackingid)        
# MAGIC From data_reco_output.ods_nl_recommendations
# MAGIC Where 1=1 
# MAGIC And trackingid is not Null
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Drop table if exists data_reco_output.ods_nl_recommendations_acm;
# MAGIC Create table data_reco_output.ods_nl_recommendations_acm
# MAGIC as
# MAGIC Select rec.operatorOhubId as opr_lnkd_integration_id,
# MAGIC UPPER(rec.language) as language,
# MAGIC cast('NLA' as string) as ws_sku1_prd_cat,
# MAGIC max(Case When recommendation_id = 1 Then 'NL_NL_NL_' || cuEanCodes END) as ws_sku1_prd_cod,
# MAGIC max(Case When recommendation_id = 1 Then productName END) as ws_sku1_prd_nme,
# MAGIC max(Case When recommendation_id = 1 Then affinity END) as ws_sku1_prd_scr,
# MAGIC max(Case When recommendation_id = 1 Then productUrl END) as ws_sku1_prd_ldp,
# MAGIC cast('NLA' as string) as ws_sku2_prd_cat,
# MAGIC max(Case When recommendation_id = 2 Then 'NL_NL_NL_' || cuEanCodes END) as ws_sku2_prd_cod,
# MAGIC max(Case When recommendation_id = 2 Then productName END) as ws_sku2_prd_nme,
# MAGIC max(Case When recommendation_id = 2 Then affinity END) as ws_sku2_prd_scr,
# MAGIC max(Case When recommendation_id = 2 Then productUrl END) as ws_sku2_prd_ldp,
# MAGIC cast('NLA' as string) as ws_sku3_prd_cat,
# MAGIC max(Case When recommendation_id = 3 Then 'NL_NL_NL_' || cuEanCodes END) as ws_sku3_prd_cod,
# MAGIC max(Case When recommendation_id = 3 Then productName END) as ws_sku3_prd_nme,
# MAGIC max(Case When recommendation_id = 3 Then affinity END) as ws_sku3_prd_scr,
# MAGIC max(Case When recommendation_id = 3 Then productUrl END) as ws_sku3_prd_ldp,
# MAGIC cast('' as string) as dps_sku1_prd_cat,
# MAGIC cast('' as string) as dps_sku1_prd_cod,
# MAGIC cast('' as string) as dps_sku1_prd_nme,
# MAGIC cast('' as string) as dps_sku1_prd_scr,
# MAGIC cast('' as string) as dps_sku1_prd_ldp,
# MAGIC cast('' as string) as dps_sku2_prd_cat,
# MAGIC cast('' as string) as dps_sku2_prd_cod,
# MAGIC cast('' as string) as dps_sku2_prd_nme,
# MAGIC cast('' as string) as dps_sku2_prd_scr,
# MAGIC cast('' as string) as dps_sku2_prd_ldp,
# MAGIC cast('' as string) as dps_sku3_prd_cat,
# MAGIC cast('' as string) as dps_sku3_prd_cod,
# MAGIC cast('' as string) as dps_sku3_prd_nme,
# MAGIC cast('' as string) as dps_sku3_prd_scr,
# MAGIC cast('' as string) as dps_sku3_prd_ldp,
# MAGIC cast('' as string) as ws_smp_sku1_prd_cat,
# MAGIC cast('' as string) as ws_smp_sku1_prd_cod,
# MAGIC cast('' as string) as ws_smp_sku1_prd_nme,
# MAGIC cast('' as string) as ws_smp_sku1_prd_scr,
# MAGIC cast('' as string) as ws_smp_sku1_prd_ldp,
# MAGIC cast('' as string) as ws_smp_sku1_prd_smp_fwu,
# MAGIC cast('' as string) as ws_smp_sku2_prd_cat,
# MAGIC cast('' as string) as ws_smp_sku2_prd_cod,
# MAGIC cast('' as string) as ws_smp_sku2_prd_nme,
# MAGIC cast('' as string) as ws_smp_sku2_prd_scr,
# MAGIC cast('' as string) as ws_smp_sku2_prd_ldp,
# MAGIC cast('' as string) as ws_smp_sku2_prd_smp_fwu,
# MAGIC cast('' as string) as ws_smp_sku3_prd_cat,
# MAGIC cast('' as string) as ws_smp_sku3_prd_cod,
# MAGIC cast('' as string) as ws_smp_sku3_prd_nme,
# MAGIC cast('' as string) as ws_smp_sku3_prd_scr,
# MAGIC cast('' as string) as ws_smp_sku3_prd_ldp,
# MAGIC cast('' as string) as ws_smp_sku3_prd_smp_fwu,
# MAGIC cast('NLA' as string) as rcr_sku1_prd_cat,
# MAGIC max(Case When recommendation_id = 1 Then 'NL_NL_NL_' ||recipeId END) as rcr_sku1_prd_cod,
# MAGIC max(Case When recommendation_id = 1 Then recipeName END) as rcr_sku1_prd_nme,
# MAGIC max(Case When recommendation_id = 1 Then affinity END) as rcr_sku1_prd_scr,
# MAGIC max(Case When recommendation_id = 1 Then recipeUrl END) as rcr_sku1_prd_ldp,
# MAGIC cast('NLA' as string) as rcr_sku2_prd_cat,
# MAGIC max(Case When recommendation_id = 2 Then 'NL_NL_NL_' ||recipeId END) as rcr_sku2_prd_cod,
# MAGIC max(Case When recommendation_id = 2 Then recipeName END) as rcr_sku2_prd_nme,
# MAGIC max(Case When recommendation_id = 2 Then affinity END) as rcr_sku2_prd_scr,
# MAGIC max(Case When recommendation_id = 2 Then recipeUrl END) as rcr_sku2_prd_ldp,
# MAGIC cast('NLA' as string) as rcr_sku3_prd_cat,
# MAGIC max(Case When recommendation_id = 3 Then 'NL_NL_NL_' ||recipeId END) as rcr_sku3_prd_cod,
# MAGIC max(Case When recommendation_id = 3 Then recipeName END) as rcr_sku3_prd_nme,
# MAGIC max(Case When recommendation_id = 3 Then affinity END) as rcr_sku3_prd_scr,
# MAGIC max(Case When recommendation_id = 3 Then recipeUrl END) as rcr_sku3_prd_ldp
# MAGIC From data_reco_output.ods_nl_recommendations rec
# MAGIC Where 1=1
# MAGIC And rec.operatorOhubId is Not Null
# MAGIC Group by rec.operatorOhubId,
# MAGIC rec.language 
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select *
# MAGIC From data_reco_output.ods_nl_recommendations

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select  userId,	trackingId,	cast('PRODUCT' as string) as recommendationType, cast('NL' as string) as countryCode, lower(language) as languageCode, productCode as recommendation, recommendation_id as rank        
# MAGIC From data_reco_output.ods_nl_recommendations
# MAGIC Where 1=1 
# MAGIC And trackingid is not Null
# MAGIC Order by userId, trackingId, recommendation_id
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select count(*),
# MAGIC        count(distinct operatorOhubId)
# MAGIC From data_reco_input.nl_recommendation_set rcs
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select  count(*), 
# MAGIC         count(distinct operatorOhubId || ' / ' || contactpersonOhubId || ' / ' || (Case When trackingid is not Null then trackingid else '' END)  || ' / ' || productCode || ' / ' || (Case When recipeId is not Null Then recipeId else '' END)), 
# MAGIC         count(distinct productCode),
# MAGIC         count(distinct operatorOhubId),
# MAGIC         count(distinct emailAddress)
# MAGIC From data_reco_output.tmp2_ods_nl_recommendations
# MAGIC Where 1=1
# MAGIC --And emailAddress in 
# MAGIC --(Select cp_emailAddress From data_fpo.reachable_fpo_20190328 fpo where fpo.op_concatId is not null and reachableEmail = 'Y')
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select  language,
# MAGIC         count(*), 
# MAGIC         count(distinct operatorOhubId || ' / ' || contactpersonOhubId || ' / ' || (Case When trackingid is not Null then trackingid else '' END)  || ' / ' || productCode || ' / ' || (Case When recipeId is not Null Then recipeId else '' END)), 
# MAGIC         count(distinct productCode),
# MAGIC         count(distinct operatorOhubId),
# MAGIC         count(distinct emailAddress)
# MAGIC From data_reco_output.ods_nl_recommendations
# MAGIC Where 1=1
# MAGIC --And operatorOhubId in 
# MAGIC --(Select op_ohubID From data_fpo.reachable_fpo_20190328 fpo where fpo.op_concatId is not null and reachableFPO = 'Y')
# MAGIC Group by language
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select  language,
# MAGIC         count(*), 
# MAGIC         count(distinct opr_lnkd_integration_id)
# MAGIC From data_reco_output.ods_nl_recommendations_acm
# MAGIC Where 1=1
# MAGIC Group by language
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select  language,
# MAGIC         count(*), 
# MAGIC         count(distinct opr_lnkd_integration_id)
# MAGIC From data_reco_output.ods_nl_recommendations_acm
# MAGIC Where 1=1
# MAGIC And opr_lnkd_integration_id in 
# MAGIC (Select op_ohubID From data_fpo.reachable_fpo_20190328 fpo where fpo.op_concatId is not null and reachableFPO = 'Y')
# MAGIC Group by language
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select  language,
# MAGIC         count(*), 
# MAGIC         count(distinct opr_lnkd_integration_id)
# MAGIC From data_reco_output.ods_nl_recommendations_acm
# MAGIC Where 1=1
# MAGIC And opr_lnkd_integration_id in 
# MAGIC (Select op_ohubID From data_fpo.reachable_fpo_20190328 fpo where fpo.op_concatId is not null and reachableFPO = 'Y')
# MAGIC And ws_sku3_prd_cod is not Null
# MAGIC Group by language
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select *
# MAGIC From data_reco_output.ods_nl_recommendations_acm
# MAGIC Where 1=1
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select  count(distinct trackingid) as cnt_tracking,
# MAGIC         count(distinct EmailAddress) as cnt_emailAddress
# MAGIC From data_reco_input.ods_reco_contacts_IDS
# MAGIC Where 1=1
# MAGIC And trackingid is not Null 
# MAGIC And countryCode = 'NL'
# MAGIC UNION ALL
# MAGIC Select  count(distinct trackingid) as cnt_tracking,
# MAGIC         count(distinct EmailAddress) as cnt_emailAddress
# MAGIC From data_reco_input.ods_reco_contacts_IDS rcs
# MAGIC Where 1=1
# MAGIC And countryCode = 'NL'
# MAGIC And trackingid is not Null
# MAGIC And emailAddress in 
# MAGIC (Select cp_emailAddress From data_fpo.reachable_fpo_20190328 fpo where cp_emailAddress is not null and reachableEmail = 'Y')
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select  personalized, 
# MAGIC         whitespot, 
# MAGIC         viewed, 
# MAGIC         recommendation_id,
# MAGIC         count(*), 
# MAGIC         count(distinct operatorOhubId || ' / ' || contactpersonOhubId), 
# MAGIC         count(distinct operatorOhubId || ' / ' || contactpersonOhubId || ' / ' || emailAddress), 
# MAGIC         count(distinct productCode),
# MAGIC         count(distinct operatorOhubId),
# MAGIC         count(distinct emailAddress)
# MAGIC From data_reco_output.ods_nl_recommendations
# MAGIC Where 1=1
# MAGIC And recommendation_id <= 3
# MAGIC And emailAddress in 
# MAGIC (Select cp_emailAddress From data_fpo.reachable_fpo_20190328 fpo where fpo.op_concatId is not null and reachableEmail = 'Y')
# MAGIC Group by personalized, 
# MAGIC          whitespot, 
# MAGIC          viewed,
# MAGIC          recommendation_id
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select  personalized,
# MAGIC         recommendation_id,
# MAGIC         local_channel,
# MAGIC         productName,
# MAGIC         productCode,
# MAGIC         count(*), 
# MAGIC         count(distinct operatorOhubId || ' / ' || contactpersonOhubId), 
# MAGIC         count(distinct emailAddress)
# MAGIC From data_reco_output.tmp2_ods_nl_recommendations
# MAGIC Where 1=1
# MAGIC And recommendation_id <= 3
# MAGIC And emailAddress in 
# MAGIC (Select cp_emailAddress From data_fpo.reachable_fpo_20190328 fpo where fpo.op_concatId is not null and reachableEmail = 'Y')
# MAGIC Group by personalized,
# MAGIC           recommendation_id,
# MAGIC           local_channel,
# MAGIC           productName,
# MAGIC           productCode
# MAGIC Order by  personalized,
# MAGIC           recommendation_id,
# MAGIC           local_channel,
# MAGIC           count(*) desc
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select  personalized,
# MAGIC         recommendation_id,
# MAGIC         local_channel,
# MAGIC         recipeName,
# MAGIC         recipeId,
# MAGIC         count(*), 
# MAGIC         count(distinct operatorOhubId || ' / ' || contactpersonOhubId), 
# MAGIC         count(distinct emailAddress)
# MAGIC From data_reco_output.tmp2_ods_nl_recommendations
# MAGIC Where 1=1
# MAGIC And recommendation_id <= 3
# MAGIC And emailAddress in 
# MAGIC (Select cp_emailAddress From data_fpo.reachable_fpo_20190328 fpo where fpo.op_concatId is not null and reachableEmail = 'Y')
# MAGIC Group by personalized,
# MAGIC           recommendation_id,
# MAGIC           local_channel,
# MAGIC           recipeName,
# MAGIC           recipeId
# MAGIC Order by  personalized,
# MAGIC           recommendation_id,
# MAGIC           local_channel,
# MAGIC           count(*) desc
# MAGIC ;
