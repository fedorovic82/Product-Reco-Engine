# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists data_reco_feature_tables.ods_features_sales;
# MAGIC create table data_reco_feature_tables.ods_features_sales
# MAGIC   using DELTA
# MAGIC   location '/delta/ods_features_sales'

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize data_reco_feature_tables.ods_features_sales
# MAGIC ZORDER by (FEATURE, operatorOhubId)
