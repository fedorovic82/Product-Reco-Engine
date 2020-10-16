# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists data_reco_feature_tables.ods_features_pageviews;
# MAGIC create table data_reco_feature_tables.ods_features_pageviews
# MAGIC   using DELTA
# MAGIC   location '/delta/ods_features_pageviews'

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize data_reco_feature_tables.ods_features_pageviews
# MAGIC ZORDER by (FEATURE, trackingId)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists data_reco_feature_tables.ods_features_users;
# MAGIC create table data_reco_feature_tables.ods_features_users
# MAGIC   using DELTA
# MAGIC   location '/delta/ods_features_users'

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize data_reco_feature_tables.ods_features_users
# MAGIC ZORDER by (FEATURE, trackingId)
