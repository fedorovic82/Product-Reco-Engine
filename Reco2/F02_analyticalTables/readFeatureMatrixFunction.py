# Databricks notebook source
from pyspark.sql import functions as f

def readFeatureMatrix(dfFeatureLong, countryCode, dimension1, dimension2, features, postfix = None):
  filterString = ''
  for i, feature in enumerate(features): 
    if i == 0:
      filterString += 'feature like "' + feature + '%"'
    else:
      filterString += ' or feature like "' + feature + '%"'
  
  if 'country_code' in dfFeatureLong.columns:
    countryCodeColName = 'country_code'
  elif 'countryCode' in dfFeatureLong.columns:
    countryCodeColName = 'countryCode'
  
  dfFeatureLong = dfFeatureLong \
    .where(filterString) \
    .where(f.col(countryCodeColName).like(countryCode))

  if postfix:
    dfFeatureLong = dfFeatureLong.withColumn('feature', f.regexp_replace('feature', postfix, ''))
  
  if dimension2 == 'NA':
    dfFeature = dfFeatureLong \
      .groupBy(countryCodeColName, dimension1) \
      .pivot('feature') \
      .max('value')
  else:
    dfFeature = dfFeatureLong \
      .groupBy(countryCodeColName, dimension1, dimension2) \
      .pivot('feature') \
      .max('value')
  return dfFeature
