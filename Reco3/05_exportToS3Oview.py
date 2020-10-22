# Databricks notebook source
import re
import boto3

date = getArgument('date')
countryCodeU = getArgument('countryCode').upper()

# date = '20200813'
# countryCodeU = 'ZA'

s3Folder = "export_files/recommendations/oview/"
tableName = 'dev_derived_reco.output_recommendations_oview'

# COMMAND ----------

fileDir = 'dbfs:/mnt/datamodel/dev/derived/reco/export/oview/' + countryCodeU + "/"
fileName = re.sub('.*\.', '', tableName)
fileNameCsv1 = fileDir + fileName + "_" + date
fileNameCsv2 = fileDir + fileName + "_" + date + '.csv'
s3FileName = s3Folder + countryCodeU.lower() + "_" + fileName + "_" + date + '.csv'
s3Source = '/' + re.sub('\:', '', fileDir) + fileName + "_" + date + '.csv'

print(fileNameCsv1)
print(s3Source)

# COMMAND ----------

from pyspark.sql import functions as f
df = spark.table(tableName).filter(f.col('countryCode') == countryCodeU)
df.show()

# COMMAND ----------

# Write csv file
df \
  .repartition(1) \
  .write \
  .mode('overwrite') \
  .option('sep', ';') \
  .option('header', 'true') \
  .option('quoteAll', 'true') \
  .csv(fileNameCsv1)

# COMMAND ----------

display(df.select('countryCode', 'opr_lnkd_integration_id', 'recommendation_type', 'recommendation_id', 'recommendation_name', 'recommendation_rank'))

# COMMAND ----------

# MAGIC %run "/Shared/S01_Libraries_Functions"

# COMMAND ----------

# Merge multiple csv into One
copyMerge(fileNameCsv1, fileNameCsv2)

# COMMAND ----------

bucketName = dbutils.secrets.get(scope = 's3bucket', key = 'bucket_name')
accessKey  = dbutils.secrets.get(scope = 's3bucket', key = 'access_key')
secretKey  = dbutils.secrets.get(scope = 's3bucket', key = 'secret_key')

# COMMAND ----------

# Put the file on the S3 bucket
s3 = boto3.resource('s3', 
                    aws_access_key_id=accessKey, 
                    aws_secret_access_key=secretKey, 
                    config=boto3.session.Config(signature_version='s3v4'))

s3.Bucket(bucketName).upload_file(s3Source, s3FileName)
