# Databricks notebook source
import re
import boto3
import zipfile
import os
import shutil
from pyspark.sql import functions as f

date = getArgument('date')
countryCodeU = getArgument('countryCode').upper()
recommendationTypeL = getArgument('recommendationType').lower()

# date = 'test'
# countryCodeU = 'NL'
# recommendationTypeL = 'product'

countryCodeL = countryCodeU.lower()
fileName = 'output_recommendations_' + recommendationTypeL + '_ufs_com' + '_' + countryCodeL

# COMMAND ----------

fileDir = 'dbfs:/mnt/datamodel/dev/derived/reco/export/ufs_com/' + countryCodeL + "/"
fileDir2 = '/' + re.sub('\:', '', fileDir)
fileNameCsv1 = fileName + "_" + date
fileNameCsv2 = fileName + "_" + date + '.csv'
fileNameZip = fileName + "_" + date + '.zip'
filePathCsv1 = fileDir + fileNameCsv1
filePathCsv2 = fileDir + fileNameCsv2
filePathZip = fileDir2 + fileNameZip
s3Folder = "ufs_com_reco_bar/"
s3FileName = s3Folder + fileNameZip
s3Source = fileDir2 + fileNameZip

print(fileNameCsv1)
print(s3Source)
print(s3FileName)

# COMMAND ----------

df = spark.table('dev_derived_reco.output_recommendations_ufs_com') \
  .where(f.col('countryCode') == countryCodeU) \
  .where(f.lower(f.col('recommendationType')) == recommendationTypeL)
df.show()

# COMMAND ----------

# Write csv file
df \
  .repartition(1) \
  .write \
  .mode('overwrite') \
  .option('sep', '|') \
  .option('header', 'true') \
  .option('quoteAll', 'false') \
  .csv(filePathCsv1)

# COMMAND ----------

# MAGIC %run "/Shared/S01_Libraries_Functions"

# COMMAND ----------

# Merge multiple csv into One
copyMerge(filePathCsv1, filePathCsv2, debug=True)

# COMMAND ----------

with zipfile.ZipFile('/tmp/' + fileNameZip, 'w', zipfile.ZIP_DEFLATED) as zipf:
  zipf.write(os.path.join(fileDir2, fileNameCsv2), arcname=fileNameCsv2)
shutil.move('/tmp/' + fileNameZip, filePathZip)

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
