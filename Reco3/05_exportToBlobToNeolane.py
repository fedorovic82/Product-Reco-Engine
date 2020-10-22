# Databricks notebook source
# DBTITLE 1,Step 1 - Write files to a blob container
# MAGIC %run "/Shared/S01_Libraries_Functions"

# COMMAND ----------

from pyspark.sql import functions as f
from datetime import datetime
from pytz import timezone

now = datetime.now(timezone('Europe/Amsterdam')).strftime('%Y%m%d%H%M%S')

mcoU = getArgument("mco").upper()
countryCodeU = getArgument("countryCode").upper()

# COMMAND ----------

inFile = 'dev_derived_reco.output_recommendations_acm'
outFile = mcoU + '_' + countryCodeU + '_recommendation_feed1_' + now + '.csv'

# COMMAND ----------

# dbfs folder in mounted folder 
csvFolderpath = "dbfs:/mnt/datamodel/dev/derived/reco/export/acm/" + countryCodeU + "/"
print(csvFolderpath)

# COMMAND ----------

df = spark.table(inFile).filter(f.col('countryCode') == countryCodeU)
df.show()

# COMMAND ----------

# DBTITLE 1,Step 2 - Write a dataframe to a single file in blob container
# Read the spark table and write with right settings (only line separator is still wrong)
df \
  .drop('countryCode') \
  .repartition(1) \
  .write \
  .option("mode", "overwrite") \
  .option("header", True) \
  .option("sep","|") \
  .option("encoding", "UTF-8") \
  .option("escape", '"') \
  .option("nullValue", "") \
  .option("emptyValue", "") \
  .csv(csvFolderpath + "tmp_csv_folder.csv")
copyMerge(csvFolderpath + "tmp_csv_folder.csv",
          csvFolderpath + "tmp_csv.csv",
          debug=True,
          overwrite=True,
          deleteSource=True)

# Read the csv file as text and modify the line separator
# Coalesce is used here instead of repartition if you write a text file, since repartition shuffles the data, coalesce doesn't.
spark.read.text(csvFolderpath + "tmp_csv.csv") \
  .coalesce(1) \
  .write \
  .option("lineSep", "\r\n") \
  .option("encoding", "UTF-8") \
  .text(csvFolderpath + "tmp_txt_folder.txt")
copyMerge(csvFolderpath + "tmp_txt_folder.txt",
          csvFolderpath + outFile,
          debug=True,
          overwrite=True,
          deleteSource=True)

# Remove csv file with wrong lineSep
dbutils.fs.rm(csvFolderpath + "tmp_csv.csv")

# COMMAND ----------

# DBTITLE 1,Step 3 - Transfer files from blob storage to Neolane SFTP
import pysftp
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

# COMMAND ----------

import re
myHostname = dbutils.secrets.get(scope = 'neolanesftp', key = 'neolaneHostname')
myUsername = dbutils.secrets.get(scope = 'neolanesftp', key = 'neolaneUsername')
myPassword = dbutils.secrets.get(scope = 'neolanesftp', key = 'neolanePassword')

csvFolderpathWorker = '/dbfs' + re.sub('dbfs:', '', csvFolderpath)

with pysftp.Connection(host=myHostname, username=myUsername, password=myPassword, cnopts=cnopts) as sftp:
  print("Connection succesfully stablished ... ")
  # Define the file that you want to upload from your local directorty
  localFilePath = csvFolderpathWorker + outFile
  # Define the remote path where the file will be uploaded
  remoteFilePath = '/incoming/import/' + outFile
  # Transfer the file to the sftp
  sftp.put(localFilePath, remoteFilePath)
  print("Put file succes: %s" % localFilePath)
# connection closed automatically at the end of the with-block

# COMMAND ----------

# DBTITLE 1,Step 4 - Check file errors by inspection of error file size
import pysftp
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

# COMMAND ----------

with pysftp.Connection(host=myHostname, username=myUsername, password=myPassword, cnopts=cnopts) as sftp:
  print("Connection succesfully stablished ... ")
  fileList = sftp.listdir('/incoming/import/')
  for f in fileList:
    if 'recommendation' in f:
      print(f)
      print(sftp.stat('/incoming/import/' + f))
# connection closed automatically at the end of the with-block

# COMMAND ----------

with pysftp.Connection(host=myHostname, username=myUsername, password=myPassword, cnopts=cnopts) as sftp:
  print("Connection succesfully stablished ... ")
  fileList = sftp.listdir('/incoming/import/archive/')
  for f in fileList:
    if 'errors' in f:
      print(f)
      print(sftp.stat('/incoming/import/archive/' + f))
# connection closed automatically at the end of the with-block
