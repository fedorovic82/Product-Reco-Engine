# Databricks notebook source
from datetime import datetime
from pytz import timezone

today = datetime.now(timezone('Europe/Amsterdam'))
today = today.strftime("%Y%m%d")

# COMMAND ----------

countriesGA = ['AR', 'AT', 'AU', 'BE', 'BG', 'BR', 'CA', 'CH', 'CL', 'CO', 'CR', 'CZ', 'DE', 'DK', 'EG', 'ES', 'FI', 'FR', 'GB', 'GR', 'HK', 'HU', 'ID', 'IE', 'IL', 'IT', 'LK', 'LT', 'MX', 'MY', 'NL', 'NZ', 'PH', 'PK', 'PL', 'PT', 'RO', 'RU', 'SA', 'SE', 'SG', 'SK', 'TH', 'TR', 'TW', 'US', 'VN', 'ZA'] 
# countriesGa = ['ES', 'IT', 'NL', 'US']
for c in countriesGa:
  try:
    dbutils.notebook.run("/Shared/dataScience/Reco2/F03_RScale/Development/logitModelGaPy", 12000, {"countryCode": c, "date": today})
  except Exception as e:
    print(c + ': ' + str(e))

# COMMAND ----------

countriesSales = ['AT', 'BE', 'CH', 'DE', 'ES', 'FR', 'MY', 'NL', 'PL', 'PT', 'TR', 'ZA']
# countriesSales = ['AT', 'CH', 'DE', 'ES', 'NL']
for c in countriesSales:
  try:
    dbutils.notebook.run("/Shared/dataScience/Reco2/F03_RScale/Development/logitModelSalesPy", 12000, {"countryCode": c, "date": today})
  except Exception as e:
    print(c + ': ' + str(e))
