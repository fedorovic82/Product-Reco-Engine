# Databricks notebook source
from datetime import datetime, timedelta
from pytz import timezone

# Set recoDate to last sunday
today = datetime.now(pytz.timezone('Europe/Amsterdam')).date()
idx = (today.weekday() + 1) % 7
lastSunday = today - timedelta(days=idx)
recoDate = lastSunday.strftime('%Y%m%d')
print(recoDate)

countryCodeListFull = ['DE', 'AT', 'CH', 'ES', 'PT', 'NL', 'PL', 'TR', 'FR', 'US', 'CA', 'AU', 'IT']
countryCodeListNoSalesScores =                                        ['FR', 'US', 'CA', 'AU', 'IT']

countryCodeListCuEanCodes = ['DE', 'AT', 'CH', 'ES', 'PT', 'NL', 'IT']
countryCodeListProductCodeLocal = [c for c in countryCodeListFull if c not in countryCodeListCuEanCodes]
print(countryCodeListProductCodeLocal)

for countryCode in countryCodeListFull:
  dbutils.notebook.run("/Shared/dataScience/Reco2/F05_businessRulingRanking/rankDistributedMemory", 
                       12000, 
                       {"runCountryCode": countryCode, 
                        "recoDate": recoDate, 
                        "countryCodeListNoSalesScores": countryCodeListNoSalesScores, 
                        "countryCodeListCuEanCodes": countryCodeListCuEanCodes, 
                        "countryCodeListProductCodeLocal": countryCodeListProductCodeLocal})
