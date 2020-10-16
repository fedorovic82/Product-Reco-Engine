# Databricks notebook source
countriesGA =      ['AR', 'AT', 'AU', 'BE', 'BR', 'CA', 'CH', 'CL', 'CO', 'DE', 'ES', 'FI', 'FR', 'GB', 'GR', 'HU', 'ID', 'IT', 'MX', 'MY', 'NL', 'PH', 'PK', 'PL', 'PT', 'RO', 'SG', 'TH', 'TR', 'US', 'VN', 'ZA']
largeCountriesGA = [            'AU', 'BE', 'BR', 'CA',                   'DE',             'FR',                   'ID',             'MY',       'PH',       'PL',                         'TR', 'US',       'ZA']

countriesSales =      ['AT', 'BE', 'CH', 'DE', 'ES', 'FR', 'MY', 'NL', 'PL', 'PT', 'TR', 'ZA']
largeCountriesSales = [                  'DE'                                                ]

countriesBusinessRuling = ['AT', 'AU', 'BR', 'CA', 'CH', 'DE', 'ES', 'FR', 'IT', 'NL', 'NZ', 'PL', 'PT', 'TR', 'US']
countriesNoBusinessRuling = [c for c in countriesGA if c not in countriesBusinessRuling]

# COMMAND ----------

for countryCode in countriesGA:
  try:
    dbutils.notebook.run("/Shared/dataScience/Reco2/F02_analyticalTables/Dev01_createAnalyticalTableGA", 12000, {"runCountryCode": countryCode})
  except Exception as e:
    print(countryCode + ": " + str(e))

# COMMAND ----------

for countryCode in countriesSales:
  try:
    dbutils.notebook.run("/Shared/dataScience/Reco2/F02_analyticalTables/Dev02_createAnalyticalTableSales", 12000, {"runCountryCode": countryCode})
  except Exception as e:
    print(countryCode + ": " + str(e))
