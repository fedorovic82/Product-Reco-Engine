# Databricks notebook source
countries = ['DE', 'AT', 'CH', 'NL', 'FR', 'BE', 'PT', 'ES', 'IT', 'GR', 'US', 'CA', 'BR', 'CL', 'AR', 'ZA', 'AU', 'PL', 'SG', 'MY', 'VN', 'TH', 'ID', 'GB', 'FI', 'HU', 'MX', 'PH', 'PK', 'RO', 'CO']

for countryCode in countries:
  dbutils.notebook.run("/Shared/dataScience/Reco2/F03_analyticalTables/S01_createAnalyticalTableGA", 12000, {"runCountryCode": countryCode})

for countryCode in countries:
  dbutils.notebook.run("/Shared/dataScience/Reco2/F03_analyticalTables/S02_loadAnalyticalTableGAIntoHive", 12000, {"runCountryCode": countryCode})
