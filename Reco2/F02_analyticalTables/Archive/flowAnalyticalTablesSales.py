# Databricks notebook source
countries = ["MY", "AT", "CH", "ES", "NL", "BE", "PL", "PT", "TR", "ZA", "DE"]

for countryCode in countries:
  dbutils.notebook.run("/Shared/dataScience/Reco2/F03_analyticalTables/S03_createAnalyticalTableSales", 12000, {"runCountryCode": countryCode})
  
for countryCode in countries:
  dbutils.notebook.run("/Shared/dataScience/Reco2/F03_analyticalTables/S04_loadAnalyticalTableSalesIntoHive", 12000, {"runCountryCode": countryCode})
