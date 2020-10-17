# Databricks notebook source
areaCode = 'emea'

for countryCode in ["AT", "CH", "DE", "FR", "NL", "ES", "PT", "IT", "PL", "TR"]:
  dbutils.notebook.run("filterDistributed", 12000, {"runCountryCode": countryCode, "runAreaCode": areaCode})
