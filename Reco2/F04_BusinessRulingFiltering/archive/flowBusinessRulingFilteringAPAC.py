# Databricks notebook source
areaCode = 'apac'

for countryCode in ["AU"]:
  dbutils.notebook.run("filterDistributed", 6000, {"runCountryCode": countryCode, "runAreaCode": areaCode})
