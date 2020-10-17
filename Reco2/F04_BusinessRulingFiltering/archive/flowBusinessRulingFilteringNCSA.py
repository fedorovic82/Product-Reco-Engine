# Databricks notebook source
areaCode = 'ncsa'

for countryCode in ["US","CA","BR"]:
  dbutils.notebook.run("filterDistributed", 6000, {"runCountryCode": countryCode, "runAreaCode": areaCode})
