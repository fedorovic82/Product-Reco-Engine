# Databricks notebook source
countries = ["AT", "CH", "DE", "FR", "NL", "ES", "PT", "IT", "PL", "TR", "AU", "NZ", "US","CA","BR"]

for c in countries:
  dbutils.notebook.run('/Shared/dataScience/Reco2/F01_businessRulingFiltering/filterDistributed', 12000, {"runCountryCode": c})
