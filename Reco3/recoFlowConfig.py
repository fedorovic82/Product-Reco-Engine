# Databricks notebook source
from datetime import datetime, timedelta
from pytz import timezone

# COMMAND ----------

# MAGIC %md ### 0) Select the countries for the steps

# COMMAND ----------

countriesGa = ['AR', 'AT', 'AU', 'BE', 'BG', 'BR', 'CA', 'CH', 'CL', 'CO', 'CR', 'CZ', 'DE', 'DK', 'EG', 'ES', 'FI', 'FR', 'GB', 'GR', 'HK', 'HU', 'ID', 'IE', 'IL', 'IT', 'LK', 'LT', 'MX', 'MY', 'NL', 'NZ', 'PH', 'PK', 'PL', 'PT', 'RO', 'RU', 'SA', 'SE', 'SG', 'SK', 'TH', 'TR', 'TW', 'US', 'VN', 'ZA'] 
# Iso, sifu and wunderman differ for: AE, BH, CN, EC, EE, GT, HN, IN, JO, KR, KW, LB, LV, MO, MQ, MV, NI, OM, PA, PY, QA, SV, UY
countriesSales = ['AT', 'BE', 'CH', 'DE', 'ES', 'FR', 'MY', 'NL', 'PH', 'PL', 'PT', 'TR', 'ZA']
# Countries with a large GA volume, smallest nodes break if estimating the model with 6 months of data (on 4GB ram), reduce train data to past 3 months (see createFeatureTablesGA script).
# DE was in here, however, big drop in trackingIds as of March 2020
largeCountriesGa = ['CA', 'ID', 'US']

countriesNoSalesScores = ['AU', 'CA', 'FR', 'IT', 'US'] # Countries where the sales scores should not be used, because?? ask tim

# Countries with or without business ruling
countriesBusinessRuling = ['AT', 'AU', 'BE', 'BR', 'CA', 'CH', 'DE', 'ES', 'FR', 'IT', 'NL', 'NZ', 'PL', 'PT', 'TR', 'US']
countriesNoBusinessRuling = [c for c in countriesGa if c not in countriesBusinessRuling]

# Countries where the cuEanCode is not the standard used code (check the sifu_product_details if the cuEanCodes field is properly filled or not)
countriesProductCodeLocal = ['BG', 'CA', 'HK', 'TH', 'US', 'VN']
countriesCuEanCodes = [c for c in countriesBusinessRuling if c not in countriesProductCodeLocal]

countriesExport = countriesGa
# countriesExport = ['AR', 'AT', 'AU', 'BE', 'BR', 'CH', 'CL', 'CO', 'CR', 'CZ', 'DE', 'DK', 'EG', 'ES', 'FI', 'FR', 'GB', 'GR', 'HU', 'ID', 'IE', 'IL', 'IT', 'LT', 'MX', 'MY', 'NL', 'NZ', 'PH', 'PK', 'PL', 'PT', 'RO', 'RU', 'SA', 'SG', 'SK', 'TR', 'TW', 'ZA']
# not exporting BG, CA, HK, LK, SE, TH, US, VN compared to countriesGa

# countriesNotPersonalized = ['BG', 'CR', 'CZ', 'DK', 'EG', 'GR', 'HK', 'IE', 'IL', 'LK', 'LT', 'RO', 'RU', 'SE', 'SK', 'TW']

# COMMAND ----------

'''
Phase 1:
BG --> no trackingid pageviews
BG --> no recipe pageviews
CR --> not in GA data
CZ --> not in config table
DK --> not in GA data
EG --> not in GA data
HK --> low volumes (products + recipes)
IE --> low volumes (recipes)
IL --> low volumes (products + recipes)
LK --> no trackingid pageviews
LT --> not in GA data
RO --> low volumes (products + recipes)
RU --> low volumes (products + recipes)
SE --> not in GA data
SK --> low volumes (products + recipes)
TW --> low volumes (recipes)
'''
