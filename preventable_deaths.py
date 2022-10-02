
# Make a map of preventable COVID deaths by county.

# Chuck Connell, fall 2022

import pandas as pd 
from urllib import request

COVID_ACT_NOW_DOWNLOAD = "https://api.covidactnow.org/v2/counties.timeseries.csv?apiKey=402c0523d9e64d4fb62f37fbf499bf7b"
COVID_ACT_NOW_LOCAL = "/Users/chuck/Desktop/COVID Programming/Covid Act Now/counties.timeseries.csv"

START_DATE = "20210715"  
END_DATE = "20220915"
PREVENTABLE_PORTION = 0.681   # from CDC data and my spreadsheet

CHART_DATA = "preventable_by_county.tsv"

# Get USA COVID outcomes by county.

#request.urlretrieve(COVID_ACT_NOW_DOWNLOAD, COVID_ACT_NOW_LOCAL)  # don't need to retrieve this every time, if we already have a good copy
CovidDF = pd.read_csv(COVID_ACT_NOW_LOCAL, sep=',', header='infer', dtype=str)

# Tweak fields as needed.

CovidDF = CovidDF[["date", "fips", "actuals.deaths"]]  # throw out many fields we don't need

CovidDF["date"] = pd.to_datetime(CovidDF["date"], errors='coerce')
CovidDF["actuals.deaths"] = pd.to_numeric(CovidDF["actuals.deaths"], errors='coerce').fillna(0)

# Get just the rows for the start date, then the end dates. Change field names to keep them straight later.

StartDF = CovidDF.query("date == " + START_DATE) 
StartDF = StartDF.rename(columns={"date": "start_date", "actuals.deaths": "start_deaths"})

EndDF = CovidDF.query("date == " + END_DATE) 
EndDF = EndDF.rename(columns={"date": "end_date", "actuals.deaths": "end_deaths"})

# Join the start and end dates, making the DF we really want.

MapDF = StartDF.merge(EndDF, how='inner', on="fips")

# Calc the deaths in the time period and the preventable portion.

MapDF["period_deaths"] = (MapDF["end_deaths"] - MapDF["start_deaths"])
MapDF["preventable_deaths"] = (MapDF["period_deaths"] * PREVENTABLE_PORTION).round(0).astype(int)

# Write out the chart data file. 

print ("\nWriting chart data to " + CHART_DATA)
MapDF.to_csv(CHART_DATA, encoding='utf-8', sep='\t', index=False)

