
# Make a datafile with the wastewater data from Biobot and COVID-19 disease outcomes for 
# those areas and dates. Put into a format that is good for creating a Flourish chart.

import pandas as pd 
from urllib import request

BIOBOT_DOWNLOAD = "https://github.com/biobotanalytics/covid19-wastewater-data/raw/master/wastewater_by_county.csv"
COVID_ACT_NOW_DOWNLOAD = "https://api.covidactnow.org/v2/country/US.timeseries.csv?apiKey=402c0523d9e64d4fb62f37fbf499bf7b"

#COUNTY_POP_LOCAL = "/Users/chuck/Desktop/COVID Programming/US Census/Population_Density_County.csv"
BIOBOT_LOCAL = "/Users/chuck/Desktop/COVID Programming/Biobot/wastewater_by_county.csv"
COVID_ACT_NOW_LOCAL = "/Users/chuck/Desktop/COVID Programming/Covid Act Now/counties.timeseries.csv"

BIOBOT_USA_CHART_DATA = "biobot_vs_outcomes_usa.tsv"
BIOBOT_REGIONS_CHART_DATA = "biobot_vs_outcomes_regions.tsv"

# Get the population density of US counties, and tweak as needed.
# This is June 2020 data that I downloaded once from https://covid19.census.gov/datasets/21843f238cbb46b08615fc53e19e0daf/explore

#CountyPopDF = pd.read_csv(COUNTY_POP_LOCAL, sep=',', header='infer', dtype=str)
#CountyPopDF = CountyPopDF[["GEOID", "B01001_calc_PopDensity"]]  
#CountyPopDF = CountyPopDF.rename(columns={"GEOID":"FIPS", "B01001_calc_PopDensity":"DensitySqKm"})  

# Get the latest data from Biobot, and tweak as we need it.

request.urlretrieve(BIOBOT_DOWNLOAD, BIOBOT_LOCAL)
BiobotDF = pd.read_csv(BIOBOT_LOCAL, sep=',', header='infer', dtype=str)

BiobotDF = BiobotDF.rename(columns={"fipscode":"FIPS"})  # to match Flourish naming
BiobotDF.loc[BiobotDF["FIPS"].str.len() == 4, "FIPS"] = "0" + BiobotDF["FIPS"]  # fix problem with missing leading zeroes 
BiobotDF["sampling_week"] = pd.to_datetime(BiobotDF["sampling_week"], errors='coerce').dt.date
BiobotDF["effective_concentration_rolling_average"] = pd.to_numeric(BiobotDF["effective_concentration_rolling_average"], errors='coerce')

# Group Biobot data by whole country or regions and get average gene copies for that week. Use as_index=False to restore grouped column names.

BiobotWeeklyUsaDF = BiobotDF[["sampling_week","effective_concentration_rolling_average"]]
BiobotWeeklyUsaDF = BiobotWeeklyUsaDF.groupby("sampling_week", sort=True, dropna=True, as_index=False).mean()

BiobotWeeklyRegionsDF = BiobotDF[["sampling_week","region","effective_concentration_rolling_average"]]
BiobotWeeklyRegionsDF = BiobotWeeklyRegionsDF.groupby(["sampling_week","region"], sort=True, dropna=True, as_index=False).mean()

# Get overall USA COVID outcomes. Tweak as needed.

request.urlretrieve(COVID_ACT_NOW_DOWNLOAD, COVID_ACT_NOW_LOCAL)
CovidDF = pd.read_csv(COVID_ACT_NOW_LOCAL, sep=',', header='infer', dtype=str)

CovidDF = CovidDF.rename(columns={"metrics.caseDensity":"metrics.caseDensity100k", "date":"covid_facts_date"})
CovidDF["covid_facts_date"] = pd.to_datetime(CovidDF["covid_facts_date"], errors='coerce')

# Add a rolling average for some key hospitalization info. It is only reported weekly, so there are many empty days now.
# The goal is to fill in the missing days with reasonable numbers, so downstream there is something there.

CovidDF = CovidDF.sort_values(["covid_facts_date"], ascending=[True])
CovidDF["metrics.icuCapacityRatioRolling10"] = CovidDF["metrics.icuCapacityRatio"].rolling(10, min_periods=1, center=True, closed='both').mean()
CovidDF["metrics.bedsWithCovidPatientsRatioRolling10"] = CovidDF["metrics.bedsWithCovidPatientsRatio"].rolling(10, min_periods=1, center=True, closed='both').mean()
CovidDF["metrics.weeklyCovidAdmissionsPer100kRolling10"] = CovidDF["metrics.weeklyCovidAdmissionsPer100k"].rolling(10, min_periods=1, center=True, closed='both').mean()

# Add a rolling average for daily deaths.

CovidDF = CovidDF.sort_values(["covid_facts_date"], ascending=[True])
CovidDF["metrics.newDeathsRolling7"] = CovidDF["actuals.newDeaths"].rolling(7, min_periods=1, center=True, closed='both').mean()

# Create the DF for whole USA Flourish chart. Tweak as needed.

UsaChartDF = BiobotWeeklyUsaDF
UsaChartDF = UsaChartDF.rename(columns={"sampling_week":"week", "effective_concentration_rolling_average":"gene_copies"})

# Create the DF for regional USA Flourish chart. Tweak as needed.

RegionsChartDF = BiobotWeeklyRegionsDF
RegionsChartDF = RegionsChartDF.rename(columns={"sampling_week":"week", "effective_concentration_rolling_average":"gene_copies"})

RegionsChartDF.loc[RegionsChartDF["region"] == "Midwest", "Midwest"] = RegionsChartDF["gene_copies"]
RegionsChartDF.loc[RegionsChartDF["region"] == "Northeast", "Northeast"] = RegionsChartDF["gene_copies"]
RegionsChartDF.loc[RegionsChartDF["region"] == "South", "South"] = RegionsChartDF["gene_copies"]
RegionsChartDF.loc[RegionsChartDF["region"] == "West", "West"] = RegionsChartDF["gene_copies"]
    
# Write out the map data files. 

print ("\nWriting map data to " + BIOBOT_USA_CHART_DATA + " and " + BIOBOT_REGIONS_CHART_DATA)
UsaChartDF.to_csv(BIOBOT_USA_CHART_DATA, encoding='utf-8', sep='\t', index=False)
RegionsChartDF.to_csv(BIOBOT_REGIONS_CHART_DATA, encoding='utf-8', sep='\t', index=False)

