
# Do some analysis of Biobot wastewater data vs Covid outcome facts.

import pandas as pd 
from urllib import request

BIOBOT_DOWNLOAD = "https://github.com/biobotanalytics/covid19-wastewater-data/raw/master/wastewater_by_region.csv"
BIOBOT_LOCAL = "/Users/chuck/Desktop/COVID Programming/Biobot/wastewater_by_region.csv"
COVID_ACT_NOW_DOWNLOAD = "https://api.covidactnow.org/v2/country/US.timeseries.csv?apiKey=402c0523d9e64d4fb62f37fbf499bf7b"
COVID_ACT_NOW_LOCAL = "/Users/chuck/Desktop/COVID Programming/Covid Act Now/US.timeseries.csv"

HOSP_LOOK_AHEAD = 14     # how far ahead of sample do we look for hospitalization info
DEATHS_LOOK_AHEAD = 21     # how far ahead of sample do we look for mortality info

BIOBOT_USA_CHART_DATA = "biobot_vs_outcomes_usa.tsv"

# Get the latest data from Biobot, and tweak as we need it.

request.urlretrieve(BIOBOT_DOWNLOAD, BIOBOT_LOCAL)
BiobotDF = pd.read_csv(BIOBOT_LOCAL, sep=',', header='infer', dtype=str)

BiobotDF["sampling_week"] = pd.to_datetime(BiobotDF["sampling_week"], errors='coerce')
BiobotDF["effective_concentration_rolling_average"] = pd.to_numeric(BiobotDF["effective_concentration_rolling_average"], errors='coerce')

# Get overall USA COVID outcomes. Tweak as needed.

request.urlretrieve(COVID_ACT_NOW_DOWNLOAD, COVID_ACT_NOW_LOCAL)
CovidDF = pd.read_csv(COVID_ACT_NOW_LOCAL, sep=',', header='infer', dtype=str)

CovidDF = CovidDF[["date", "actuals.hospitalBeds.weeklyCovidAdmissions", "actuals.hospitalBeds.currentUsageCovid", "actuals.icuBeds.currentUsageCovid", "actuals.newDeaths"]]
CovidDF = CovidDF.rename(columns={"date":"covid_facts_date"})
CovidDF["covid_facts_date"] = pd.to_datetime(CovidDF["covid_facts_date"], errors='coerce')

# Add a rolling average for some key hospitalization info. It is only reported weekly, so there are many empty days now.
# The goal is to fill in the missing days with reasonable numbers, so downstream there is something there.

CovidDF = CovidDF.sort_values(["covid_facts_date"], ascending=[True])
CovidDF["admits_rolling10"] = (CovidDF["actuals.hospitalBeds.weeklyCovidAdmissions"].rolling(10, min_periods=1, center=True, closed='both').mean() )
CovidDF["beds_rolling10"] = (CovidDF["actuals.hospitalBeds.currentUsageCovid"].rolling(10, min_periods=1, center=True, closed='both').mean() )
CovidDF["icu_rolling10"] = (CovidDF["actuals.icuBeds.currentUsageCovid"].rolling(10, min_periods=1, center=True, closed='both').mean() )

# Deaths are reported every day, but smooth them out.

CovidDF["deaths_rolling10"] = CovidDF["actuals.newDeaths"].rolling(10, min_periods=1, center=True, closed='both').mean()

# Create the DF for whole USA Flourish chart, by merging Biobot with Covid Act Now. Tweak as needed.

UsaDF = BiobotDF.query("region=='Nationwide'")  # don't need the regional data
UsaDF = UsaDF.merge(CovidDF, how='left', left_on="sampling_week", right_on="covid_facts_date")
UsaDF = UsaDF.drop(columns=["covid_facts_date"])
UsaDF = UsaDF.rename(columns={"sampling_week":"week", "effective_concentration_rolling_average":"copies_ml"})
UsaDF = UsaDF.query("week >= 20200401")   # not much useful data before this

# Write out the chart data file. 

print ("\nWriting chart data to " + BIOBOT_USA_CHART_DATA)
UsaDF.to_csv(BIOBOT_USA_CHART_DATA, encoding='utf-8', sep='\t', index=False)

# Add some look-ahead dates to the data, so we can look up covid outcomes AFTER the water test dates.

UsaDF["hosp_date"] = UsaDF["week"] +  pd.offsets.Day(HOSP_LOOK_AHEAD)
UsaDF["deaths_date"] = UsaDF["week"] +  pd.offsets.Day(DEATHS_LOOK_AHEAD)

# Create DFs with just the hospitalization info and deaths info. We will use these for "lookahead" info.

HospDF = CovidDF[["covid_facts_date","admits_rolling10","beds_rolling10","icu_rolling10"]]
HospDF = HospDF.rename(columns={"admits_rolling10":"admits_later", "beds_rolling10":"beds_later", "icu_rolling10":"icu_later"})

DeathsDF = CovidDF[["covid_facts_date", "deaths_rolling10"]]
DeathsDF = DeathsDF.rename(columns={"deaths_rolling10":"deaths_later"})

# Join these look-ahead facts with the main DF

UsaDF = UsaDF.merge(HospDF, how='left', left_on="hosp_date", right_on="covid_facts_date")
UsaDF = UsaDF.drop(columns=["covid_facts_date"])

UsaDF = UsaDF.merge(DeathsDF, how='left', left_on="deaths_date", right_on="covid_facts_date")
UsaDF = UsaDF.drop(columns=["covid_facts_date"])

# TODO Look at RNA vs test positivity

#RawDF.plot.scatter(x="pcr_target_avg_conc_norm", y="metrics.testPositivityRatio")
#print ("\nRNA corr test positive ratio: " + str(RawDF["pcr_target_avg_conc_norm"].corr(RawDF["metrics.testPositivityRatio"], method="spearman").round(3)))

