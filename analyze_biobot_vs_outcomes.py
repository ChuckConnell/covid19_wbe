
# Do some analysis of Biobot wastewater data vs Covid outcome facts.

import pandas as pd 
import numpy as np
from urllib import request

BIOBOT_DOWNLOAD = "https://github.com/biobotanalytics/covid19-wastewater-data/raw/master/wastewater_by_region.csv"
BIOBOT_LOCAL = "/Users/chuck/Desktop/COVID Programming/Biobot/wastewater_by_region.csv"
COVID_ACT_NOW_DOWNLOAD = "https://api.covidactnow.org/v2/country/US.timeseries.csv?apiKey=402c0523d9e64d4fb62f37fbf499bf7b"
COVID_ACT_NOW_LOCAL = "/Users/chuck/Desktop/COVID Programming/Covid Act Now/US.timeseries.csv"

VAX_LOOK_BACK = 10     # how far back from RNA data do we look for vax info
HOSP_LOOK_AHEAD = 14     # how far ahead of RNA data do we look for hospitalization info
DEATHS_LOOK_AHEAD = 28    # how far ahead of RNA data do we look for mortality info

RNA_TOP_COMPRESSION = 0.90  # to help assign percentile rank to RNA levels
INFECTED_NOW = 0.90   # Portion of US pop that has gotten a natural infection of covid

VIRUS_START = "2020-03-01"  # for various date calculations
VAX_START = "2020-12-13"
BOOST_START = "2021-08-15"

BIOBOT_USA_CHART_DATA = "biobot_vs_outcomes_usa.tsv"

# Calc overall months since start of the pandemic.

pandemic_weeks = int(round(((pd.Timestamp.today() - pd.Timestamp(VIRUS_START)) / np.timedelta64(1, 'W'))))

# Get the latest data from Biobot, and tweak as we need it.

request.urlretrieve(BIOBOT_DOWNLOAD, BIOBOT_LOCAL)
BiobotDF = pd.read_csv(BIOBOT_LOCAL, sep=',', header='infer', dtype=str)

BiobotDF["sampling_week"] = pd.to_datetime(BiobotDF["sampling_week"], errors='coerce')
BiobotDF["effective_concentration_rolling_average"] = pd.to_numeric(BiobotDF["effective_concentration_rolling_average"], errors='coerce')

# Normalize the RNA signal so that it is out of 100. This requires compressing all the very high signals to 100.

top_rna = BiobotDF["effective_concentration_rolling_average"].quantile(RNA_TOP_COMPRESSION)
BiobotDF["rna_signal_pct"] = (BiobotDF["effective_concentration_rolling_average"] / top_rna) * 100
BiobotDF.loc[BiobotDF["rna_signal_pct"] > 100, "rna_signal_pct"] = 100

# Get overall USA COVID outcomes. Tweak as needed.

request.urlretrieve(COVID_ACT_NOW_DOWNLOAD, COVID_ACT_NOW_LOCAL)
CovidDF = pd.read_csv(COVID_ACT_NOW_LOCAL, sep=',', header='infer', dtype=str)

CovidDF = CovidDF[["date", "actuals.hospitalBeds.weeklyCovidAdmissions", "actuals.hospitalBeds.currentUsageCovid", "actuals.icuBeds.currentUsageCovid", "actuals.newDeaths", "metrics.vaccinationsInitiatedRatio", "metrics.vaccinationsCompletedRatio", "metrics.vaccinationsAdditionalDoseRatio"]]
CovidDF = CovidDF.rename(columns={"date":"covid_facts_date"})
CovidDF["covid_facts_date"] = pd.to_datetime(CovidDF["covid_facts_date"], errors='coerce')
CovidDF["metrics.vaccinationsInitiatedRatio"] = pd.to_numeric(CovidDF["metrics.vaccinationsInitiatedRatio"], errors='coerce')
CovidDF["metrics.vaccinationsCompletedRatio"] = pd.to_numeric(CovidDF["metrics.vaccinationsCompletedRatio"], errors='coerce')
CovidDF["metrics.vaccinationsAdditionalDoseRatio"] = pd.to_numeric(CovidDF["metrics.vaccinationsAdditionalDoseRatio"], errors='coerce')

# Add a rolling average for some key hospitalization info. It is only reported weekly, so there are many empty days now.
# The goal is to fill in the missing days with reasonable numbers, so downstream there is something there.

CovidDF = CovidDF.sort_values(["covid_facts_date"], ascending=[True])
CovidDF["admits_rolling10"] = (CovidDF["actuals.hospitalBeds.weeklyCovidAdmissions"].rolling(10, min_periods=1, center=True, closed='both').mean() )
CovidDF["beds_rolling10"] = (CovidDF["actuals.hospitalBeds.currentUsageCovid"].rolling(10, min_periods=1, center=True, closed='both').mean() )
CovidDF["icu_rolling10"] = (CovidDF["actuals.icuBeds.currentUsageCovid"].rolling(10, min_periods=1, center=True, closed='both').mean() )

# Deaths are reported every day, but smooth them out.

CovidDF["deaths_rolling5"] = CovidDF["actuals.newDeaths"].rolling(5, min_periods=1, center=True, closed='both').mean()

# All vax numbers in USA before the first vax jab are zero even if they are missing in the CovidActNow file. Downstream we want a zero, not NAN.
# All boost numbers before booster start are also zero.

CovidDF.loc[CovidDF["covid_facts_date"] <= VAX_START, "metrics.vaccinationsInitiatedRatio"] = 0.0
CovidDF.loc[CovidDF["covid_facts_date"] <= VAX_START, "metrics.vaccinationsCompletedRatio"] = 0.0

CovidDF.loc[CovidDF["covid_facts_date"] <= BOOST_START, "metrics.vaccinationsAdditionalDoseRatio"] = 0.0

# Add new columns that show unvaxed as a percent, rather than vaxed ratio. This might help downstream with some analysis.

CovidDF["not_one_vax_pct"] = (1.0 - CovidDF["metrics.vaccinationsInitiatedRatio"]) * 100 
CovidDF["not_full_vax_pct"] = (1.0 - CovidDF["metrics.vaccinationsCompletedRatio"]) * 100 
CovidDF["not_boost_vax_pct"] = (1.0 - CovidDF["metrics.vaccinationsAdditionalDoseRatio"]) * 100 

# Create the starting DF for Flourish chart. Tweak as needed.

UsaDF = BiobotDF.query("region=='Nationwide'")  # don't need the regional data
UsaDF = UsaDF.rename(columns={"sampling_week":"week", "effective_concentration_rolling_average":"copies_ml"})
UsaDF = UsaDF.query("week >= 20200401")   # not much useful data before this

# Add some look-back and look-ahead dates to the data, so we can look up vax status BEFORE the water tests and covid outcomes AFTER the water test dates.

UsaDF["vax_date"] = UsaDF["week"] -  pd.offsets.Day(VAX_LOOK_BACK)

UsaDF["hosp_date"] = UsaDF["week"] +  pd.offsets.Day(HOSP_LOOK_AHEAD)
UsaDF["deaths_date"] = UsaDF["week"] +  pd.offsets.Day(DEATHS_LOOK_AHEAD)

# Make a column that estimates the % of people who have not been infected.
# The assumption is that the number of people uninfected in the US is linear, from March 2020 until today. This is mostly true.

UsaDF['pandemic_week'] = ((UsaDF.week - pd.Timestamp(VIRUS_START)) / np.timedelta64(1, 'W')).round().astype(int) 
UsaDF["infected_pct"] = ((UsaDF['pandemic_week'] / pandemic_weeks) * 100 * INFECTED_NOW).round(2)
UsaDF["not_infected_pct"] = 100 - UsaDF["infected_pct"] 
UsaDF = UsaDF.drop(columns=["pandemic_week", "infected_pct"])

# Create DFs with just the vax, hospitalization and deaths info. We will use these for look-back and look-ahead info.

HospDF = CovidDF[["covid_facts_date","admits_rolling10","beds_rolling10","icu_rolling10"]]
HospDF = HospDF.rename(columns={"admits_rolling10":"admits_later", "beds_rolling10":"beds_later", "icu_rolling10":"icu_later"})

DeathsDF = CovidDF[["covid_facts_date", "deaths_rolling5"]]
DeathsDF = DeathsDF.rename(columns={"deaths_rolling5":"deaths_later"})

VaxDF = CovidDF[["covid_facts_date", "not_one_vax_pct", "not_full_vax_pct", "not_boost_vax_pct"]]
VaxDF = VaxDF.rename(columns={"not_one_vax_pct":"not_one_vax_earlier", "not_full_vax_pct":"not_full_vax_earlier", "not_boost_vax_pct":"not_boost_vax_earlier"})

# Join these look-ahead and look-back facts with the main DF. 
# Must use inner join for outcomes because recent water test dates do not yet have any matching outcome data.

UsaDF = UsaDF.merge(VaxDF, how='left', left_on="vax_date", right_on="covid_facts_date")
UsaDF = UsaDF.drop(columns=["covid_facts_date"])

UsaDF = UsaDF.merge(HospDF, how='inner', left_on="hosp_date", right_on="covid_facts_date")
UsaDF = UsaDF.drop(columns=["covid_facts_date"])

UsaDF = UsaDF.merge(DeathsDF, how='inner', left_on="deaths_date", right_on="covid_facts_date")
UsaDF = UsaDF.drop(columns=["covid_facts_date"])

# Make the UPR metrics -- Unvaxed Plus Rna. 
# Worst case is 200 = 100% unvaxed + 100% of the found RNA levels.

#UsaDF["UPR_one_vax"] = (UsaDF["not_one_vax_earlier"] + UsaDF["rna_signal_pct"]).round(2)
#UsaDF["UPR_full_vax"] = (UsaDF["not_full_vax_earlier"] + UsaDF["rna_signal_pct"]).round(2)
UsaDF["UPR_boost_vax"] = (UsaDF["not_boost_vax_earlier"] + UsaDF["rna_signal_pct"]).round(2)

# Make NPR metric -- % Not Infected Plus Rna. This ignores vaccination.  
# So worst case is 200 = 100% not infected + 100% of the found RNA levels. 

UsaDF["NPR"] = (UsaDF["not_infected_pct"] + UsaDF["rna_signal_pct"]).round(2)

# Final dataset info

print ("\nThe final dataset has " + str(UsaDF.shape[0]) + " rows from " + str(UsaDF["week"].min().date()) + " to " + str(UsaDF["week"].max().date()) )
print ("\nVaccination look back = " + str(VAX_LOOK_BACK))
print ("\nHospitalization look ahead = " + str(HOSP_LOOK_AHEAD))
print ("\nMortality look ahead = " + str(DEATHS_LOOK_AHEAD))

#### Plain wastewater RNA levels

# Look at RNA vs hospital admissions

UsaDF.plot.scatter(x="copies_ml", y="admits_later")
print ("\nRNA corr later hospital admits: " + str(UsaDF["copies_ml"].corr(UsaDF["admits_later"], method="spearman").round(3)))

# Look at RNA vs hospital beds

UsaDF.plot.scatter(x="copies_ml", y="beds_later")
print ("\nRNA corr later hospital beds: " + str(UsaDF["copies_ml"].corr(UsaDF["beds_later"], method="spearman").round(3)))

# Look at RNA vs ICU

UsaDF.plot.scatter(x="copies_ml", y="icu_later")
print ("\nRNA corr later ICU: " + str(UsaDF["copies_ml"].corr(UsaDF["icu_later"], method="spearman").round(3)))

# Look at RNA vs deaths

UsaDF.plot.scatter(x="copies_ml", y="deaths_later")
print ("\nRNA corr later deaths: " + str(UsaDF["copies_ml"].corr(UsaDF["deaths_later"], method="spearman").round(3)))

#### My UPR, which is wastewater + vax

# Look at UPR vs hospital admissions

#UsaDF.plot.scatter(x="UPR_one_vax", y="admits_later")
#print ("\nUPR 1 corr later hospital admits: " + str(UsaDF["UPR_one_vax"].corr(UsaDF["admits_later"], method="spearman").round(3)))

#UsaDF.plot.scatter(x="UPR_full_vax", y="admits_later")
#print ("\nUPR Full corr later hospital admits: " + str(UsaDF["UPR_full_vax"].corr(UsaDF["admits_later"], method="spearman").round(3)))

UsaDF.plot.scatter(x="UPR_boost_vax", y="admits_later")
print ("\nUPR Boost corr later hospital admits: " + str(UsaDF["UPR_boost_vax"].corr(UsaDF["admits_later"], method="spearman").round(3)))

# Look at UPR vs hospital beds

#UsaDF.plot.scatter(x="UPR_one_vax", y="beds_later")
#print ("\nUPR 1 corr later hospital beds: " + str(UsaDF["UPR_one_vax"].corr(UsaDF["beds_later"], method="spearman").round(3)))

#UsaDF.plot.scatter(x="UPR_full_vax", y="beds_later")
#print ("\nUPR Full corr later hospital beds: " + str(UsaDF["UPR_full_vax"].corr(UsaDF["beds_later"], method="spearman").round(3)))

UsaDF.plot.scatter(x="UPR_boost_vax", y="beds_later")
print ("\nUPR Boost corr later hospital beds: " + str(UsaDF["UPR_boost_vax"].corr(UsaDF["beds_later"], method="spearman").round(3)))

# Look at UPR vs ICU beds

#UsaDF.plot.scatter(x="UPR_one_vax", y="icu_later")
#print ("\nUPR 1 corr later ICU: " + str(UsaDF["UPR_one_vax"].corr(UsaDF["icu_later"], method="spearman").round(3)))

#UsaDF.plot.scatter(x="UPR_full_vax", y="icu_later")
#print ("\nUPR Full corr later ICU: " + str(UsaDF["UPR_full_vax"].corr(UsaDF["icu_later"], method="spearman").round(3)))

UsaDF.plot.scatter(x="UPR_boost_vax", y="icu_later")
print ("\nUPR Boost corr later ICU: " + str(UsaDF["UPR_boost_vax"].corr(UsaDF["icu_later"], method="spearman").round(3)))

# Look at UPR vs deaths

#UsaDF.plot.scatter(x="UPR_one_vax", y="deaths_later")
#print ("\nUPR 1 corr later deaths: " + str(UsaDF["UPR_one_vax"].corr(UsaDF["deaths_later"], method="spearman").round(3)))

#UsaDF.plot.scatter(x="UPR_full_vax", y="deaths_later")
#print ("\nUPR Full corr later deaths: " + str(UsaDF["UPR_full_vax"].corr(UsaDF["deaths_later"], method="spearman").round(3)))

UsaDF.plot.scatter(x="UPR_boost_vax", y="deaths_later")
print ("\nUPR Boost corr later deaths: " + str(UsaDF["UPR_boost_vax"].corr(UsaDF["deaths_later"], method="spearman").round(3)))

###### My NPR, which is wastewater + not infected

# Look at NPR vs hospital admissions

UsaDF.plot.scatter(x="NPR", y="admits_later")
print ("\nNPR corr later hospital admits: " + str(UsaDF["NPR"].corr(UsaDF["admits_later"], method="spearman").round(3)))

# Look at NPR vs hospital beds

UsaDF.plot.scatter(x="NPR", y="beds_later")
print ("\nNPR corr later hospital beds: " + str(UsaDF["NPR"].corr(UsaDF["beds_later"], method="spearman").round(3)))

# Look at NPR vs ICU beds

UsaDF.plot.scatter(x="NPR", y="icu_later")
print ("\nNPR corr later ICU: " + str(UsaDF["NPR"].corr(UsaDF["icu_later"], method="spearman").round(3)))

# Look at NPR vs deaths

UsaDF.plot.scatter(x="NPR", y="deaths_later")
print ("\nNPR corr later deaths: " + str(UsaDF["NPR"].corr(UsaDF["deaths_later"], method="spearman").round(3)))

# Write out the chart data file. 

print ("\nWriting chart data to " + BIOBOT_USA_CHART_DATA)
UsaDF.to_csv(BIOBOT_USA_CHART_DATA, encoding='utf-8', sep='\t', index=False)

