
# Take the full CDC NWSS datasets and add fields for vax status, cases, hospitalizations and deaths,
# in that location at that time. But be smart about what dates we use for vax, death, etc since these
# dates need a setback or set-forward to be meaningful to the wastewater signal.

import pandas as pd 

VAX_LOOK_BACK = 10     # how far back from sample date do we look for vax info
CASES_LOOK_AHEAD = 7     # how far ahead of sample do we look for case info
HOSP_LOOK_AHEAD = 14     # how far ahead of sample do we look for hospitalization info
DEATHS_LOOK_AHEAD = 21     # how far ahead of sample do we look for mortality info
RAW_OUTPUT_FILE = "NwssRawEnhanced.tsv"
ANALYTIC_OUTPUT_FILE = "NwssAnalyticEnhanced.tsv" 
RAW_OUTPUT_SAMPLE = "NwssRawEnhancedSample.tsv"
ANALYTIC_OUTPUT_SAMPLE = "NwssAnalyticEnhancedSample.tsv"
SAMPLE_SIZE = 1000

# TODO Get a fresh NWSS update. This is 2 months old.

# Wastewater samples raw data and analytics data. Acquired by restricted download from CDC, after signing data-use agreement.

path = "~/Desktop/COVID Programming/CDC/cdc-nwss-restricted-data-set-wastewater-2022-02-08.csv"
RawDF = pd.read_csv(path, sep=',', header='infer', dtype=str)

path = "~/Desktop/COVID Programming/CDC/cdc-nwss-restricted-data-set-final-2022-02-08.csv"
AnalyticDF = pd.read_csv(path, sep=',', header='infer', dtype=str)

# Covid facts per county from https://apidocs.covidactnow.org. You need an API key, which they give to anyone who asks.

path = "~/Desktop/COVID Programming/Covid Act Now/counties.timeseries.20apr2022.csv"
CovidDF = pd.read_csv(path, sep=',', header='infer', dtype=str)

# BostonDF = CovidDF.query("fips =='25025' ")   # debugging

# The CovidActNow data seems pretty good. But it is a secondary source, so we have to trust that it is compiled correctly.
# If we want to skip that and go back upstream, we could replace CovidActNow with these sources...
# Vax  https://data.cdc.gov/Vaccinations/COVID-19-Vaccinations-in-the-United-States-County/8xkx-amqh
# Hospitalizations  https://healthdata.gov/Hospital/COVID-19-Reported-Patient-Impact-and-Hospital-Capa/anag-cw7u
# Deaths, also secondary but looks good, https://github.com/nytimes/covid-19-data/blob/master/us-counties.csv

# Population by county
# From https://www.census.gov/programs-surveys/popest/technical-documentation/research/evaluation-estimates/2020-evaluation-estimates/2010s-counties-total.html  

path = "~/Desktop/COVID Programming/US Census/co-est2020.csv"
PopDF = pd.read_csv(path, sep=',', header='infer', dtype=str, encoding='latin-1')

# In the "Covid facts file" clarify some column names.
                                                            
CovidDF = CovidDF.rename(columns={"metrics.caseDensity":"metrics.caseDensity100k", "date":"covid_facts_date"})
CovidDF["covid_facts_date"] = pd.to_datetime(CovidDF["covid_facts_date"], errors='coerce')

# Add a rolling average for some key hospitalization info. It is only reported weekly, so there are many empty days now.
# The goal is to fill in the missing days with reasonable numbers, so downstream there is something there.

CovidDF = CovidDF.sort_values(["fips", "covid_facts_date"], ascending=[True, True])
CovidDF["metrics.icuCapacityRatioRolling10"] = CovidDF["metrics.icuCapacityRatio"].rolling(10, min_periods=1, center=True, closed='both').mean()
CovidDF["metrics.bedsWithCovidPatientsRatioRolling10"] = CovidDF["metrics.bedsWithCovidPatientsRatio"].rolling(10, min_periods=1, center=True, closed='both').mean()
CovidDF["metrics.weeklyCovidAdmissionsPer100kRolling10"] = CovidDF["metrics.weeklyCovidAdmissionsPer100k"].rolling(10, min_periods=1, center=True, closed='both').mean()

# Add a rolling average for daily deaths.

CovidDF = CovidDF.sort_values(["fips", "covid_facts_date"], ascending=[True, True])
CovidDF["metrics.newDeathsRolling7"] = CovidDF["actuals.newDeaths"].rolling(7, min_periods=1, center=True, closed='both').mean()

# All vax numbers in USA before 2020-12-13 (date of first vax jab) are zero even if they are missing in the CovidActNow file. Downstream we want a zero, not NAN.
# All "addditional dose" numbers before 2021-08-15 are zero.

CovidDF.loc[CovidDF["covid_facts_date"] <= "2020-12-13", "actuals.vaccinationsInitiated"] = 0.0
CovidDF.loc[CovidDF["covid_facts_date"] <= "2020-12-13", "actuals.vaccinationsCompleted"] = 0.0
CovidDF.loc[CovidDF["covid_facts_date"] <= "2020-12-13", "metrics.vaccinationsInitiatedRatio"] = 0.0
CovidDF.loc[CovidDF["covid_facts_date"] <= "2020-12-13", "metrics.vaccinationsCompletedRatio"] = 0.0

CovidDF.loc[CovidDF["covid_facts_date"] <= "2021-08-15", "actuals.vaccinationsAdditionalDose"] = 0.0
CovidDF.loc[CovidDF["covid_facts_date"] <= "2021-08-15", "metrics.vaccinationsAdditionalDoseRatio"] = 0.0

# testDF = CovidDF.query("fips == '25025' or fips == '09001' ")  # debugging

# Make a few changes to the raw wastewater samples file.

RawDF = RawDF.rename(columns={"county_names": "CountyFIPS", "population_served": "sewershed_population_served"})

RawDF["sample_collect_date"] = pd.to_datetime(RawDF["sample_collect_date"], errors='coerce')
RawDF = RawDF[RawDF["sample_collect_date"].notna()]   # get rid of bad/missing sample dates

RawDF["sewershed_population_served"] = pd.to_numeric(RawDF["sewershed_population_served"], errors='coerce').fillna(0).astype(int)

RawDF["CountyFIPS"] = RawDF["CountyFIPS"].str.split(",")    # change comma separate string to array
RawDF = RawDF.explode("CountyFIPS")          # make one row per county
RawDF["CountyFIPS"] = RawDF["CountyFIPS"].str.strip("[]' ")   # clean up, one pure FIPS per row

# Make a few changes to the analytic wastewater samples file.

AnalyticDF = AnalyticDF[AnalyticDF["sample_id"].str.strip().str.len() > 0]   # If no sample_id, this is not a real water sample, just a row to hold covid case counts on nearby dates. We are going to add our own case data.

AnalyticDF = AnalyticDF.rename(columns={"county_names": "CountyFIPS", "date": "sample_collect_date", "population_served": "sewershed_population_served"})

AnalyticDF["sample_collect_date"] = pd.to_datetime(AnalyticDF["sample_collect_date"], errors='coerce')
AnalyticDF = AnalyticDF[AnalyticDF["sample_collect_date"].notna()]   # get rid of bad/missing sample dates

AnalyticDF["sewershed_population_served"] = pd.to_numeric(AnalyticDF["sewershed_population_served"], errors='coerce').fillna(0).astype(int)

AnalyticDF["CountyFIPS"] = AnalyticDF["CountyFIPS"].str.split(",")   # one county per row
AnalyticDF = AnalyticDF.explode("CountyFIPS")    
AnalyticDF["CountyFIPS"] = AnalyticDF["CountyFIPS"].str.strip("[]' ")

# Make a few changes to the population file.

PopDF = PopDF.rename(columns={"STATE":"StateFIPS", "COUNTY":"CountyFIPS-3", "POPESTIMATE2020":"COUNTY_POPESTIMATE2020"})  
PopDF["CountyFIPS"] = PopDF["StateFIPS"] + PopDF["CountyFIPS-3"]  # make full 5-digit county FIPS
PopDF["COUNTY_POPESTIMATE2020"] = pd.to_numeric(PopDF["COUNTY_POPESTIMATE2020"], errors='coerce')
PopDF = PopDF[PopDF["COUNTY_POPESTIMATE2020"].notna()]   # throw out bad rows
PopDF = PopDF[["CountyFIPS", "COUNTY_POPESTIMATE2020"]]  # just the fields we need

# Add county population to each sample row. 
# Numbers get changed to float during merge, so fix back to integer.

RawDF = RawDF.merge(PopDF, how='left', on="CountyFIPS")
RawDF["COUNTY_POPESTIMATE2020"] = pd.to_numeric(RawDF["COUNTY_POPESTIMATE2020"], errors='coerce')

AnalyticDF = AnalyticDF.merge(PopDF, how='left', on="CountyFIPS")
AnalyticDF["COUNTY_POPESTIMATE2020"] = pd.to_numeric(AnalyticDF["COUNTY_POPESTIMATE2020"], errors='coerce')

# Create some additional date columns on the sample files. These will be used to add the 
# look ahead / look back info.

RawDF["vax_date"] = RawDF["sample_collect_date"] -  pd.offsets.Day(VAX_LOOK_BACK)
RawDF["cases_date"] = RawDF["sample_collect_date"] + pd.offsets.Day(CASES_LOOK_AHEAD)
RawDF["hosp_date"] = RawDF["sample_collect_date"] +  pd.offsets.Day(HOSP_LOOK_AHEAD)
RawDF["deaths_date"] = RawDF["sample_collect_date"] +  pd.offsets.Day(DEATHS_LOOK_AHEAD)

AnalyticDF["vax_date"] = AnalyticDF["sample_collect_date"] -  pd.offsets.Day(VAX_LOOK_BACK)
AnalyticDF["cases_date"] = AnalyticDF["sample_collect_date"] + pd.offsets.Day(CASES_LOOK_AHEAD)
AnalyticDF["hosp_date"] = AnalyticDF["sample_collect_date"] +  pd.offsets.Day(HOSP_LOOK_AHEAD)
AnalyticDF["deaths_date"] = AnalyticDF["sample_collect_date"] +  pd.offsets.Day(DEATHS_LOOK_AHEAD)

# Make a subset of just the vax facts we want and join it to the samples.
# Note that we join the vax info from a previous date onto each sample.

VaxDF = CovidDF[["covid_facts_date", "fips", "actuals.vaccinationsInitiated", "actuals.vaccinationsCompleted", "actuals.vaccinationsAdditionalDose", "metrics.vaccinationsInitiatedRatio", "metrics.vaccinationsCompletedRatio", "metrics.vaccinationsAdditionalDoseRatio"]]

RawDF = RawDF.merge(VaxDF, how='left', left_on=["vax_date", "CountyFIPS"], right_on=["covid_facts_date", "fips"])
RawDF = RawDF.drop(columns=["covid_facts_date", "fips"])

AnalyticDF = AnalyticDF.merge(VaxDF, how='left', left_on=["vax_date", "CountyFIPS"], right_on=["covid_facts_date", "fips"])
AnalyticDF = AnalyticDF.drop(columns=["covid_facts_date", "fips"])

# Make a subset of just the covid case facts we want and join it to the samples.
# Note that we join the case info from a future date onto each sample.

CasesDF = CovidDF[["covid_facts_date", "fips", "actuals.newCases", "metrics.caseDensity100k", "metrics.infectionRate", "metrics.testPositivityRatio"]]

RawDF = RawDF.merge(CasesDF, how='left', left_on=["cases_date", "CountyFIPS"], right_on=["covid_facts_date", "fips"])
RawDF = RawDF.drop(columns=["covid_facts_date", "fips"])

AnalyticDF = AnalyticDF.merge(CasesDF, how='left', left_on=["cases_date", "CountyFIPS"], right_on=["covid_facts_date", "fips"])
AnalyticDF = AnalyticDF.drop(columns=["covid_facts_date", "fips"])

# Add hospitialization facts to the water files.

HospDF = CovidDF[["covid_facts_date", "fips", "actuals.hospitalBeds.capacity", "actuals.icuBeds.capacity", "actuals.hospitalBeds.currentUsageCovid", "actuals.icuBeds.currentUsageCovid", "metrics.icuCapacityRatio", "metrics.bedsWithCovidPatientsRatio" , "metrics.bedsWithCovidPatientsRatioRolling10" ,"metrics.weeklyCovidAdmissionsPer100k" , "metrics.weeklyCovidAdmissionsPer100kRolling10", "metrics.icuCapacityRatioRolling10" ]]

RawDF = RawDF.merge(HospDF, how='left', left_on=["hosp_date", "CountyFIPS"], right_on=["covid_facts_date", "fips"])
RawDF = RawDF.drop(columns=["covid_facts_date", "fips"])

AnalyticDF = AnalyticDF.merge(HospDF, how='left', left_on=["hosp_date", "CountyFIPS"], right_on=["covid_facts_date", "fips"])
AnalyticDF = AnalyticDF.drop(columns=["covid_facts_date", "fips"])

# Add mortality info

DeathsDF = CovidDF[["covid_facts_date", "fips", "actuals.newDeaths", "metrics.newDeathsRolling7"]]

RawDF = RawDF.merge(DeathsDF, how='left', left_on=["deaths_date", "CountyFIPS"], right_on=["covid_facts_date", "fips"])
RawDF = RawDF.drop(columns=["covid_facts_date", "fips"])

AnalyticDF = AnalyticDF.merge(DeathsDF, how='left', left_on=["deaths_date", "CountyFIPS"], right_on=["covid_facts_date", "fips"])
AnalyticDF = AnalyticDF.drop(columns=["covid_facts_date", "fips"])

# Write to files.

print ("\nWriting enhanced raw wastewater data to", RAW_OUTPUT_FILE, "with", RawDF.shape[0], "rows.\n")
RawDF.to_csv(RAW_OUTPUT_FILE, encoding='utf-8', sep='\t', index=False)

print ("Writing enhanced analytic wastewater data to", ANALYTIC_OUTPUT_FILE, "with", AnalyticDF.shape[0], "rows.\n")
AnalyticDF.to_csv(ANALYTIC_OUTPUT_FILE, encoding='utf-8', sep='\t', index=False)

# Create and write anonymized sample files. The rules for anonymization are in the data-use agreement.

RawSampleDF = RawDF.sample(n=SAMPLE_SIZE)
RawSampleDF["sample_location"] = "XXXX"
RawSampleDF["sample_location_specify"] = "XXXX"
RawSampleDF["wwtp_name"] = "XXXX"
RawSampleDF["lab_id"] = "XXXX"
RawSampleDF["sample_id"] = "XXXX"
RawSampleDF = RawSampleDF.query("sewershed_population_served > 3000")

AnalyticSampleDF = AnalyticDF.sample(n=SAMPLE_SIZE)
AnalyticSampleDF["sample_location"] = "XXXX"
AnalyticSampleDF["sample_location_specify"] = "XXXX"
AnalyticSampleDF["wwtp_name"] = "XXXX"
AnalyticSampleDF["lab_id"] = "XXXX"
AnalyticSampleDF["sample_id"] = "XXXX"
AnalyticSampleDF = AnalyticSampleDF.query("sewershed_population_served > 3000")

print ("\nWriting anonymized sample of enhanced raw wastewater data to", RAW_OUTPUT_SAMPLE, "with", RawSampleDF.shape[0], "rows.\n")
RawSampleDF.to_csv(RAW_OUTPUT_SAMPLE, encoding='utf-8', sep='\t', index=False)

print ("Writing anonymized sample of enhanced analytic wastewater data to", ANALYTIC_OUTPUT_SAMPLE, "with", AnalyticSampleDF.shape[0], "rows.\n")
AnalyticSampleDF.to_csv(ANALYTIC_OUTPUT_SAMPLE, encoding='utf-8', sep='\t', index=False)







