
# Take the full CDC NWSS dataset and add fields for vax status, cases, hospitalizations and deaths,
# in that location at that time. But be smart about what dates we use for vax, death, etc since these
# dates need a setback or set-forward to be meaningful to the wastewater signal.

import pandas as pd 

VAX_LOOK_BACK = 10     # how far back from sample date do we look for vax info
CASES_LOOK_AHEAD = 7     # how far ahead of sample do we look for case info
HOSP_LOOK_AHEAD = 14     # how far ahead of sample do we look for hospitalization info
DEATHS_LOOK_AHEAD = 21     # how far ahead of sample do we look for mortality info
SAMPLES_OUTPUT_FILE = "NwssRawEnhanced.tsv"


# Wastewater samples raw data. Acquired by restricted download from CDC, after signing data-use agreement.

path = "~/Desktop/COVID Programming/CDC/cdc-nwss-restricted-data-set-wastewater-2022-02-08.csv"
SampleDF = pd.read_csv(path, sep=',', header='infer', dtype=str)

# Covid facts per county from https://apidocs.covidactnow.org. You need an API key, which they give to anyone who asks.

path = "~/Desktop/COVID Programming/Covid Act Now/counties.timeseries-16feb2022.csv"
CovidDF = pd.read_csv(path, sep=',', header='infer', dtype=str)

# Population by county. 
# From https://www.census.gov/programs-surveys/popest/technical-documentation/research/evaluation-estimates/2020-evaluation-estimates/2010s-counties-total.html  

path = "~/Desktop/COVID Programming/US Census/co-est2020.csv"
PopDF = pd.read_csv(path, sep=',', header='infer', dtype=str, encoding='latin-1')

# Make a few changes to the covid facts file.
                                                            
CovidDF = CovidDF.rename(columns={"metrics.caseDensity":"metrics.caseDensity100k", "date":"covid_facts_date"})
CovidDF["covid_facts_date"] = pd.to_datetime(CovidDF["covid_facts_date"], errors='coerce')

# Make a few changes to the wastewater samples file.

SampleDF["sample_collect_date"] = pd.to_datetime(SampleDF["sample_collect_date"], errors='coerce')
SampleDF = SampleDF.rename(columns={"county_names": "CountyFIPS"})
SampleDF["CountyFIPS"] = SampleDF["CountyFIPS"].str.split(",")   
SampleDF = SampleDF.explode("CountyFIPS")          # One county per row
SampleDF["CountyFIPS"] = SampleDF["CountyFIPS"].str.strip("[]' ")

# Make a few changes to the population file.

PopDF = PopDF.rename(columns={"STATE":"StateFIPS", "COUNTY":"CountyFIPS-3"})  
PopDF["CountyFIPS"] = PopDF["StateFIPS"] + PopDF["CountyFIPS-3"]  # make full 5-digit county FIPS
PopDF["POPESTIMATE2020"] = pd.to_numeric(PopDF["POPESTIMATE2020"], errors='coerce').fillna(0).astype(int)
PopDF = PopDF[["CountyFIPS", "POPESTIMATE2020"]]
PopDF = PopDF[PopDF.POPESTIMATE2020 > 0]     # throw out bad data

# Add county population to each sample row. 
# Numbers get changed to float during merge, so fix back to integer.

SampleDF = SampleDF.merge(PopDF, how='left', on="CountyFIPS")
SampleDF["POPESTIMATE2020"] = pd.to_numeric(SampleDF["POPESTIMATE2020"], errors='coerce').fillna(0).astype(int)

# Create some additional date columns on the sample file. These will be used to add the 
# look ahead / look back info.

SampleDF["vax_date"] = SampleDF["sample_collect_date"] -  pd.offsets.Day(VAX_LOOK_BACK)
SampleDF["cases_date"] = SampleDF["sample_collect_date"] + pd.offsets.Day(CASES_LOOK_AHEAD)
SampleDF["hosp_date"] = SampleDF["sample_collect_date"] +  pd.offsets.Day(HOSP_LOOK_AHEAD)
SampleDF["deaths_date"] = SampleDF["sample_collect_date"] +  pd.offsets.Day(DEATHS_LOOK_AHEAD)

# Make a subset of just the vax facts we want and join it to the samples.
# Note that we join the vax info from a previous date onto each sample.

VaxDF = CovidDF[["covid_facts_date", "fips", "actuals.vaccinationsInitiated", "actuals.vaccinationsCompleted", "actuals.vaccinationsAdditionalDose", "metrics.vaccinationsInitiatedRatio", "metrics.vaccinationsCompletedRatio", "metrics.vaccinationsAdditionalDoseRatio"]]

SampleDF = SampleDF.merge(VaxDF, how='left', left_on=["vax_date", "CountyFIPS"], right_on=["covid_facts_date", "fips"])
SampleDF = SampleDF.drop(columns=["covid_facts_date", "fips"])

# Make a subset of just the covid case facts we want and join it to the samples.
# Note that we join the case info from a future date onto each sample.

CasesDF = CovidDF[["covid_facts_date", "fips", "actuals.newCases", "metrics.caseDensity100k", "metrics.infectionRate", "metrics.testPositivityRatio"]]

SampleDF = SampleDF.merge(CasesDF, how='left', left_on=["cases_date", "CountyFIPS"], right_on=["covid_facts_date", "fips"])
SampleDF = SampleDF.drop(columns=["covid_facts_date", "fips"])

# Add hospitialization facts to the sample file.

HospDF = CovidDF[["covid_facts_date", "fips", "actuals.icuBeds.currentUsageCovid", "actuals.hospitalBeds.currentUsageCovid"]]

SampleDF = SampleDF.merge(HospDF, how='left', left_on=["hosp_date", "CountyFIPS"], right_on=["covid_facts_date", "fips"])
SampleDF = SampleDF.drop(columns=["covid_facts_date", "fips"])

# Add mortality info

DeathsDF = CovidDF[["covid_facts_date", "fips", "actuals.newDeaths"]]

SampleDF = SampleDF.merge(DeathsDF, how='left', left_on=["deaths_date", "CountyFIPS"], right_on=["covid_facts_date", "fips"])
SampleDF = SampleDF.drop(columns=["covid_facts_date", "fips"])

# Debugging

#print (SampleDF.head(10), "\n")
#print (CovidDF.head(10), "\n")
#print (PopDF.head(10), "\n")

#print (SampleDF.dtypes, "\n")
#print (CovidDF.dtypes, "\n")
#print (PopDF.dtypes, "\n")

     
# Write to file.

print ("Writing enhanced wastewater samples to", SAMPLES_OUTPUT_FILE, "with", SampleDF.shape[0], "rows.\n")
SampleDF.to_csv(SAMPLES_OUTPUT_FILE, encoding='utf-8', sep='\t', index=False)

