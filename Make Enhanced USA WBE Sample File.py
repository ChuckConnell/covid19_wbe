
# Take the full CDC NWSS dataset and add fields for vax status, cases, hospitalizations and deaths,
# in that location at that time. But be clever about what dates we use for vax, death, etc since these
# dates need a setback or set-forward to be meaningul to the wastewater signal.

import pandas as pd 
from sys import exit

VAX_LOOK_BACK = 10     # how far back from sample date do we look for vax info
CASES_LOOK_AHEAD = 7     # how far ahead of sample do we look for case info
HOSP_LOOK_AHEAD = 14     # how far ahead of sample do we look for hospitalization info
ICU_LOOK_AHEAD = 14     # how far ahead of sample do we look for ICU info
DEATHS_LOOK_AHEAD = 21     # how far ahead of sample do we look for mortality info
SAMPLES_OUTPUT_FILE = "NwssRawEnhanced.tsv"


# Get the source data. 

# Wastewater samples raw data. Acquired by restricted download from CDC, after signing data-use agreement.
#path = "~/Desktop/COVID Programming/CDC/cdc-nwss-restricted-data-set-wastewater-2022-02-08.csv"
path = "~/Desktop/COVID Programming/CDC/wastewater-2022-02-08-small.csv"
SampleDF = pd.read_csv(path, sep=',', header='infer', dtype=str)

# Covid facts per county from https://apidocs.covidactnow.org. You need an API key, which they give to anyone who asks.
#path = "~/Desktop/COVID Programming/Covid Act Now/counties.timeseries-16feb2022.csv"
path = "~/Desktop/COVID Programming/Covid Act Now/small.csv"
CovidDF = pd.read_csv(path, sep=',', header='infer', dtype=str)

# Population, to compute per capita
# From https://www.census.gov/programs-surveys/popest/technical-documentation/research/evaluation-estimates/2020-evaluation-estimates/2010s-counties-total.html  
path = "~/Desktop/COVID Programming/US Census/co-est2020.csv"
PopDF = pd.read_csv(path, sep=',', header='infer', dtype=str, encoding='latin-1')

# Make the population file the way we want it.

PopDF = PopDF.rename(columns={"STATE": "StateFIPS", "COUNTY":"CountyFIPS-3"})  # this file has the 3-digit county code
PopDF["CountyFIPS"] = PopDF["StateFIPS"] + PopDF["CountyFIPS-3"]  # make full 5-digit county FIPS
PopDF["POPESTIMATE2020"] = pd.to_numeric(PopDF["POPESTIMATE2020"], errors='coerce').fillna(0).astype(int)
PopDF = PopDF[["CountyFIPS", "POPESTIMATE2020"]]

# Fix the data types and clean up missing values. Clarify column names.
                                                            
SampleDF["sample_collect_date"] = pd.to_datetime(SampleDF["sample_collect_date"], errors='coerce')

CovidDF["covid_facts_date"] = pd.to_datetime(CovidDF["date"], errors='coerce')

# Misc data clean up looking for obviously bad rows.

PopDF = PopDF[PopDF.POPESTIMATE2020 > 0]   

# The WBE sample file has a LIST of counties on each row. Change to one county per row.
# Clarify that these are FIPS codes, not regular county names.
                                                            
SampleDF = SampleDF.rename(columns={"county_names": "CountyFIPS"})
SampleDF["CountyFIPS"] = SampleDF["CountyFIPS"].str.split(",")
SampleDF = SampleDF.explode("CountyFIPS")
SampleDF["CountyFIPS"] = SampleDF["CountyFIPS"].str.strip("[]' ")

# Add county population to each sample row. Numbers get changed to float during merge, so fix back to integer.

SampleDF = SampleDF.merge(PopDF, how='left', on="CountyFIPS")
SampleDF["POPESTIMATE2020"] = pd.to_numeric(SampleDF["POPESTIMATE2020"], errors='coerce').fillna(0).astype(int)

# Create some additional date columns on the sample file. These will be used to add the 
# look ahead / look back info.

SampleDF["vax_date"] = SampleDF["sample_collect_date"] -  pd.offsets.Day(VAX_LOOK_BACK)
SampleDF["cases_date"] = SampleDF["sample_collect_date"] + pd.offsets.Day(CASES_LOOK_AHEAD)
SampleDF["hosp_date"] = SampleDF["sample_collect_date"] +  pd.offsets.Day(HOSP_LOOK_AHEAD)
SampleDF["icu_date"] = SampleDF["sample_collect_date"] +  pd.offsets.Day(ICU_LOOK_AHEAD)
SampleDF["deaths_date"] = SampleDF["sample_collect_date"] +  pd.offsets.Day(DEATHS_LOOK_AHEAD)

# Make a subset of just the vax facts we want and join it to the samples.
# Note that we join the vax info from a previous date onto each sample.

VaxDF = CovidDF[["covid_facts_date", "fips", "actuals.vaccinationsInitiated", "actuals.vaccinationsCompleted", "actuals.vaccinationsAdditionalDose", "metrics.vaccinationsInitiatedRatio", "metrics.vaccinationsCompletedRatio", "metrics.vaccinationsAdditionalDoseRatio"]]

SampleDF = SampleDF.merge(VaxDF, how='left', left_on=["vax_date", "CountyFIPS"], right_on=["covid_facts_date", "fips"])
SampleDF = SampleDF.drop(columns=["covid_facts_date", "fips"])

# Add hospitialization facts to the sample file.


# Add ICU info


# Add mortality info



# Debugging

print (SampleDF.head(10), "\n")
print (CovidDF.head(10), "\n")
print (PopDF.head(10), "\n")

print (SampleDF.dtypes, "\n")
print (CovidDF.dtypes, "\n")
print (PopDF.dtypes, "\n")

     

# Write to file.

print ("Writing county output to", SAMPLES_OUTPUT_FILE, "with", SampleDF.shape[0], "rows.\n")
SampleDF.head(100).to_csv(SAMPLES_OUTPUT_FILE, encoding='utf-8', sep='\t', index=False)


exit()

#
# The code below here is copied from a similar program. I am stealing peices of it then will delete.
#

# Make a DataFrame that will hold all of our results.

AllCountiesAllPeriodsDF = pd.DataFrame()

# Loop over our whole time period. 

for this_period in range(PERIOD_COUNT):
    
    # Calc the dates we need.
    
    this_period_start = OVERALL_START_DATE + pd.offsets.Day(this_period * PERIOD_LENGTH)
    this_period_end = this_period_start + pd.offsets.Day(PERIOD_LENGTH)
    this_period_vax_start = this_period_start - pd.offsets.Day(VAX_BACKDATE)
    this_period_vax_end = this_period_end - pd.offsets.Day(VAX_BACKDATE)
    
    print ("Working on mortality between {} and {}, with vax dates of {} and {}...\n".format(this_period_start.date(), this_period_end.date(), this_period_vax_start.date(), this_period_vax_end.date()))
    
    # Get the deaths in each county for the start and end of this time period. We get two DFs and will join them later.
    
    PeriodStartDeathDF = DeathDF[DeathDF.date == this_period_start]
    PeriodEndDeathDF = DeathDF[DeathDF.date == this_period_end]

    # Rename some death columns so we can keep them straight after the join.
    
    PeriodStartDeathDF = PeriodStartDeathDF.rename(columns={"date": "StartDate"})
    PeriodEndDeathDF = PeriodEndDeathDF.rename(columns={"date": "EndDate"})
    
    PeriodStartDeathDF = PeriodStartDeathDF.rename(columns={"deaths": "StartDeaths"})
    PeriodEndDeathDF = PeriodEndDeathDF.rename(columns={"deaths": "EndDeaths"})

    # Get the vax facts for start and end of this time period.
    
    PeriodStartVaxDF = VaxDF[VaxDF.Date == this_period_vax_start]
    PeriodEndVaxDF = VaxDF[VaxDF.Date == this_period_vax_end]

    # Rename some vax columns so we can keep them straight after the join.

    PeriodStartVaxDF = PeriodStartVaxDF.rename(columns={"Date": "VaxStartDate"})
    PeriodEndVaxDF = PeriodEndVaxDF.rename(columns={"Date": "VaxEndDate"})

    PeriodStartVaxDF = PeriodStartVaxDF.rename(columns={"Series_Complete_Yes": "Series_Complete_Yes_Start"})
    PeriodEndVaxDF = PeriodEndVaxDF.rename(columns={"Series_Complete_Yes": "Series_Complete_Yes_End"})

    PeriodStartVaxDF = PeriodStartVaxDF.rename(columns={"Administered_Dose1_Recip": "Administered_Dose1_Recip_Start"})
    PeriodEndVaxDF = PeriodEndVaxDF.rename(columns={"Administered_Dose1_Recip": "Administered_Dose1_Recip_End"})  
    
    # Join all the info we have so each row is one county for this time period. Use inner join because we only want counties
    # that we have full info about.
    
    AllCountiesOnePeriodDF = PeriodEndDeathDF.merge(PeriodStartDeathDF, how='inner', on="STATE-COUNTY")
    AllCountiesOnePeriodDF = AllCountiesOnePeriodDF.merge(PopDF, how='inner', on="STATE-COUNTY")
    AllCountiesOnePeriodDF = AllCountiesOnePeriodDF.merge(PeriodStartVaxDF, how='inner', on="STATE-COUNTY")
    AllCountiesOnePeriodDF = AllCountiesOnePeriodDF.merge(PeriodEndVaxDF, how='inner', on="STATE-COUNTY")
    
    # Not getting anything interesting from adding obesity and SVI info, so don't join it.
    #AllCountiesOnePeriodDF = AllCountiesOnePeriodDF.merge(ObesityDF, how='inner', on="STATE-COUNTY")
    
    # Add all counties for this time period to the overall result set. 
    
    AllCountiesAllPeriodsDF = AllCountiesAllPeriodsDF.append(AllCountiesOnePeriodDF)
        
    # End of big loop gathering data for each time period. Now we can start the analysis on it.

# Make a column that shows deaths in each county in each period. 
# This will produce some negative numbers, since death counts can be readjusted later or incorrect. Throw out those rows.

AllCountiesAllPeriodsDF["Deaths"] = (AllCountiesAllPeriodsDF["EndDeaths"] - AllCountiesAllPeriodsDF["StartDeaths"])
AllCountiesAllPeriodsDF = AllCountiesAllPeriodsDF[AllCountiesAllPeriodsDF.Deaths >= 0]

# Make a column that is average vax for this period, for both vax numbers.

AllCountiesAllPeriodsDF["Series_Complete_Yes_Mid"] = (AllCountiesAllPeriodsDF["Series_Complete_Yes_Start"] + AllCountiesAllPeriodsDF["Series_Complete_Yes_End"]) / 2
AllCountiesAllPeriodsDF["Administered_Dose1_Recip_Mid"] = (AllCountiesAllPeriodsDF["Administered_Dose1_Recip_Start"] + AllCountiesAllPeriodsDF["Administered_Dose1_Recip_End"]) / 2

# Make new columns that are "deaths per 100k pop", "fully vaccinated per 100" and "one+ vax per 100".

AllCountiesAllPeriodsDF["FullVaxPer100"] = (100*(AllCountiesAllPeriodsDF["Series_Complete_Yes_Mid"]/AllCountiesAllPeriodsDF["POPESTIMATE2020"])).round(1)
AllCountiesAllPeriodsDF["OnePlusVaxPer100"] = (100*(AllCountiesAllPeriodsDF["Administered_Dose1_Recip_Mid"]/AllCountiesAllPeriodsDF["POPESTIMATE2020"])).round(1)
AllCountiesAllPeriodsDF["DeathsPer100k"] = (100000*(AllCountiesAllPeriodsDF["Deaths"]/AllCountiesAllPeriodsDF["POPESTIMATE2020"])).round(1)

# Some data cleanup, throwing out obviously bad values.

AllCountiesAllPeriodsDF = AllCountiesAllPeriodsDF[AllCountiesAllPeriodsDF.FullVaxPer100 <= 100]  
AllCountiesAllPeriodsDF = AllCountiesAllPeriodsDF[AllCountiesAllPeriodsDF.FullVaxPer100 >= 0]  

AllCountiesAllPeriodsDF = AllCountiesAllPeriodsDF[AllCountiesAllPeriodsDF.OnePlusVaxPer100 <= 100]
AllCountiesAllPeriodsDF = AllCountiesAllPeriodsDF[AllCountiesAllPeriodsDF.OnePlusVaxPer100 >= 0]

AllCountiesAllPeriodsDF = AllCountiesAllPeriodsDF[AllCountiesAllPeriodsDF.DeathsPer100k >= 0]



