
# Take the full CDC NWSS dataset and add fields for vax status, cases, hospitalizations and deaths,
# in that location at that time. 

import pandas as pd 
from sys import exit

from wbe_helpers import us_state_to_abbrev

#OVERALL_START_DATE = pd.to_datetime("2020-03-01")
#PERIOD_LENGTH = 180 # days over which to count deaths
#PERIOD_COUNT = 1  # how many time blocks to count
VAX_LOOK_BACK = 10     # how far back from sample date do we look for vax info
CASES_LOOK_AHEAD = 7     # how far ahead of sample do we look for case info
HOSP_LOOK_AHEAD = 14     # how far ahead of sample do we look for hospitalization info
ICU_LOOK_AHEAD = 14     # how far ahead of sample do we look for ICU info
DEATHS_LOOK_AHEAD = 21     # how far ahead of sample do we look for mortality info
SAMPLES_OUTPUT_FILE = "NwssRawEnhanced.tsv"


# Get the source data. 
# Some of these could be automated downloads, but for now I am using snapshots on approxiately the same date.

# Wastewater samples raw data. Acquired by restricted download from CDC, after signing data-use agreement.
#path = "~/Desktop/COVID Programming/CDC/cdc-nwss-restricted-data-set-wastewater-2022-02-08.csv"
path = "~/Desktop/COVID Programming/CDC/wastewater-2022-02-08-small.csv"
SampleDF = pd.read_csv(path, sep=',', header='infer', dtype=str)

# Vaccination
# https://data.cdc.gov/Vaccinations/COVID-19-Vaccinations-in-the-United-States-County/8xkx-amqh 
path = "~/Desktop/COVID Programming/CDC/COVID-19_Vaccinations_in_the_United_States_County_10feb2022.tsv"
VaxDF = pd.read_csv(path, sep='\t', header='infer', dtype=str)

# Cases and deaths
# https://github.com/nytimes/covid-19-data
path = "~/Desktop/COVID Programming/NYT/us-counties-10feb2022.csv"
CaseDF = pd.read_csv(path, sep=',', header='infer', dtype=str)

# Cases, hosp, icu, deaths, at case detail level
# Maybe process this dataset first and create a summary per day-county?
# Or a summary that shows increasing values by day?
# https://data.cdc.gov/Case-Surveillance/COVID-19-Case-Surveillance-Public-Use-Data-with-Ge/n8mc-b4w4

# Population, to compute per capita
# From https://www.census.gov/programs-surveys/popest/technical-documentation/research/evaluation-estimates/2020-evaluation-estimates/2010s-counties-total.html  
path = "~/Desktop/COVID Programming/US Census/co-est2020.csv"
PopDF = pd.read_csv(path, sep=',', header='infer', dtype=str, encoding='latin-1')

# FIPS to county names, from https://www.ncei.noaa.gov/erddap/convert/fipscounty.html in February 2022.
path = "~/Desktop/COVID Programming/fips_counties.tsv"
FipsDF = pd.read_csv(path, sep='\t', header='infer', dtype=str, encoding='latin-1')

# We don't need every column from the files. But keep all the WBE sample fields, since our goal is to ENHANCE this file.

FipsDF = FipsDF[["FIPS", "STATE-COUNTY"]]
VaxDF = VaxDF[["Date", "Recip_County", "Recip_State", "Series_Complete_Yes", "Administered_Dose1_Recip"]]
CaseDF = CaseDF[["date", "county", "state", "cases", "deaths"]]
PopDF = PopDF[["STNAME", "CTYNAME", "POPESTIMATE2020"]]

# Fix the data types and clean up missing values. For any column name that has a space, change it to underscore.
# Clarify some column names.

SampleDF["sample_collect_date"] = pd.to_datetime(SampleDF["sample_collect_date"], errors='coerce')

VaxDF = VaxDF.rename(columns={"Date": "VaxDate"})
VaxDF["VaxDate"] = pd.to_datetime(VaxDF["VaxDate"], errors='coerce')
VaxDF["Series_Complete_Yes"] = pd.to_numeric(VaxDF["Series_Complete_Yes"], errors='coerce').fillna(0).astype(int)
VaxDF["Administered_Dose1_Recip"] = pd.to_numeric(VaxDF["Administered_Dose1_Recip"], errors='coerce').fillna(0).astype(int)

CaseDF["date"] = pd.to_datetime(CaseDF["date"], errors='coerce')
CaseDF["deaths"] = pd.to_numeric(CaseDF["deaths"], errors='coerce').fillna(0).astype(int)
CaseDF["cases"] = pd.to_numeric(CaseDF["cases"], errors='coerce').fillna(0).astype(int)

PopDF["POPESTIMATE2020"] = pd.to_numeric(PopDF["POPESTIMATE2020"], errors='coerce').fillna(0).astype(int)

# Misc data clean up looking for obviously bad rows.

# TODO throw out bad sample dates
VaxDF = VaxDF[VaxDF.Series_Complete_Yes >= 0]
VaxDF = VaxDF[VaxDF.Administered_Dose1_Recip >= 0]
CaseDF = CaseDF[CaseDF.deaths >= 0]
CaseDF = CaseDF[CaseDF.cases >= 0]
PopDF = PopDF[PopDF.POPESTIMATE2020 > 0]   

# The WBE sample file has a LIST of counties on each row. Change to one county per row.
# Clarify that these are FIPS codes, not regular county names.
# Then use the FIPS to find the real state and county
                                                            
SampleDF = SampleDF.rename(columns={"county_names": "FIPS"})
SampleDF["FIPS"] = SampleDF["FIPS"].str.split(",")
SampleDF = SampleDF.explode("FIPS")
SampleDF["FIPS"] = SampleDF["FIPS"].str.strip("[]' ")

SampleDF = SampleDF.merge(FipsDF, how='left', on="FIPS")
  
# In each other data source, we need a column that is STATE-COUNTY, so we can use it as a join key.

CaseDF["state_abbr"] = CaseDF['state'].str.upper().map(us_state_to_abbrev).fillna(CaseDF["state"])
CaseDF["county"] = CaseDF["county"].str.upper()
CaseDF["STATE-COUNTY"] = CaseDF["state_abbr"] + "-" + CaseDF["county"]
CaseDF = CaseDF.drop(columns=["state_abbr", "county", "state"])

VaxDF["Recip_County"] = VaxDF["Recip_County"].str.upper()
VaxDF["Recip_County"] = VaxDF["Recip_County"].str.split(" COUNTY").str[0]  # drop the word COUNTY
VaxDF["STATE-COUNTY"] = VaxDF["Recip_State"] + "-" + VaxDF["Recip_County"]
VaxDF = VaxDF.drop(columns=["Recip_State", "Recip_County"])  

PopDF["ST_ABBR"] = PopDF['STNAME'].str.upper().map(us_state_to_abbrev).fillna(PopDF["STNAME"])
PopDF["CTYNAME"] = PopDF["CTYNAME"].str.upper()
PopDF["CTYNAME"] = PopDF["CTYNAME"].str.split(" COUNTY").str[0]
PopDF["STATE-COUNTY"] = PopDF["ST_ABBR"] + "-" + PopDF["CTYNAME"]
PopDF = PopDF.drop(columns=["CTYNAME", "STNAME", "ST_ABBR"])

# Create some additional date columns on the sample file. These will be used to add the 
# look ahead / look back info.

SampleDF["vax_date"] = SampleDF["sample_collect_date"] -  pd.offsets.Day(VAX_LOOK_BACK)
SampleDF["cases_date"] = SampleDF["sample_collect_date"] + pd.offsets.Day(CASES_LOOK_AHEAD)
SampleDF["hosp_date"] = SampleDF["sample_collect_date"] +  pd.offsets.Day(HOSP_LOOK_AHEAD)
SampleDF["icu_date"] = SampleDF["sample_collect_date"] +  pd.offsets.Day(ICU_LOOK_AHEAD)
SampleDF["deaths_date"] = SampleDF["sample_collect_date"] +  pd.offsets.Day(DEATHS_LOOK_AHEAD)

# Add vax % from the look-back date. The join/merge operation changes ints to floats, so fix the datatype after the join.

SampleDF = SampleDF.merge(VaxDF, how='left', left_on=["vax_date", "STATE-COUNTY"], right_on=["VaxDate", "STATE-COUNTY"])
SampleDF["Series_Complete_Yes"] = pd.to_numeric(SampleDF["Series_Complete_Yes"], errors='coerce').fillna(0).astype(int)
SampleDF["Administered_Dose1_Recip"] = pd.to_numeric(SampleDF["Administered_Dose1_Recip"], errors='coerce').fillna(0).astype(int)
SampleDF = SampleDF.drop(columns=["VaxDate"])   # we had two columns with same info

# Add county population to each sample row. 

SampleDF = SampleDF.merge(PopDF, how='left', on="STATE-COUNTY")
SampleDF["POPESTIMATE2020"] = pd.to_numeric(SampleDF["POPESTIMATE2020"], errors='coerce').fillna(0).astype(int)


# Debugging

print (SampleDF.head(10), "\n")
print (VaxDF.head(10), "\n")
print (CaseDF.head(10), "\n")
print (PopDF.head(10), "\n")
print (FipsDF.head(10), "\n")

print (SampleDF.dtypes, "\n")
print (VaxDF.dtypes, "\n")
print (CaseDF.dtypes, "\n")
print (PopDF.dtypes, "\n")
print (FipsDF.dtypes, "\n")

     

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


'''
# This stuff has turned out not to be useful.
# Make a new column that changes SVI to a percent.

AllCountiesAllPeriodsDF["Overall_SVI_Pct"] = (100*(AllCountiesAllPeriodsDF["Overall_SVI"])).round(1)

# Make a new column that is "not fully vaxed" as a percent.

AllCountiesAllPeriodsDF["NotFullVaxPer100"] = (100 - (AllCountiesAllPeriodsDF["FullVaxPer100"])).round(1)

# Add column that combines Not Vaxxed and SVI.

AllCountiesAllPeriodsDF["NFV_Plus_SVI"] = (AllCountiesAllPeriodsDF["NotFullVaxPer100"] + AllCountiesAllPeriodsDF["Overall_SVI_Pct"]).round(1)
'''

