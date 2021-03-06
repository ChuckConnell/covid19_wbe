
# Compare the counties covered by the COVID-19 wastewater analysis from Biobot and USA CDC NWSS and make lists of the results.
# Put the counties in "STATE | COUNTY_NAME | FIPS" format so they are readable.

# Output a file that can be used as input to a map of the US, showing the results.
# Add various other information about the counties which might be useful to map viewers.

import pandas as pd 
from urllib import request
from sodapy import Socrata

BIOBOT_DOWNLOAD = "https://github.com/biobotanalytics/covid19-wastewater-data/raw/master/wastewater_by_county.csv"
NWSS_DATASET = "2ew6-ywp6"

USA_COUNTIES_LOCAL = "/Users/chuck/Desktop/COVID Programming/fips2county.tsv"
COUNTY_POP_LOCAL = "/Users/chuck/Desktop/COVID Programming/US Census/Population_Density_County.csv"
BIOBOT_LOCAL = "/Users/chuck/Desktop/COVID Programming/Biobot/wastewater_by_county.csv"
SVI_LOCAL = "/Users/chuck/Desktop/COVID Programming/CDC/DiabetesAtlasData_2018.csv"

BIOBOT_LIST = "biobot_counties.txt"
NWSS_LIST = "nwss_counties.txt"
BIOBOT_NWSS_INTERSECTION_LIST = "biobot_and_nwss_counties.txt"
BIOBOT_NWSS_UNION_LIST = "biobot_or_nwss_counties.txt"
BIOBOT_ONLY_LIST = "biobot_only_counties.txt"
NWSS_ONLY_LIST = "nwss_only_counties.txt"
MISSING_COUNTIES_LIST = "missing_counties.txt"
FIPS_MAP_DATA = "wbe_coverage_map.tsv"

# Get the list of all USA counties and tweak as we need it. 
# Downloaded once from https://github.com/ChuckConnell/articles/raw/master/fips2county.tsv

AllCountiesDF = pd.read_csv(USA_COUNTIES_LOCAL, sep='\t', header='infer', dtype=str)
AllCountiesDF["STATE_COUNTY_FIPS"] = AllCountiesDF["STATE_COUNTY"] + " | " + AllCountiesDF["CountyFIPS"]  # Add a useful field that combines state+county+fips. 
AllCountiesDF = AllCountiesDF[["CountyFIPS", "STATE_COUNTY_FIPS"]]  # don't need any other columns
AllCountiesDF = AllCountiesDF.rename(columns={"CountyFIPS":"FIPS"})  # to match Flourish naming

# Get the population density of US counties, and tweak as needed.
# This is June 2020 data that I downloaded once from https://covid19.census.gov/datasets/21843f238cbb46b08615fc53e19e0daf/explore

CountyPopDF = pd.read_csv(COUNTY_POP_LOCAL, sep=',', header='infer', dtype=str)
CountyPopDF = CountyPopDF[["GEOID", "B01001_calc_PopDensity"]]
CountyPopDF = CountyPopDF.rename(columns={"GEOID":"FIPS", "B01001_calc_PopDensity":"DensitySqKm"})  

# SVI from 2018. From https://gis.cdc.gov/grasp/diabetes/DiabetesAtlas.html
# For info about SVI see https://www.atsdr.cdc.gov/placeandhealth/svi/index.html

SviDF = pd.read_csv(SVI_LOCAL, sep=',', header='infer', dtype=str)
SviDF = SviDF[["County_FIPS", "Overall SVI"]]
SviDF = SviDF.rename(columns={"County_FIPS":"FIPS", "Overall SVI":"SVI"})  

# Income

# TODO

# Ethnicity / race 

# TODO

# Get the latest counties covered by Biobot, and tweak as we need it.

request.urlretrieve(BIOBOT_DOWNLOAD, BIOBOT_LOCAL)
BiobotDF = pd.read_csv(BIOBOT_LOCAL, sep=',', header='infer', dtype=str)

BiobotDF = BiobotDF[["fipscode"]]  # don't need any other columns
BiobotDF = BiobotDF.rename(columns={"fipscode":"FIPS"})  # to match Flourish naming
BiobotDF.loc[BiobotDF["FIPS"].str.len() == 4, "FIPS"] = "0" + BiobotDF["FIPS"]  # fix problem with missing leading zeroes 
BiobotDF = BiobotDF.merge(AllCountiesDF, how="inner", on=["FIPS"])  # add readable names

# Get the latest counties covered by NWSS. 
# We grab this from their public dataset, not the special restricted data.
# This dataset sometimes has more than one FIPS per row, so we have to "normalize" and explode these rows.

cdc_client = Socrata("data.cdc.gov", None)
NwssDF = pd.DataFrame.from_records(cdc_client.get(NWSS_DATASET, limit=1000000))

NwssDF = NwssDF[["county_fips"]]  # don't need any other columns
NwssDF["county_fips"] = NwssDF["county_fips"].astype(str)  # make sure it is a string
NwssDF = NwssDF.rename(columns={"county_fips":"FIPS"})  # to match Flourish naming

NwssDF["FIPS"] = NwssDF["FIPS"].str.split(",")    # change comma separate string to array
NwssDF = NwssDF.explode("FIPS")          # make one row per county
NwssDF["FIPS"] = NwssDF["FIPS"].str.strip("[]' ")   # clean up, one pure FIPS per row

NwssDF = NwssDF.merge(AllCountiesDF, how="inner", on=["FIPS"])  # add readable names

# For each data source, get only the field we want in the output lists, and put them into a Python set.

all_counties = set(AllCountiesDF["STATE_COUNTY_FIPS"])
biobot_counties = set(BiobotDF["STATE_COUNTY_FIPS"])
nwss_counties = set(NwssDF["STATE_COUNTY_FIPS"])

# Find the union, intersection, differences, and missing counties.

biobot_nwss_union = biobot_counties.union(nwss_counties)
biobot_nwss_intersection = biobot_counties.intersection(nwss_counties)
biobot_only = biobot_counties.difference(nwss_counties)
nwss_only = nwss_counties.difference(biobot_counties)
missing_counties = all_counties.difference(biobot_nwss_union)

# Output the result sets in sorted order.

print ("\nWriting list of " + str(len(biobot_counties)) + " counties in Biobot data to file " + BIOBOT_LIST)
with open(BIOBOT_LIST, 'w') as f:
    print(*sorted(biobot_counties), file=f, sep="\n")

print ("\nWriting list of " + str(len(nwss_counties)) + " counties in NWSS data to file " + NWSS_LIST)
with open(NWSS_LIST, 'w') as f:
    print(*sorted(nwss_counties), file=f, sep="\n")

print ("\nWriting list of " + str(len(biobot_nwss_union)) + " counties in Biobot or NWSS data to file " + BIOBOT_NWSS_UNION_LIST)
with open(BIOBOT_NWSS_UNION_LIST, 'w') as f:
    print(*sorted(biobot_nwss_union), file=f, sep="\n")

print ("\nWriting list of " + str(len(biobot_nwss_intersection)) + " counties in both Biobot and NWSS data to file " + BIOBOT_NWSS_INTERSECTION_LIST)
with open(BIOBOT_NWSS_INTERSECTION_LIST, 'w') as f:
    print(*sorted(biobot_nwss_intersection), file=f, sep="\n")

print ("\nWriting list of " + str(len(biobot_only)) + " counties only in Biobot data to file " + BIOBOT_ONLY_LIST)
with open(BIOBOT_ONLY_LIST, 'w') as f:
    print(*sorted(biobot_only), file=f, sep="\n")

print ("\nWriting list of " + str(len(nwss_only)) + " counties only in NWSS data to file " + NWSS_ONLY_LIST)
with open(NWSS_ONLY_LIST, 'w') as f:
    print(*sorted(nwss_only), file=f, sep="\n")

print ("\nWriting list of " + str(len(missing_counties)) + " counties not in Biobot or NWSS data to file " + MISSING_COUNTIES_LIST)
with open(MISSING_COUNTIES_LIST, 'w') as f:
    print(*sorted(missing_counties), file=f, sep="\n")

# Create a DF and file that holds info we need to make a map of WBE collection counties.

MapDF = AllCountiesDF
MapDF["WBE_WHO"] = "none"
MapDF.loc[MapDF["STATE_COUNTY_FIPS"].isin(biobot_only), "WBE_WHO"] = "biobot"
MapDF.loc[MapDF["STATE_COUNTY_FIPS"].isin(nwss_only), "WBE_WHO"] = "nwss"
MapDF.loc[MapDF["STATE_COUNTY_FIPS"].isin(biobot_nwss_intersection), "WBE_WHO"] = "nwss-biobot"
    
# Add population density in the counties

MapDF = MapDF.merge(CountyPopDF, how="left", on=["FIPS"])  # add pop density for all
#MapDF.loc[MapDF["STATE_COUNTY_FIPS"].isin(missing_counties), "DensitySqKm"] = ""   # suppress counties with no WBE data 
MapDF["DensitySqKm"] = pd.to_numeric(MapDF["DensitySqKm"], errors='coerce').round(1)
MapDF["DensityCategory"] = pd.qcut(MapDF['DensitySqKm'], 3, labels=["low", "med", "high"])

# Add SVI for the counties

MapDF = MapDF.merge(SviDF, how="left", on=["FIPS"])  # add pop density for all
#MapDF.loc[MapDF["STATE_COUNTY_FIPS"].isin(missing_counties), "SVI"] = ""   # suppress counties with no WBE data 
MapDF["SVI"] = pd.to_numeric(MapDF["SVI"], errors='coerce').round(3)
MapDF["SviCategory"] = pd.qcut(MapDF["SVI"], 3, labels=["low", "med", "high"])

# Write out the map data file.

print ("\nWriting map data to " + FIPS_MAP_DATA)
MapDF.to_csv(FIPS_MAP_DATA, encoding='utf-8', sep='\t', index=False)

