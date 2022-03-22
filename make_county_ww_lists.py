
# Compare the counties covered by the COVID-19 wastewater analysis from Biobot and USA CDC NWSS.
# Make five lists: counties in Biobot, counties in NWSS, counties in both, counties one or the other, counties in neither.
# Put the counties in "STATE-COUNTY_NAME" format (not FIPS code) so they are readable.

import pandas as pd
from urllib import request

USA_COUNTIES_DOWNLOAD = "https://github.com/ChuckConnell/articles/raw/master/fips2county.tsv"
BIOBOT_DOWNLOAD = "https://github.com/biobotanalytics/covid19-wastewater-data/raw/master/wastewater_by_county.csv"
NWSS_DOWNLOAD = "https://data.cdc.gov/resource/2ew6-ywp6.csv"  # this url only gets 1000 rows, I think it is a CDC bug

USA_COUNTIES_LOCAL = "/Users/chuck/Desktop/COVID Programming/fips2county.tsv"
BIOBOT_LOCAL = "/Users/chuck/Desktop/COVID Programming/Biobot/wastewater_by_county.csv"
NWSS_LOCAL = "/Users/chuck/Desktop/COVID Programming/CDC/NWSS_Public_SARS-CoV-2_Wastewater_Data.csv"  # i got this manually for now

BIOBOT_LIST = "biobot_counties.txt"
NWSS_LIST = "nwss_counties.txt"
BIOBOT_NWSS_INTERSECTION_LIST = "biobot_and_nwss_counties.txt"
BIOBOT_NWSS_UNION_LIST = "biobot_or_nwss_counties.txt"
MISSING_COUNTIES_LIST = "missing_counties.txt"

# Get the list of all USA counties. Does not change very often.

request.urlretrieve(USA_COUNTIES_DOWNLOAD, USA_COUNTIES_LOCAL)
AllCountiesDF = pd.read_csv(USA_COUNTIES_LOCAL, sep='\t', header='infer', dtype=str)
FipsNameDF = AllCountiesDF[["CountyFIPS", "STATE-COUNTY"]]  # will be useful for joins

# Get the latest counties covered by Biobot.

request.urlretrieve(BIOBOT_DOWNLOAD, BIOBOT_LOCAL)
BiobotDF = pd.read_csv(BIOBOT_LOCAL, sep=',', header='infer', dtype=str)

BiobotDF = BiobotDF[["fipscode"]]  # don't need any other columns
BiobotDF.loc[BiobotDF["fipscode"].str.len() == 4, "fipscode"] = "0" + BiobotDF["fipscode"]  # fix problem with missing leading zeroes 
BiobotDF = BiobotDF.merge(FipsNameDF, how="left", left_on=["fipscode"], right_on=["CountyFIPS"])  # add readable names

# Get the latest counties covered by NWSS. We grab this from their public dataset, not the special restricted data.
# This dataset sometimes has more than one FIPS per row, so we have to "normalize" and explode these rows.

#request.urlretrieve(NWSS_DOWNLOAD, NWSS_LOCAL)   # TODO uncomment when we have a url that will get all rows, using a full local copy for now
NwssDF = pd.read_csv(NWSS_LOCAL, sep=',', header='infer',  dtype=str)

NwssDF = NwssDF[["county_fips"]]  # don't need any other columns

NwssDF["county_fips"] = NwssDF["county_fips"].str.split(",")    # change comma separate string to array
NwssDF = NwssDF.explode("county_fips")          # make one row per county
NwssDF["county_fips"] = NwssDF["county_fips"].str.strip("[]' ")   # clean up, one pure FIPS per row

NwssDF = NwssDF.merge(FipsNameDF, how="left", left_on=["county_fips"], right_on=["CountyFIPS"])  # add readable names

# For each data source, get only the readable county names, and put them into a Python set.

all_counties = set(AllCountiesDF["STATE-COUNTY"])
biobot_counties = set(BiobotDF["STATE-COUNTY"])
nwss_counties = set(NwssDF["STATE-COUNTY"])

# Find the union, intersection and missing counties.

biobot_nwss_union = biobot_counties.union(nwss_counties)
biobot_nwss_intersection = biobot_counties.intersection(nwss_counties)
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

print ("\nWriting list of " + str(len(missing_counties)) + " counties not in Biobot or NWSS data to file " + MISSING_COUNTIES_LIST)
with open(MISSING_COUNTIES_LIST, 'w') as f:
    print(*sorted(missing_counties), file=f, sep="\n")


