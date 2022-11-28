
# Computes some basic statistics from the Global.health covid dataset, then puts them in a 
# spreadsheet for easy scanning. 

# The spreadsheet is a sanity check on the g.h covid data. For each country: number of case/list rows,
# number of deaths, date of latest case in file. 

# Chuck Connell, November 2022

import os
import fnmatch
import pandas as pd 

# Gives fast close approximations. 1 means read all the data, no skipping. 
# Just make sure to multiply results by this number.

SAMPLE_SKIP = 1

# Directory that holds all the g.h CSV country files.  (input)

GH_COUNTRY_DIR = "/Users/chuck/Desktop/COVID Programming/Global.Health/gh_2022-11-28/country/"

# The spreadsheet file we create. (output)

GH_SUMMARY_FILE = "gh_summary.tsv"

#  EXECUTABLE CODE

# Show the skip count, if any.

print ("\nSkip count: " + str(SAMPLE_SKIP))

# Get all the file objects in the input directory.

files = os.scandir(GH_COUNTRY_DIR)

# Make a dataframe that will hold the output.

summary_DF = pd.DataFrame(data=None, dtype=str, columns=["file", "latest_case", "rows", "deaths"])

# Loop over all the files in the input directory.
 
for f in files:

    # Throw out files we don't want.
    
    if not (f.is_file()): continue
    if not (fnmatch.fnmatch(f, "*.csv")): continue

    # Get  and show name of this file.
    
    print ("\nWorking on: " + f.name)
    GH_PATH = GH_COUNTRY_DIR + f.name

    # Find the number of rows and last date.
    
    gh_DF = pd.read_csv(GH_PATH, sep=',', header=0, dtype=str, skiprows=(lambda i : i % SAMPLE_SKIP != 0))
    rows = str(gh_DF.shape[0])
    latest = str(gh_DF["events.confirmed.date"].max())

    # Find the number of deaths in this file.

    gh_DF["events.outcome.value"] = gh_DF["events.outcome.value"].str.lower()
    gh_deaths_DF = gh_DF[gh_DF["events.outcome.value"].isin(["died","death"])]
    deaths = str(gh_deaths_DF.shape[0])

    # Append info for this file to the overall output spreadsheet.
    
    summary_DF = summary_DF.append({"file":f.name, "latest_case":latest, "rows":rows, "deaths":deaths}, ignore_index=True)
    
# Done with file loop. Close the file list.

files.close()

# Write the spreadsheet.

print ("\nWriting summary data to " + GH_SUMMARY_FILE)
summary_DF.to_csv(GH_SUMMARY_FILE, encoding='utf-8', sep='\t', index=False)


