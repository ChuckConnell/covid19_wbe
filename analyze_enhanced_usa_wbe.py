
# Starting with the "enhanced" wastewater data from CDC NWSS, see what interesting analysis we can do on it.

import pandas as pd 
import math

ENHANCED_RAW_FILE = "~/Desktop/COVID Programming/NwssRawEnhanced.tsv"
RNA_TOP_COMPRESSION = 0.90  # consider any RNA signal above this quantile as 100%

# Open the file.

RawDF = pd.read_csv(ENHANCED_RAW_FILE, sep='\t', header='infer', dtype='str')
print("\nRows in raw file: " + str(RawDF.shape[0]))   
                      
# Fix datatypes of fields we are going to use.

RawDF["sample_collect_date"] = pd.to_datetime(RawDF["sample_collect_date"], errors='coerce')

RawDF["metrics.vaccinationsInitiatedRatio"] = pd.to_numeric(RawDF["metrics.vaccinationsInitiatedRatio"], errors='coerce')
RawDF["metrics.vaccinationsCompletedRatio"] = pd.to_numeric(RawDF["metrics.vaccinationsCompletedRatio"], errors='coerce')
RawDF["metrics.vaccinationsAdditionalDoseRatio"] = pd.to_numeric(RawDF["metrics.vaccinationsAdditionalDoseRatio"], errors='coerce')

RawDF["pcr_target_avg_conc"] = pd.to_numeric(RawDF["pcr_target_avg_conc"], errors='coerce')

RawDF["metrics.caseDensity100k"] = pd.to_numeric(RawDF["metrics.caseDensity100k"], errors='coerce')
RawDF["metrics.infectionRate"] = pd.to_numeric(RawDF["metrics.infectionRate"], errors='coerce')
RawDF["metrics.testPositivityRatio"] = pd.to_numeric(RawDF["metrics.testPositivityRatio"], errors='coerce')

RawDF["actuals.hospitalBeds.capacity"] = pd.to_numeric(RawDF["actuals.hospitalBeds.capacity"], errors='coerce')
RawDF["actuals.icuBeds.capacity"] = pd.to_numeric(RawDF["actuals.icuBeds.capacity"], errors='coerce')
RawDF["actuals.hospitalBeds.currentUsageCovid"] = pd.to_numeric(RawDF["actuals.hospitalBeds.currentUsageCovid"], errors='coerce')
RawDF["actuals.icuBeds.currentUsageCovid"] = pd.to_numeric(RawDF["actuals.icuBeds.currentUsageCovid"], errors='coerce')

RawDF["metrics.icuCapacityRatioRolling10"] = pd.to_numeric(RawDF["metrics.icuCapacityRatioRolling10"], errors='coerce')
RawDF["metrics.bedsWithCovidPatientsRatioRolling10"] = pd.to_numeric(RawDF["metrics.bedsWithCovidPatientsRatioRolling10"], errors='coerce')
RawDF["metrics.weeklyCovidAdmissionsPer100kRolling10"] = pd.to_numeric(RawDF["metrics.weeklyCovidAdmissionsPer100kRolling10"], errors='coerce')

RawDF["actuals.newDeaths"] = pd.to_numeric(RawDF["actuals.newDeaths"], errors='coerce')
RawDF["metrics.newDeathsRolling7"] = pd.to_numeric(RawDF["metrics.newDeathsRolling7"], errors='coerce')

RawDF["COUNTY_POPESTIMATE2020"] = pd.to_numeric(RawDF["COUNTY_POPESTIMATE2020"], errors='coerce')

# Get rid of rows we are not going to use now. 

RawDF = RawDF.query("pcr_target == 'sars-cov-2' ")
RawDF = RawDF.query("pcr_gene_target == 'n1' or pcr_gene_target == 'n2' or pcr_gene_target == 'n1 and n2 combined'  ")
RawDF = RawDF.query("sample_matrix == 'raw wastewater' ")
print("\nRows with N1 or N2 water samples: " + str(RawDF.shape[0]))

# Get rid of rows with various bad data.

RawDF = RawDF[RawDF["sample_collect_date"].notna()]  
print("\nRows with good sample dates: " + str(RawDF.shape[0]))

RawDF = RawDF[RawDF["pcr_target_avg_conc"].notna()]  
RawDF = RawDF.query("pcr_target_avg_conc >= 0")
print("\nRows with good sample measures: " + str(RawDF.shape[0]))

# The RNA detections are not in consistent units. Some are plain and some are log10. Make one consistent column.

RawDF["pcr_target_units_norm"] = "copies/ml wastewater"

RawDF.loc[RawDF["pcr_target_units"] == "copies/l wastewater", "pcr_target_avg_conc_norm"] = (RawDF["pcr_target_avg_conc"] / 1000)
RawDF.loc[RawDF["pcr_target_units"] == "log10 copies/l wastewater", "pcr_target_avg_conc_norm"] = ((10 ** RawDF["pcr_target_avg_conc"]) / 1000) 

print ()
print (RawDF["pcr_target_avg_conc_norm"].describe())

# Create a new field with daily deaths per 100k pop. Use the rolling average.

RawDF["metrics.newDeathsRolling7per100k"] = (RawDF["metrics.newDeathsRolling7"] / RawDF["COUNTY_POPESTIMATE2020"]) * 100000
print ()
print (RawDF["metrics.newDeathsRolling7per100k"].describe())

# Check on data we will work with...

print ()
print (RawDF["metrics.vaccinationsInitiatedRatio"].describe())
print ()
print (RawDF["metrics.vaccinationsCompletedRatio"].describe())
print ()
print (RawDF["metrics.vaccinationsAdditionalDoseRatio"].describe())

print ()
print (RawDF["metrics.icuCapacityRatioRolling10"].describe())
print ()
print (RawDF["metrics.bedsWithCovidPatientsRatioRolling10"].describe())
print ()
print (RawDF["metrics.weeklyCovidAdmissionsPer100kRolling10"].describe())


# Create a new metric UPR that is "unvaccinated percent + RNA signal percent" The max value will be 200.
# The hypothesis is that UPR predicts hospitalization and death.
# RESULT - It turns out this metric is not very useful, at least not so far. It does not correlate
# with anything any more than straight RNA level. I leave this code in just in case it is useful later.

# First find the percent not vaxed, so we invert the vax ratios.

RawDF["not_one_vax_pct"] = (1.0 - RawDF["metrics.vaccinationsInitiatedRatio"]) * 100 
RawDF["not_full_vax_pct"] = (1.0 - RawDF["metrics.vaccinationsCompletedRatio"]) * 100 
RawDF["not_boost_vax_pct"] = (1.0 - RawDF["metrics.vaccinationsAdditionalDoseRatio"]) * 100 

# Normalize the RNA signal so that it is out of 100. This requires compressing all the very high signals to 100.

top_rna = RawDF["pcr_target_avg_conc_norm"].quantile(RNA_TOP_COMPRESSION)
RawDF["rna_signal_pct"] = (RawDF["pcr_target_avg_conc_norm"] / top_rna) * 100
RawDF.loc[RawDF["rna_signal_pct"] > 100, "rna_signal_pct"] = 100

# Make the UPR metrics -- Unvaxed Plus Rna

RawDF["UPR_one_vax"] = (RawDF["not_one_vax_pct"] + RawDF["rna_signal_pct"]).round(2)
RawDF["UPR_full_vax"] = (RawDF["not_full_vax_pct"] + RawDF["rna_signal_pct"]).round(2)
RawDF["UPR_boost_vax"] = (RawDF["not_boost_vax_pct"] + RawDF["rna_signal_pct"]).round(2)

# Histogram of RNA copies / ml

RawDF.hist(column="pcr_target_avg_conc_norm", bins=10)
RawDF.hist(column="pcr_target_avg_conc_norm", bins=10, range=[0,500])

# Look at RNA vs test positivity

RawDF.plot.scatter(x="pcr_target_avg_conc_norm", y="metrics.testPositivityRatio")
print ("\nRNA corr test positive ratio: " + str(RawDF["pcr_target_avg_conc_norm"].corr(RawDF["metrics.testPositivityRatio"], method="spearman").round(3)))

# Look at RNA vs case density

RawDF.plot.scatter(x="pcr_target_avg_conc_norm", y="metrics.caseDensity100k")
print ("\nRNA corr case densty per 100k pop: " + str(RawDF["pcr_target_avg_conc_norm"].corr(RawDF["metrics.caseDensity100k"], method="spearman").round(3)))

# Look at RNA vs hospitalization

RawDF.plot.scatter(x="pcr_target_avg_conc_norm", y="metrics.weeklyCovidAdmissionsPer100kRolling10")
r = RawDF["pcr_target_avg_conc_norm"].corr(RawDF["metrics.weeklyCovidAdmissionsPer100kRolling10"], method="spearman").round(3)
print ("\nRNA corr hospital covid admits per 100k pop: " + str(r))
       
num = 50000
stderr = 1.0 / math.sqrt(num - 3)
delta = 1.96 * stderr
lower = math.tanh(math.atanh(r) - delta)
upper = math.tanh(math.atanh(r) + delta)
print ("\n95 pct confidence for above: lower %.4f, upper %.4f" % (lower, upper))

RawDF.plot.scatter(x="pcr_target_avg_conc_norm", y="metrics.bedsWithCovidPatientsRatioRolling10")
print ("\nRNA corr hospital beds with covid patients ratio: " + str(RawDF["pcr_target_avg_conc_norm"].corr(RawDF["metrics.bedsWithCovidPatientsRatioRolling10"], method="spearman").round(3)))

# Look at RNA vs ICU

RawDF.plot.scatter(x="pcr_target_avg_conc_norm", y="metrics.icuCapacityRatioRolling10")
print ("\nRNA corr ICU capacity ratio: " + str(RawDF["pcr_target_avg_conc_norm"].corr(RawDF["metrics.icuCapacityRatioRolling10"], method="spearman").round(3)))

# Look at RNA vs death

RawDF.plot.scatter(x="pcr_target_avg_conc_norm", y="metrics.newDeathsRolling7per100k")
print ("\nRNA corr new deaths per day per 100k pop: " + str(RawDF["pcr_target_avg_conc_norm"].corr(RawDF["metrics.newDeathsRolling7per100k"], method="spearman").round(3)))




