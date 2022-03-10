
# Starting with the "enhanced" wastewater data from CDC NWSS, see what interesting analysis we can do on it.

import pandas as pd 

ENHANCED_RAW_FILE = "~/Desktop/COVID Programming/NwssRawEnhanced.tsv"

# Open the file.

RawDF = pd.read_csv(ENHANCED_RAW_FILE, sep='\t', header='infer', dtype='str')
print("\nRows in raw file: " + str(RawDF.shape[0]))   
                      
# Get rid of rows we are not going to use now. 

RawDF = RawDF.query("pcr_target == 'sars-cov-2' ")
RawDF = RawDF.query("pcr_gene_target == 'n1' or pcr_gene_target == 'n2' or pcr_gene_target == 'n1 and n2 combined'  ")
RawDF = RawDF.query("sample_matrix == 'raw wastewater' ")
print("\nRows with N1 or N2 water samples: " + str(RawDF.shape[0]))

# Fix datatypes of fields we are going to use.

RawDF["sample_collect_date"] = pd.to_datetime(RawDF["sample_collect_date"], errors='coerce')
RawDF["metrics.vaccinationsInitiatedRatio"] = pd.to_numeric(RawDF["metrics.vaccinationsInitiatedRatio"], errors='coerce')
RawDF["metrics.vaccinationsCompletedRatio"] = pd.to_numeric(RawDF["metrics.vaccinationsCompletedRatio"], errors='coerce')
RawDF["metrics.vaccinationsAdditonalDoseRatio"] = pd.to_numeric(RawDF["metrics.vaccinationsAdditionalDoseRatio"], errors='coerce')
RawDF["pcr_target_avg_conc"] = pd.to_numeric(RawDF["pcr_target_avg_conc"], errors='coerce')

# Show scatter of vaccination over time, for all water samples. This is mostly a sanity check, since each
# county should always be increasing.

RawDF.plot.scatter(x="sample_collect_date", y="metrics.vaccinationsInitiatedRatio", title="USA WBE -- Date vs Vax -- " + str(RawDF.shape[0]) + " data points" )

# The RNA detections are not in consistent units. Some are plain and some are log10. Make one consistent column.

RawDF["pcr_target_units_norm"] = "copies/ml wastewater"

RawDF.loc[RawDF["pcr_target_units"] == "copies/l wastewater", "pcr_target_avg_conc_norm"] = (RawDF["pcr_target_avg_conc"] / 1000)
RawDF.loc[RawDF["pcr_target_units"] == "log10 copies/l wastewater", "pcr_target_avg_conc_norm"] = ((10 ** RawDF["pcr_target_avg_conc"]) / 1000) 
print ()
print (RawDF["pcr_target_avg_conc_norm"].describe())

# Throw out outliers for RNA signal. Be careful as this might change over time.

RawDF = RawDF.query("pcr_target_avg_conc_norm <= 30000")
print("\nRows with N1 or N2 copies/ml <= 30,000: " + str(RawDF.shape[0]))

# Show scatter of RNA signal to vaccinations.

RawDF.plot.scatter(x="metrics.vaccinationsInitiatedRatio", y="pcr_target_avg_conc_norm", title="USA WBE -- 1+ Vax vs WW RNA c/ml")
RawDF.plot.scatter(x="metrics.vaccinationsCompletedRatio", y="pcr_target_avg_conc_norm", title="USA WBE -- Full Vax vs WW RNA c/ml")
RawDF.plot.scatter(x="metrics.vaccinationsAdditionalDoseRatio", y="pcr_target_avg_conc_norm", title="USA WBE -- Boost Vax vs WW RNA c/ml")

# Find correlations of RNA to vax status. Note that this is per COUNTY, not per PERSON.

OneVaxCorr = (RawDF["metrics.vaccinationsInitiatedRatio"].corr(RawDF["pcr_target_avg_conc_norm"], method="spearman")).round(3)
FullVaxCorr = (RawDF["metrics.vaccinationsCompletedRatio"].corr(RawDF["pcr_target_avg_conc_norm"], method="spearman")).round(3)
BoostVaxCorr = (RawDF["metrics.vaccinationsAdditionalDoseRatio"].corr(RawDF["pcr_target_avg_conc_norm"], method="spearman")).round(3)

print ("\nOne vax to WW RNA correlation (Spearman) over", RawDF.shape[0], "data points is", OneVaxCorr)
print ("\nFull vax to WW RNA correlation (Spearman) over", RawDF.shape[0], "data points is", FullVaxCorr)
print ("\nBoosted vax to WW RNA correlation (Spearman) over", RawDF.shape[0], "data points is", BoostVaxCorr)

# Show scatter of RNA signal to disease outcomes.




# Calc correlation of RNA to disease outcomes. 




# Calc a new value, RNA + unvaccinated %, for each sample. Intuitively, this is what would correlate to 
# worse outcomes.






 