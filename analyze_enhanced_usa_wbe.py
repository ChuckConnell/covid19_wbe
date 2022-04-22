
# Starting with the "enhanced" wastewater data from CDC NWSS, see what interesting analysis we can do on it.

import pandas as pd 

ENHANCED_RAW_FILE = "~/Desktop/COVID Programming/NwssRawEnhanced.tsv"

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

RawDF["actuals.hospitalBeds.capacity"] = pd.to_numeric(RawDF["actuals.hospitalBeds.capacity"], errors='coerce').astype("Int32")  # Int32 handles NAN values
RawDF["actuals.icuBeds.capacity"] = pd.to_numeric(RawDF["actuals.icuBeds.capacity"], errors='coerce').astype("Int32")
RawDF["actuals.hospitalBeds.currentUsageCovid"] = pd.to_numeric(RawDF["actuals.hospitalBeds.currentUsageCovid"], errors='coerce').astype("Int32")
RawDF["actuals.icuBeds.currentUsageCovid"] = pd.to_numeric(RawDF["actuals.icuBeds.currentUsageCovid"], errors='coerce').astype("Int32")

RawDF["metrics.icuCapacityRatioRolling10"] = pd.to_numeric(RawDF["metrics.icuCapacityRatioRolling10"], errors='coerce')
RawDF["metrics.bedsWithCovidPatientsRatioRolling10"] = pd.to_numeric(RawDF["metrics.bedsWithCovidPatientsRatioRolling10"], errors='coerce')
RawDF["metrics.weeklyCovidAdmissionsPer100kRolling10"] = pd.to_numeric(RawDF["metrics.weeklyCovidAdmissionsPer100kRolling10"], errors='coerce')

RawDF["actuals.newDeaths"] = pd.to_numeric(RawDF["actuals.newDeaths"], errors='coerce').astype("Int32")
RawDF["metrics.newDeathsRolling7"] = pd.to_numeric(RawDF["metrics.newDeathsRolling7"], errors='coerce')

RawDF["COUNTY_POPESTIMATE2020"] = pd.to_numeric(RawDF["COUNTY_POPESTIMATE2020"], errors='coerce').astype("Int32")

# Get rid of rows we are not going to use now. 

RawDF = RawDF.query("pcr_target == 'sars-cov-2' ")
RawDF = RawDF.query("pcr_gene_target == 'n1' or pcr_gene_target == 'n2' or pcr_gene_target == 'n1 and n2 combined'  ")
RawDF = RawDF.query("sample_matrix == 'raw wastewater' ")
print("\nRows with N1 or N2 water samples: " + str(RawDF.shape[0]))

# Get rid of rows with bad data.

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

testDF = RawDF.query("CountyFIPS =='06037' ")  # debugging

# Create a new DF with valid vax info.
# Not sure we realy need this for analysis. Correlation will ignore empty values. But helpful to see what data we have.

VaxDF = RawDF
VaxDF = VaxDF[VaxDF["metrics.vaccinationsInitiatedRatio"].notna()]  
VaxDF = VaxDF[VaxDF["metrics.vaccinationsCompletedRatio"].notna()]  
#VaxDF = VaxDF[VaxDF["metrics.vaccinationsAdditionalDoseRatio"].notna()]  # many missing values

print("\nRows with good vax ratios: " + str(VaxDF.shape[0]))

print ()
print (VaxDF["metrics.vaccinationsInitiatedRatio"].describe())
print ()
print (VaxDF["metrics.vaccinationsCompletedRatio"].describe())

# Create a new DF with valid hospital info.
# Not sure we realy need this for analysis. Correlation will ignore empty values. But helpful to see what data we have.

HospDF = RawDF
HospDF = HospDF[HospDF["metrics.icuCapacityRatioRolling10"].notna()]  
HospDF = HospDF[HospDF["metrics.bedsWithCovidPatientsRatioRolling10"].notna()]  
HospDF = HospDF[HospDF["metrics.weeklyCovidAdmissionsPer100kRolling10"].notna()]  

print("\nRows with good hospital ratios: " + str(HospDF.shape[0]))

# Create a new DF with valid death info.
# Not sure we realy need this for analysis. Correlation will ignore empty values. But helpful to see what data we have.

DeathDF = RawDF
DeathDF = DeathDF[DeathDF["metrics.newDeathsRolling7"].notna()]  

print("\nRows with good mortality info: " + str(DeathDF.shape[0]))

#RawDF["not_one_vax_pct"] = (1.0 - RawDF["metrics.vaccinationsInitiatedRatio"]) * 100 
#RawDF["not_full_vax_pct"] = (1.0 - RawDF["metrics.vaccinationsCompletedRatio"]) * 100 
#RawDF["not_boost_vax_pct"] = (1.0 - RawDF["metrics.vaccinationsAdditionalDoseRatio"]) * 100 


# Look at the results of vax+RNA. We make a special dataframe for this.

#VaxRnaDF = RawDF[RawDF["metrics.vaccinationsInitiatedRatio"].notna()]  
#VaxRnaDF = VaxRnaDF[VaxRnaDF["metrics.vaccinationsCompletedRatio"].notna()]  
#RawDF = RawDF[RawDF["metrics.vaccinationsAdditionalDoseRatio"].notna()]   # many values missing here

#print("\nRows in Vax+RNA DF: " + str(VaxRnaDF.shape[0]))

# Create soem new vax columns 

#RawDF["not_one_vax_pct"] = (RawDF["metrics.vaccinationsInitiatedRatio"] - 1.0) * 100 
#RawDF["not_full_vax_pct"] = (1.0 - RawDF["metrics.vaccinationsCompletedRatio"]) * 100 
#RawDF["not_boost_vax_pct"] = (1.0 - RawDF["metrics.vaccinationsAdditionalDoseRatio"]) * 100 

'''
rna_std = 
rna_pct = 

not_one_vax_plus_rna = 200 max
not_full_vax_plus_rna = 200 max
not_boost_vax_plus_rna = 200 max
'''


'''
# TODO not 1 vax pct, not full vax pct, not boosted pct
# RNA signal out of 100, perhaps 6 std above zero
# vax + RNA, for all 3 vax

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

print ("\nNo negative correlation between vax ratio and WW RNA. Maybe because both vax and RNA increased over the timespan studied.")

# Show scatter of RNA signal to sickness.

RawDF.plot.scatter(x="pcr_target_avg_conc_norm", y="metrics.caseDensity100k", title="USA WBE -- RNA c/ml vs caseDensity100k")
RawDF.plot.scatter(x="pcr_target_avg_conc_norm", y="metrics.infectionRate", title="USA WBE -- RNA c/ml vs infectionRate")
RawDF.plot.scatter(x="pcr_target_avg_conc_norm", y="metrics.testPositivityRatio", title="USA WBE -- RNA c/ml vs testPositivityRatio")

# Computer correlations, RNA to sickness.

# ??
OneVaxCorr = (RawDF["pcr_target_avg_conc_norm"].corr(RawDF["metrics.caseDensity100k"], method="spearman")).round(3)
FullVaxCorr = (RawDF["pcr_target_avg_conc_norm"].corr(RawDF["metrics.caseDensity100k"], method="spearman")).round(3)
BoostVaxCorr = (RawDF["pcr_target_avg_conc_norm"].corr(RawDF["metrics.caseDensity100k"], method="spearman")).round(3)

# Hospitalizations

'''

'''metrics.caseDensity100k                  60874 non-null  object
 85  metrics.infectionRate                    61427 non-null  object
 86  metrics.testPositivityRatio              61046 non-null  object
 87  actuals.icuBeds.currentUsageCovid        4826 non-null   object
 88  actuals.hospitalBeds.currentUsageCovid   5281 non-null   object
 89  actuals.newDeaths     
actuals.hospitalBeds.capacity	actuals.icuBeds.capacity

'''

# Calc correlation of RNA to disease outcomes. 




# Calc a new value, RNA + unvaccinated %, for each sample. Intuitively, this is what would correlate to 
# worse outcomes.






 