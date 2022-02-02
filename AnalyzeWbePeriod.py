
import pandas as pd 

VAX_COUNTY_FILE = "AllCountiesAllPeriods.tsv"

# Get the source data. 

path = VAX_COUNTY_FILE
CountyVaxMortalityDF = pd.read_csv(path, sep='\t', header='infer')

county_rows = CountyVaxMortalityDF.shape[0]

# Compute correlation between death ranking and vax rankings. We will do both vax rankings because it is easy and might be interesting.

FullVaxCorr = (CountyVaxMortalityDF["DeathsPer100k"].corr(CountyVaxMortalityDF["FullVaxPer100"], method="spearman")).round(3)
OnePlusVaxCorr = (CountyVaxMortalityDF["DeathsPer100k"].corr(CountyVaxMortalityDF["OnePlusVaxPer100"], method="spearman")).round(3)

print ("Full vax to death correlation (Spearman) for all US counties over", county_rows, "data points is", FullVaxCorr, "\n")
print ("1+ vax to death correlation (Spearman) for all US counties over", county_rows, "data points is ", OnePlusVaxCorr, "\n")

# Show some data visualizations.

CountyVaxMortalityDF.plot.scatter(x="FullVaxPer100", y="DeathsPer100k", title="US Counties -- Full Vax per 100 vs Deaths per 100k -- " + str(county_rows) + " data points" )
CountyVaxMortalityDF.plot.scatter(x="OnePlusVaxPer100", y="DeathsPer100k", title="US Counties -- 1+ Vax per 100 vs Deaths per 100k -- " + str(county_rows) + " data points")

# Histograms of mortality and vax %s

CountyVaxMortalityDF.hist(column="FullVaxPer100", bins=10)
CountyVaxMortalityDF.hist(column="OnePlusVaxPer100", bins=10)
CountyVaxMortalityDF.hist(column="DeathsPer100k", bins=10)
CountyVaxMortalityDF.hist(column="DeathsPer100k", bins=10, range=[0, 200])

#########   EXTREME SETS

# Find the hghest vaccinated 500 counties and the lowest 500. Goal is to throw out the noise in the middle.

HILOW_CUTOFF = 500

CountyVaxHiDF = CountyVaxMortalityDF.nlargest(HILOW_CUTOFF, "FullVaxPer100", keep='all')
CountyVaxLowDF = CountyVaxMortalityDF.nsmallest(HILOW_CUTOFF, "OnePlusVaxPer100", keep='all')

#print(CountyVaxHiDF.describe(), "n")
#print(CountyVaxLowDF.describe(), "n")


# Get aome stats for these two data sets.

HiVaxMean = round(CountyVaxHiDF.FullVaxPer100.mean(), 1)
HiVaxStd = round(CountyVaxHiDF.FullVaxPer100.std(), 1)
HiVaxDeathsMean = round(CountyVaxHiDF.DeathsPer100k.mean(), 1)
HiVaxDeathsStd = round(CountyVaxHiDF.DeathsPer100k.std(),1) 

LowVaxMean = round(CountyVaxLowDF.OnePlusVaxPer100.mean(), 1)
LowVaxStd = round(CountyVaxLowDF.OnePlusVaxPer100.std(), 1)
LowVaxDeathsMean = round(CountyVaxLowDF.DeathsPer100k.mean(), 1)
LowVaxDeathsStd = round(CountyVaxLowDF.DeathsPer100k.std(), 1)

# Show results

print ("For the highest", HILOW_CUTOFF, "vaxxed counties...")
print ("Full vax per 100 mean and std are:",  HiVaxMean,  "and", HiVaxStd)
print ("Deaths per 100k mean and std are:",  HiVaxDeathsMean, "and", HiVaxDeathsStd, "\n")

print ("For the lowest", HILOW_CUTOFF, "vaxxed counties...")
print ("1+ vax per 100 mean and std are:",  LowVaxMean,  "and", LowVaxStd)
print ("Deaths per 100k mean and std are:",  LowVaxDeathsMean, "and", LowVaxDeathsStd, "\n")



###########   MAINE ONLY

print ("\n\nJust for Maine...\n")
# Just for Maine, to confirm some analysis from a newspaper.

CountyVaxMaineDF = CountyVaxMortalityDF[(CountyVaxMortalityDF.ST_ABBR == 'ME') ]
maine_rows = CountyVaxMaineDF.shape[0]

# Compute correlation between death ranking and vax rankings. We will do both vax rankings because it is easy and might be interesting.

MaineFullVaxCorr = (CountyVaxMaineDF["DeathsPer100k"].corr(CountyVaxMaineDF["FullVaxPer100"], method="spearman")).round(3)
MaineOnePlusVaxCorr = (CountyVaxMaineDF["DeathsPer100k"].corr(CountyVaxMaineDF["OnePlusVaxPer100"], method="spearman")).round(3)

print ("Full vax to death correlation (Spearman) for Maine counties over", maine_rows, "data points is", MaineFullVaxCorr, "\n")
print ("1+ vax to death correlation (Spearman) for Maine counties over", maine_rows, "data points is", MaineOnePlusVaxCorr, "\n")

# Show some data visualizations.

CountyVaxMaineDF.plot.scatter(x="FullVaxPer100", y="DeathsPer100k", title="MAINE Counties -- Full vax vs Deaths -- " + str(maine_rows) + " data points" )
CountyVaxMaineDF.plot.scatter(x="OnePlusVaxPer100", y="DeathsPer100k", title="MAINE Counties -- 1+ vax vs Deaths -- " + str(maine_rows) + " data points" )


'''
############    SVI

print ("\n\nSVI results...\n")

SviCorr = (CountyVaxMortalityDF["DeathsPer100k"].corr(CountyVaxMortalityDF["Overall_SVI_Pct"], method="spearman")).round(3)

print ("SVI to death correlation (Spearman) for all US counties over", county_rows, "data points is", SviCorr, "\n")

CountyVaxMortalityDF.plot.scatter(x="Overall_SVI_Pct", y="DeathsPer100k", title="US Counties -- SVI % vs Deaths per 100k -- " + str(county_rows) + " data points" )

CountyVaxMortalityDF.hist(column="Overall_SVI_Pct", bins=10)



############    Not Fully Vaxxed + SVI

print ("\n\nNot Fully Vaxxed + SVI results...\n")

NFV_Plus_SVI_Corr = (CountyVaxMortalityDF["DeathsPer100k"].corr(CountyVaxMortalityDF["NFV_Plus_SVI"], method="spearman")).round(3)

print ("Not Fully Vaxxed + SVI to death correlation (Spearman) for all US counties over", county_rows, "data points is", NFV_Plus_SVI_Corr, "\n")

CountyVaxMortalityDF.plot.scatter(x="NFV_Plus_SVI", y="DeathsPer100k", title="US Counties -- Not Fully Vaxxed + SVI % vs Deaths per 100k -- " + str(county_rows) + " data points" )

CountyVaxMortalityDF.hist(column="NFV_Plus_SVI", bins=10)
'''



'''
############    OBESITY 

print ("\n\nObesity results...\n")

ObesityCorr = (CountyVaxMortalityDF["DeathsPer100k"].corr(CountyVaxMortalityDF["Obesity_Percentage"], method="spearman")).round(3)

print ("Obesity to death correlation (Spearman) for all US counties over", county_rows, "data points is", ObesityCorr, "\n")

CountyVaxMortalityDF.plot.scatter(x="Obesity_Percentage", y="DeathsPer100k", title="US Counties -- Obesity % vs Deaths per 100k -- " + str(county_rows) + " data points" )

CountyVaxMortalityDF.hist(column="Obesity_Percentage", bins=10)
'''
