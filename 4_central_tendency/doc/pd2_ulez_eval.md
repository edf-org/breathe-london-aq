Author: Lauren Padilla\
Project: London GSV

## Objective
To evaluate trends in air pollution central tendency before and after London implemented ULEZ policy (April 8 2019). 

## Data sources
- street-view-air-quality-london BQ datasets
- CERC model results at receptor locations corresponding to mobile segments with minimum 10 drive periods before and after ULEZ (tables generated manually in BQ console from GCS files: cerc_receptor_segment_lookup, cerc_model_data_mobile_receptors)
- AURN historic timeseries data

## Scripts
**Step:** 0  
**Purpose:** To plot distribution of typical air pollution concentrations for road segments meeting minimum pass criteria before and after ULEZ implementation. Distributions are separated as those that include only road segments inside or outside of ULEZ boundary and with A or local function.  
**Path:** .\central_tendency\central2_ulezeval_boxplots.py  
**Requirements:** Google cloud API python client and bigquery with pandas, central tendency processing and corresponding output tables/files  
**Output:** .\charts\ulezeval_[species]_[n]drivesb4andafter_inpoly[bool]_%Y%b%d.png and ..\..\..\Data\Intermediate\central_tendency\central2_ulezeval_[species]_[n]drivesb4andafter_%Y%b%d.csv

**Step:** 1  
**Purpose:** To evaluate the sampling uncertainty in central estimates associated with a small number of passes in each period before and after ULEZ tool effect. 
**Path:** .\central_tendency\central2_ulezeval_sampleuncertainty.py  
**Requirements:** Google cloud API python client and bigquery with pandas, central tendency processing and corresponding output tables/files  
**Output:**
.\charts\ulezeval_[species]_sampleuncertainty_%Y%b%d.png and ..\..\..\Data\Intermediate\central_tendency\central2_ulezeval_[species]_[period]_sampleuncertainty_%Y%b%d.csv

**Step:** 2  
**Purpose:** To plot scatterplots of segment median concentrations before vs. after ULEZ with confidence limits showing whether observed change was below the 1:1 line, separated by inside or outside ULEZ and A or local road function. 
**Path:** .\central_tendency\central2_ulezeval_scatterplots.py  
**Requirements:** Google cloud API python client and bigquery with pandas, central tendency processing and corresponding output tables/files  
**Output:**
.\charts\ulezeval_[species]_scatter_[n]drivesb4andafter_inpoly[bool]_%Y%b%d.png and  
.\charts\ulezeval_[species]_count_[n]drivesb4andafter_inpoly[bool]_%Y%b%d.png

**Step:** 3  
**Purpose:** To plot timeseries of selected stationary monitors including historic trends through the ULEZ implementation period.  
**Path:** .\central_tendency\central2_ulezeval_stationarytrend.py  
**Requirements:** Python pandas, AURN network data files  
**Output:**
.\charts\ulezeval_AURN_monthly_[species]_[site].png 

**Step:** 4  
**Purpose:** To plot monthly and hourly timeseries of all drive-period-median concentrations for those segments with a monitoring period in that month and compare to corresponding data from CERC model without ULEZ effects and historic AURN data excluding ULEZ period. The goal is to identify a downward monthly trend in the observed mobile data that is steeper than the model data and the observed stationary data with the difference partly attributable to the ULEZ.  
**Path:** .\central_tendency\central2_ulezeval_mobiletrend.py  
**Requirements:** Google cloud API python client and bigquery with pandas, central tendency processing and corresponding output tables/files, CERC model results  
**Output:**
.\charts\ulezeval_mobile_seasonality_onechart_[species]_[n]drivesb4andafter_inpoly[bool]_%Y%b%d.png and  
.\charts\ulezeval_mobile_seasonality_[species]_[n]drivesb4andafter_inpoly[bool]_%Y%b%d.png and  
.\charts\ulezeval_mobile_diurnalcycle_[species]_[n]drivesb4andafter_inpoly[bool]_%Y%b%d.png and  
..\..\..\Data\Intermediate\central_tendency\central2_ulezeval_mobile_seasonality_[species]_[n]drivesb4andafter_%Y%b%d.csv and  
..\..\..\Data\Intermediate\central_tendency\central2_ulezeval_mobile_seasonality_[species]_[n]drivesb4andafter_segments_%Y%b%d.csv



## Notes
