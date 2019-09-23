Author: Lauren Padilla\
Project: London GSV

## Objective
Scoping study of temporal bias in mobile data collection.
Compare distributions of AQMesh stationary data at full temporal resolution within the mobile collection period (a. all times, b. all daytime,weekday times) to distributions of AQMesh stationary data only at the same times (nearest minute) of mobile sampling within the mobile collection period (selected subset). Every road segment will have two bias statistics: bias_a = (selected subset median - all time median)/(all time median)* 100 and bias_b = (selected subset daytime,weekly median - all daytime,weekday median)/(all daytime,weekday median)* 100. Comparison should be completed for all species, however we currently have only AQMesh data for NO2.  
The AQMesh timeseries will be derived from individual pods in the ULEZ that have data going back to Fall of 2018 (> 6000 hours) and the median of all such pods to test the sensitivity of results to our choice of stationary data. Bias calcs will be grouped and summarized by the number of mobile drive times included in the measurement subset.

## Data sources
- street-view-air-quality-london BQ datasets

## Scripts
**Step:** 0  
**Purpose:** To calculate percent bias, summarize by number of drives for case where all times (0-23 hours, Sun-Sat) are included, and plot percent bias as a function of N drives. 

> *Scripted Query:* 0  
*Purpose:* Calc 5th, 25th, 50th, 75th and 95th percentile percent bias at each of N drives.  
*Output:* dataframe of summary stats

**Path:** ..\temporal_bias\tempbias0_AQMeshcomparison.py  
**Requirements:** Google cloud API python client and bigquery with pandas, ggplot, pandas, central_tendency processing and corresponding output tables  
**Output:** ..\charts\bias.png and n_segments.png

**Step:** 1  
**Purpose:** To determine which days and hours should be included in daytime, weekday restricted comparison. 

> *Scripted Query:* 0  
*Purpose:* Calc distribution of mobile measurement hours of the day 
*Output:* print hour of day and percentage of samples to console and csv

> *Scripted Query:* 1  
*Purpose:* Calc distribution of mobile measurement days of the week 
*Output:* print day of week and percentage of samples to console and csv

**Path:** ..\temporal_bias\tempbias1_choosedayshours.py  
**Requirements:** Google cloud API python client and bigquery with pandas, pandas, central_tendency processing and corresponding output tables  
**Output:** ..\charts\[*].csv indicating drives primarily occurred on M-F and between 4am and 11pm

**Step:** 2  
**Purpose:** To calculate percent bias, summarize by number of drives for case where only M-F, 4am-11pm times are included, and plot percent bias as a function of N drives. 

> *Scripted Query:* 0  
*Purpose:* Calc 5th, 25th, 50th, 75th and 95th percentile percent bias at each of N drives.  
*Output:* dataframe of summary stats

**Path:** ..\temporal_bias\tempbias2_AQMeshrestrictedcomparison.py  
**Requirements:** Google cloud API python client and bigquery with pandas, ggplot, pandas, central_tendency processing and corresponding output tables  
**Output:** ..\charts\bias_restricted.png and n_segments_restricted.png

**Step:** 3  
**Purpose:** Map the temporal bias estimate for the pod median on the road segments in the leaflet dashboard.  

> *Scripted Query:* 0  
*Purpose:* Select the segments, geometry and bias estimates.  
*Output:* ..\charts\map.csv

**Path:** ..\temporal_bias\tempbias3_maptemporalbias.py  
**Requirements:** Google cloud API python client and bigquery with pandas, pandas, central_tendency processing and corresponding output tables  
**Output:** ..\charts\map.csv 

## Manual

**Step:** 4
*Purpose:* Make geojson (https://ogre.adc4gis.com/) and update dashboard to show bias map layer
**Output:** ..\..\dashboards\leaflet\pctbias_2019Sep18.json and updated dashboard code

## Notes
