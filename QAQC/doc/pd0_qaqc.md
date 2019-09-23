Author: Lauren Padilla\
Project: London GSV

## Objective
To clean, format, and quality control the London Google Street View mobile air pollution data.

## Data sources
- Device manuals
- Protocol document in Google drive
- street-view-air-quality-london BQ UK.raw_data table 
- London extent, bounding box coordinates
- NPL parking area bounding box coordinates

## Manual steps
**Step:** 0  
**Purpose:** Prepare lookup tables for parameters, devices, lagtimes, and statuses  
**Input:** Instrument manuals, guidance from Air Monitors, status interpretation table  (Google drive), Cambridge instrument response time testing results

## Scripts
**Step:** 1  
**Purpose:** Run through QAQC stages.

> *Subscript:* qaqc_stage0_0_removeduplicates.py  
*Purpose:*  Initial preparation and staging of tables, removal of duplicate records  
*Input:* UK.raw_data  
*Output:* UK.stage0_test

> *Subscript:* qaqc_stage0_1_spatialuncertainty.py  
*Purpose:*  We had issues with our gps and pollutant clocks not synchronized to a common source of time. This script quantifies the uncertainty in seconds for later conversion to distance in meters. Issues were resolved after xxxx-xx-xx. This script does not need to be run again for more recent data.   
*Input:* UK.stage0_test  
*Output:* UK.stage0_clock_uncertainty_test  

> *Subscript:* qaqc_stage1_0_loadtables.py  
*Purpose:* Load 3 metadata lookup tables from files. Don't need to run again as more data streams in unless we added new parameters or changed response times. 
*Input:* csv files 
*Output:* UK.qaqc_devices_lookup, UK.qaqc_parameters_lookup, UK.qaqc_lagtimes_lookup

> *Subscript:* qaqc_stage1_1_alignlatlon.py  
*Purpose:*  Pair non-gps measurands with gps coordinates on response-time-adjusted timestamps and calculate spatial uncertainty of this pairing given any clock uncertainty and vehicle speed
*Input:* UK.stage0_test, UK.stage0_clock_uncertainty_test, UK.qaqc_devices_lookup, UK.qaqc_parameters_lookup, UK.qaqc_lagtimes_lookup 
*Output:* UK.stage1_test

> *Subscript:* qaqc_stage2a_0_makehexlookup.py  
*Purpose:*  Build lookup for all possible status codes decoding hex bit map into status and qc flags. 
*Input:* csv files with device, status info (based on Status Interpretation Table in Sheets) 
*Output:* hexstatus_lookup.csv

> *Subscript:* qaqc_stage2a_1_loadhexlookupbq.py  
*Purpose:*  Load status code lookup into BQ 
*Input:* hexstatus_lookup.csv 
*Output:* UK.qaqc_hexstatus_lookup

> *Subscript:* qaqc_stage2a_2_maketable.py  
*Purpose:*  Make initial stage 2 table adding column for status flags and QC flags
*Input:* UK.qaqc_hexstatus_lookup, UK.stage1_test 
*Output:* UK.stage2_test

> *Subscript:* qaqc_stage2b_0_morechecks.py  
*Purpose:*  Update of the QC flags based on additional checks e.g. temperature bounds, lat/lon bounds etc.
*Input:* UK.stage2_test 
*Output:* UK.stage2_test

**Path:** .\QAQC\bat\qaqc.bat  
**Requirements:** Google cloud API python client and bigquery with pandas  
**Output:** BQ tables with stage 0 through N mobile data  

## Notes
