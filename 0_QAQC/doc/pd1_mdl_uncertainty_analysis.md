Author(s): Dan Peters, Lauren Padilla  
Project: London GSV

## Objective
To quantify instrument precision, accuracy, and minimum detection limits for each pollutant measured in Breathe London mobile data collection. 

## Data sources
**Zero data is flagged in BQ table: `street-view-air-quality-london.UK_partners.stage3_v3_2019Sep23` with mmode = 4. Consider only valid ((qc_flag = 0) or (qc_flag = 24 and parameter_id = 133 and vid = 27522)) data in this assessment. Sources of zero data include:**  
- Weekly ~5 minute zero checks for CO2, BC, PDR PM 2.5, O3, NO2, Palas PM, NO, LDSA collected at NPL
  - BQ table: `street-view-air-quality-london.UK.instrument_weekly_data`
  - GCS: Buckets/street-view-air-quality-london/instrument_and_driving_reports/instrument_reports (the source data for the BQ table)  
- Instrument-initiated zero periods for NO2 collected during driving and at NPL. Periods varied in frequency throughout the project from every 15 minutes to every ~6 hours. Periods lasted on the order of 1 minute. 
  - Indicated by instrument status description of 'Baseline on - measurement period'
- Instrument-initiated zero periods for NO collected during driving and at NPL. 
  - Indicated by instrument status description of 'Currently in zero mode'  
- Longer ~45 minute gas-zero periods collected at NPL on May 23 2019 for car 27533 and on June 5-6 2019 for car 27522.
  
**Calibration data from all of the above sources is flagged in BQ table: `street-view-air-quality-london.UK_partners.stage3_v3_2019Sep23` with mmode = 3. Consider only valid ((qc_flag = 0) or (qc_flag = 24 and parameter_id = 133 and vid = 27522)) data in this assessment. Calibration data must be parsed further to identify data from span periods. Sources of span data inlude:**  
- Weekly ~5 minute span checks for CO2, NO2, NO 
  - BQ table: `street-view-air-quality-london.UK.instrument_weekly_data`
  - GCS: Buckets/street-view-air-quality-london/instrument_and_driving_reports/instrument_reports (the source data for the BQ table)  
  - Indicated by location at NPL and measurement values close to the targets: 
    - CO2: 1000 ppm
    - NO2: 504 ppb
    - NO: 0.48 ppm
- Instrument-initiated span periods for NO collected during driving and at NPL. 
  - Indicated by instrument status description of 'Currently in span mode' 
- *Hopefully we also have multi-point calibration data - need to follow up*

**Colocation data when both vehicles are at NPL is flagged in BQ table: `street-view-air-quality-london.UK_partners.stage3_v3_2019Sep23` with mmode = 2. Consider only valid ((qc_flag = 0) or (qc_flag = 24 and parameter_id = 133 and vid = 27522)) data in this assessment. Select when vehicle speeds are 0.**  
- Nightly colocations when the vehicles are parked between shifts and are not otherwise being calibrated or maintained
   
**Collocation data with reference instruments also occurs at NPL and is indicated by the mmode = 2 in BQ table: `street-view-air-quality-london.UK_partners.stage3_v3_2019Sep23`. Consider only valid ((qc_flag = 0) or (qc_flag = 24 and parameter_id = 133 and vid = 27522)) data in this assessment. Select when vehicle speeds are 0.**
- NPL maintains a Palas Fidas.
  - BQ table: *need to create this*
  - GCS: Buckets/street-view-air-quality-london/instrument-data/ref-data-NPL/fidas/bv-palas-data (original proprietary source files)
  - GCS: Buckets/street-view-air-quality-london/instrument-data/ref-data-NPL/fidas/extracted (extracted text files)  
- NPL may maintain a NOx instrument
  - *need to follow up*

## Console Queries
**Step:**   
**Purpose:**    
**Database:** street-view-air-quality-london BQ  
**Query name:**   
**Output:**   

## Scripts
**Step:** 0  
**Purpose:**  Retrieve relevant zero, span, and calibration data  
>*Scripted Query:* 0  
*Purpose:* Retrieve zero data  
*Output:* mdl_uncertainty_analysis_{pollutant}_zero.csv   

>*Scripted Query:* 1  
*Purpose:* Retrieve span data  
*Output:* mdl_uncertainty_analysis_{pollutant}_span.csv   

>*Scripted Query:* 2  
*Purpose:* Retrieve collocation data  
*Output:* mdl_uncertainty_analysis_{pollutant}_collocation.csv   

**Path:** ..\mdl_uncertainty_analysis\md10_retrieve_data.py  
**Requirements:** Google cloud API python client and bigquery with pandas, qcstages processing and corresponding output tables  
**Output:** ..\mdl_uncertainty_analysis\charts\*.csv 

## Manual steps
**Step:**   
**Purpose:**    
**Input:**  
**Output:**  

## Notes
