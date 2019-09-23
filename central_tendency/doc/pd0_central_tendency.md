Author: Lauren Padilla\
Project: London GSV

## Objective
To compute the central tendency of drive pass aveage measurements. 

## Data sources
- street-view-air-quality-london BQ datasets

## Scripts
**Step:** 0  
**Purpose:** To calculate statistics from drive-hour distributions on each road segment for species of interest.

> *Scripted Query:* 0  
*Purpose:* Calc median, standard deviation, pass count for pm, co2, no, bc, o3, no2. Result may be converted to geojson for viewing in leaflet dashboard.  
*Output:* csv of drive summary stats

> *Scripted Query:* 1  
*Purpose:* Remove dangles to avoid self-sampling on dead end roads.  
*Output:* TBD

**Path:** .\central_tendency\central0_1hour_medians.py  
**Requirements:** Google cloud API python client and bigquery with pandas, geolocation\grid_snap_roads processing and corresponding output tables  
**Output:** ..\\..\Data\Intermediate\geolocation\testmeddrivepassmeas\central0_drivesummarystats_%y%b%d.csv and BQ table central0_drivesummarystats

## Notes
