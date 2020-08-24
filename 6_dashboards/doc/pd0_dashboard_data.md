Author: Lauren Padilla\
Project: London GSV

## Objective
To prepare a geojson file for interactive display in a lightweight leaflet.js dashboard and add it to the map.

## Data sources
- Geojson data files
- javascript, html files in GCS bucket: https://console.cloud.google.com/storage/browser/london-viz?project=street-view-air-quality-london

## Manual steps
**Step:** 0  
**Purpose:** Prepare the data in a csv with fields that ogr2ogr will recognize for geometry (i.e. “the_geom” field with WKT geometry e.g. LINESTRING, POINT, etc)  
**Input:** result from central_tendency\central0_1hour_medians.py (central0_drivesummarystats_[%y%b%d].csv)  
**Output:** https://storage.cloud.google.com/london-viz/[].csv  

**Step:** 1  
**Purpose:** Run the csv through ogr2ogr (https://ogre.adc4gis.com/ is one option) to create a geojson file with jsonp callback such as  “jsonCallback”  
**Input:** csv from step 0  
**Output:** https://storage.cloud.google.com/london-viz/[].json 

**Step:** 2  
**Purpose:** Update Javascript code to display the data in leaflet.js map  
**Input:**   
**Output:** : .\leaflet\london.js  

## Scripts
**Purpose:** Run html in brower (must be logged into GCP account) to display interactive map and data  
**Path:** .\leaflet\londondash.html  
**Requirements:** Mapbox basemap tile key   
**Output:** Cool interactive visualization  

## Notes
