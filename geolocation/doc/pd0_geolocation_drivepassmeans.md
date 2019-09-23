Author: Lauren Padilla  
Project: London GSV

## Objective
To generate an appropriately uniform grid along roads, section segments, and accurately snap measurement points to the nearest road segment sections. 

## Data sources
- Greater London roads shapefile in WGS 1984 geographic coordinate system (..\\..\Data\Intermediate\greaterLondon_roads_wgs84\greaterLondon_roads_wgs84.shp)
- Mobile monitoring polygon boundaries in WGS 1984 geographic coordinate system ()
- street-view-air-quality-london BQ datasets

## Scripts
**Step:** 0  
**Purpose:** Convert road and polygon shapefile geometries to well-known text format for subsequent BQ import  
**Path:** .\grid_snap_road\step0_shp2wkt.py  
**Requirements:** arcpy, run with python 2.7 install from arcgis (C:\Python27\ArcGIS10.7\python.exe)  
**Output:** ..\\..\Data\Intermediate\greaterLondon_roads_wgs84\greaterLondon_roads_wgs84.csv  
..\\..\Data\Intermediate\boundaries\london_drive_polygons.csv

**Step:** 1  
**Purpose:** Upload road and polygon well-known text data tables to BQ tables  
**Path:** .\grid_snap_road\step1_loadwktbq.py  
**Output:** BQ tables UK.greaterlondon_roads_wgs84, UK.london_drive_polygons

**Step:** 2  
**Purpose:** Split intersection-to-intersection segments into smaller uniformly spaced segments, spacing based on instrument measurement time (width of peak) and vehicle speed, use generalized spacing by road type. Analysis of spatial scale here: .\London\Data\Intermediate\grid_snap_road\gridspacing\gridspacing_eval.xlsx\
>*Scripted query:* 0  
*Purpose:* Generate 30 m points  
*Output:* List of generated points at which to split lines ([outpath]\genpts.csv)

**Path:** .\grid_snap_roads\snap2_gridroads.py  
**Requirements:** ..\helpers\grid_road_helpers.py and arcpy for ..\helpers\arc_road_helpers.py if want to generate shapefile  
**Output:** shapefile ([outpath]\grid_30m_greaterlondon_v[version].shp) and BQ table ([dataset].[table name]_split30m)

**Step:** 3  
**Purpose:** Snap all measurements to closest road within 60 to 100 m of a road and average measurements from unique passes based on start and end times.

>*Scripted query:* 0  
*Purpose:* Determine distinct combinations of drive times and points  
*Output:* UK.snapz_distinctdrivepts  
*Note:* needs refresh every time UK.stage3_test updates

>*Scripted query:* 1  
*Purpose:* This is the heavy lifter doing the snapping. Find the distance between drive second points and road segments, joining on polygon ID and point lat/lon with road segment centroid lat/lon rounded to 2 or 3 decimal places (a search distance of ~1000 m and ~100 m, respectively). Limiting the number of points that join to a segment is a major time saver.  
*Output:* UK.snap1_distance_pt2road  
*Note:* using ST_WITHIN to limit the results returned greatly slowed things down, better to join on rounded coordinates. 

>*Scripted query:* 2  
*Purpose:* Choose the closest segment (minimum distance between road segment and drive hour point) and order the points in time, giving them a passid that increments when the segid changes for the current point compared to the previous point.  
*Output:* UK.snap2_ptclosestroad_lookup, the key lookup table for mapping measurements onto segments and passes  
*Note:* qc the coordinates that didn’t get matched to a road using snapqc_unsnappedpts_count query, also can map these to see if they should have been snapped

>*Scripted query:* 3  
*Purpose:* Initial average of 1-s measurements in a pass, average of just the consecutive points when the road segment changes, could be an average over seconds or minutes depending on traffic, ensures individual passes are treated equally even if they are of different durations. A single pass of a segment is defined by visiting another segment before returning to the same spot.  
This intermediate result has one row per road segment and columns for arrays of the start time of each pass (passtime_arr) and the concentration average for each pass (passmean_arr) by parameter_id  
*Output:* UK.snap3_drivepass_means  

>*Scripted query:* 4  
*Purpose:* This step takes the median of drive pass averages in an N-hour return period. For example, 2 passes in N hours (e.g. N=1 or N=4) would be averaged.  
*Output:* UK.snap4_drivepass_[N]hr_medians  

**Path:** .\grid_snap_roads\snap3_snapdrivepasses.py  
**Requirements:** Google API python client  
**Output:** BQ tables snapz through snap4  

## Notes
It’s possible that some points will be snapped to spurious spokes (roads we didn’t drive). Spokes will be minimized by using shorter segment spacing and will ultimately be removed because they won’t have enough drive passes.
