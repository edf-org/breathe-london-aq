::::The following only need to be executed if inputs change
::::They are constant input tables required for execution of geolocation.bat scripts
::::0. Convert Greater London Roads shapefile to well known text needed for BQ
::::The step numbers are consecutive with the steps in geolocation.bat
python ..\grid_snap_roads\snap0_shp2wkt.py
::::1. Load the well-known-text file into BQ
python ..\grid_snap_roads\snap1_loadwktbq.py
::::2. Split the roads into approx. equal segments close to 30 m long
python ..\grid_snap_roads\snap2_gridroads.py


