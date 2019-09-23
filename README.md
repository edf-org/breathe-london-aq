# london-aq
scripts to post-process and analyze london mobile and stationary air quality data.\
uses Google BigQuery and python to create mobile datasets at various stages of QAQC protocol.\
splits road network into uniform grid with resolution appropriate for the spatial uncertainty of our measurement collection methods and assigns quality controlled mobile data to road segments.\
calculates drive pass averages of measurements and central tendency of the drive pass distribution for each road segment.
