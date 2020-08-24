::::Generate most comprehensive dataset for internal and partner use
::python ..\centralstages\central0_1hour_medians.py

::::Generate filtered data products for different audiences
::python ..\centralstages\central0_BL.py
::python ..\centralstages\central0_google.py
::python ..\centralstages\central0_mapbox.py

::::Generate shapefile from internal dataset
::C:\Python27\ArcGIS10.7\python.exe ..\centralstages\central1_csv2shp.py

::::Run ULEZ analyses based on central tendency
::python ..\centralstages\central2_ulezeval_boxplots.py
::python ..\centralstages\central2_ulezeval_mobiletrend.py
::python ..\centralstages\central2_ulezeval_scatterplots.py

::::Run WHO exceedance probability analysis based on central tendencies and uncertainty
::python ..\centralstages\central2a_ulezeval_sampleuncertainty.py
::python ..\centralstages\central3a_exceeds_WHO_confidence.py
C:\Python27\ArcGIS10.7\python.exe ..\centralstages\central3b_confidence2shp.py
::python ..\centralstages\central3c_exceeds_WHO_test.py
