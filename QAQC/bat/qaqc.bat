::::0.0 remove duplicates from reading file sections multiple times and from time synchronization
::python ..\qcstages\qaqc_stage0_0_removeduplicates.py
::::0.1 calc clock uncertainty
::python ..\qcstages\qaqc_stage0_1_spatialuncertainty.py
::::1.0 load device, parameter, lagtime lookup tables into BQ
::python ..\qcstages\qaqc_stage1_0_loadtables.py
::::1.1 timeshift for lag times and join latitude and longitude on timestamp
::python ..\qcstages\qaqc_stage1_1_alignlatlon.py
::::2a.0 make hex status lookup table
::python ..\qcstages\qaqc_stage2a_0_makehexlookup.py
::::2a.1 load hex status lookup table into BQ
::python ..\qcstages\qaqc_stage2a_1_loadhexlookupbq.py
::::2a.2 make initial stage 2 table
python ..\qcstages\qaqc_stage2a_2_maketable.py
::::2b.0 apply additional stage2 flags
python ..\qcstages\qaqc_stage2b_0_morechecks.py
::::3.0 set null values where flagged, add measurement mode column, select into initial stage 3 table
python ..\qcstages\qaqc_stage3_0_nullify.py
::::3.1 apply measurement mode
python ..\qcstages\qaqc_stage3_1_measurementmode.py
::::3.2 calc nox as no2 and append to stage 3
python ..\qcstages\qaqc_stage3_2_noxasno2.py

