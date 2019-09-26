::::The following only need to be executed if inputs change
::::They are constant lookup tables required for execution of qaqc.bat scripts
::::1.0 load device, parameter, lagtime lookup tables into BQ
::::The step numbers are consecutive with the steps in qaqc.bat
::::(i.e. indicating at which step each lookup is prerequisite) 
python ..\qcstages\qaqc_stage1_0_loadtables.py
::::2a.0 make hex status lookup table
python ..\qcstages\qaqc_stage2a_0_makehexlookup.py
::::2a.1 load hex status lookup table into BQ
python ..\qcstages\qaqc_stage2a_1_loadhexlookupbq.py

