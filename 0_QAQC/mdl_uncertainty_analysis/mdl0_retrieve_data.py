#This program is an air quality monitoring data post-processing and analysis routine
#prepared by Environmental Defense Fund.

#For details on how to use this program refer to the doc/ folder in each root
#subfolder.

#This program is free software: you can redistribute it and/or modify
#it under the terms of the GNU General Public License as published by
#the Free Software Foundation, either version 3 of the License, or
#(at your option) any later version.   This program is distributed in the hope that it will be useful,
#but WITHOUT ANY WARRANTY; without even the implied warranty of
#MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#GNU General Public License for more details at root level in LICENSE.txt
#or see http://www.gnu.org/licenses/.

from google.cloud import bigquery
from google.oauth2 import service_account
import ggplot as gg
import matplotlib.pyplot as plt
import matplotlib.dates as md
import pandas as pd
import datetime as dt
import numpy as np
import sys
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()

credentials = service_account.Credentials.from_service_account_file(r"..\..\..\pks\street-view-air-quality-london-dc8f329b26cf.json")
project_id = 'street-view-air-quality-london'
bqclient = bigquery.Client(credentials=credentials,project=project_id)

species = 'NO2'
if species == 'NO2':
    pid = 133
    span_target = 504 #ppb
elif species == 'NO':
    pid = 45
    span_target = 0.48 #ppm
elif species == 'CO2':
    pid = 32
    span_target = 1000 #ppm
#0.retrieve zero data
qry_str = """
select parameter_id, vid, timestamp, value, status from `street-view-air-quality-london.UK_partners.stage3_v3_2019Sep23` 
where mmode = 4 and ((qc_flag = 0) or (qc_flag = 24 and parameter_id = 133 and vid = 27522))
and parameter_id = {0}
order by vid, timestamp
""".format(pid)
#qry_job = bqclient.query(qry_str,location='EU',job_config=bigquery.QueryJobConfig())
#save result as dataframe
#df = qry_job.to_dataframe()
#write dataframe to csv for later use
#df.to_csv(r'.\charts\mdl_uncertainty_analysis_{0}_zero.csv'.format(species.lower()),index=False)
print('retrieved {0} zero data'.format(species))

#1.retrieve span data
#based on times data more than 75% of span target, will need further refinement manually
#or when NO automatically doing spans
qry_str = """
select parameter_id, vid, timestamp, value, a.status from `street-view-air-quality-london.UK_partners.stage3_v3_2019Sep23` a
left join UK.qaqc_hexstatus_lookup b
on a.status=b.status and a.dev_id = b.dev_id
where mmode = 3 and ((a.qc_flag = 0) or (a.qc_flag = 24 and parameter_id = 133 and vid = 27522))
and parameter_id = {0} and speed_kmh = 0 and (value > {1}*0.75 or (status_desc like '%Currently in span mode%' and parameter_id = 45))
order by vid, timestamp
""".format(pid,span_target)
qry_job = bqclient.query(qry_str,location='EU',job_config=bigquery.QueryJobConfig())
#save result as dataframe
df = qry_job.to_dataframe()
#write dataframe to csv for later use
df.to_csv(r'.\charts\mdl_uncertainty_analysis_{0}_span.csv'.format(species.lower()),index=False)
print('retrieved {0} span data'.format(species))

#2.retrieve collocation data
qry_str = """
select parameter_id, vid, timestamp, value, status from `street-view-air-quality-london.UK_partners.stage3_v3_2019Sep23` 
where mmode = 2 and ((qc_flag = 0) or (qc_flag = 24 and parameter_id = 133 and vid = 27522))
and parameter_id = {0} and speed_kmh = 0
order by vid, timestamp
""".format(pid)
#qry_job = bqclient.query(qry_str,location='EU',job_config=bigquery.QueryJobConfig())
#save result as dataframe
#df = qry_job.to_dataframe()
#write dataframe to csv for later use
#df.to_csv(r'.\charts\mdl_uncertainty_analysis_{0}_collocation.csv'.format(species.lower()),index=False)
print('retrieved {0} collocation data'.format(species))
