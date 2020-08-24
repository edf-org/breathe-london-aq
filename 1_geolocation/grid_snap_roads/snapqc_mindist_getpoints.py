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
import pandas as pd
import datetime as dt
import numpy as np
import sys

#BQ connection
credentials = service_account.Credentials.from_service_account_file(r"..\..\..\pks\street-view-air-quality-london-dc8f329b26cf.json")
project_id = 'street-view-air-quality-london'
client = bigquery.Client(credentials=credentials,project=project_id)

#true for all queries in this process
location = 'EU'

#read drive days
#df = pd.read_csv(fpath,parse_dates=['timestamp'])
dfdates=pd.read_csv(r"..\..\..\..\Data\Intermediate\geolocation\testmindistance\bq-results-20200624-211526-d9nmi4juqpt.csv")
period_list = dfdates.sort_values(by=['dt'])['dt'].unique()
#plots
n_period = len(period_list)

#loop each period saving points
for p in period_list: 
    print(p)
    #query data
    qry_str = """
    SELECT a.vid, EXTRACT(date from a.timestamp) as dt, a.timestamp, mindist_m
, ST_X(geopt) as longitude, ST_Y(geopt) as latitude 
, no2_ppb, satellite_visible, satellite_accuracy_horizontal, satellite_tracking
FROM UK.snap2_ptclosestroad_lookup a
    join UK.snapz_distinctdrivepts b on a.timestamp = b.timestamp and a.pid = b.pid
    join (select value as no2_ppb, timestamp, vid from UK.stage3_test where parameter_id = 133 and mmode = 1) c on a.timestamp = c.timestamp and a.vid = c.vid
    join (select value as satellite_visible, timestamp, vid from UK.stage3_test where parameter_id = 149 and mmode = 1) d on a.timestamp = d.timestamp and a.vid = d.vid
    join (select value as satellite_accuracy_horizontal, timestamp, vid from UK.stage3_test where parameter_id = 154 and mmode = 1) e on a.timestamp = e.timestamp and a.vid = e.vid
    join (select value as satellite_tracking, timestamp, vid from UK.stage3_test where parameter_id = 150 and mmode = 1) f on a.timestamp = f.timestamp and a.vid = f.vid
    where EXTRACT(date from a.timestamp)= '{0}' 
    order by timestamp
    """.format(p)
    qry_job = client.query(qry_str,location=location,job_config=bigquery.QueryJobConfig())
    qry_job.to_dataframe().to_csv(r'..\..\..\..\Data\Intermediate\geolocation\testmindistance\driveshifts\bq_results_points_{0}.csv'.format(p))
