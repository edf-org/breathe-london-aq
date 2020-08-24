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

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
from pandas.plotting import register_matplotlib_converters
import datetime as dt
import os
import sys
import glob
from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    r"C:\Users\lpadilla\Documents\London\Scripts\pks\street-view-air-quality-london-dc8f329b26cf.json")
project_id = 'street-view-air-quality-london'
bqclient = bigquery.Client(credentials=credentials,project=project_id)

dataset_str = 'UK'

register_matplotlib_converters()
fig,ax = plt.subplots(2,1)
fig.set_size_inches(6,8)

for i,car in enumerate(['27522','27533']):
    qry_str = ("""SELECT DATETIME_TRUNC(timestamp,DAY) as daystamp, MIN(id) FROM UK.raw_data 
    WHERE parameter_id = 3 
    AND vid = {0}
    AND timestamp > '2018-09-25'
    GROUP BY daystamp
    ORDER BY daystamp 
    """.format(car))
    qry_job = bqclient.query(qry_str,location='EU',job_config=bigquery.QueryJobConfig())
    rows = qry_job.result()
    timestamp,dbid=zip(*rows)
    dbid_int = [int(x.replace('\n','')) for x in dbid]

    qry_str = ("""SELECT DATETIME_TRUNC(timestamp,DAY) as daystamp, MIN(id) FROM UK.raw_data 
    WHERE parameter_id = 127 
    AND vid = {0}
    AND timestamp > '2018-09-25'
    GROUP BY daystamp
    ORDER BY daystamp 
    """.format(car))
    qry_job = bqclient.query(qry_str,location='EU',job_config=bigquery.QueryJobConfig())
    rows = qry_job.result()
    timestamp127,dbid127=zip(*rows)
    dbid127_int = [int(x.replace('\n','')) for x in dbid127]

    ax[i].plot(timestamp,dbid_int,'b')
    ax[i].plot(timestamp127,dbid127_int,'g')
    ax[i].legend(['pm2.5','o3'])
    #fig.autofmt_xdate()
    ax[i].xaxis.set_major_formatter(mdates.DateFormatter('%b'))
    ax[i].xaxis.set_minor_formatter(mdates.DateFormatter('%d'))
    ax[i].xaxis.set_major_locator(mdates.MonthLocator())
    ax[i].xaxis.set_minor_locator(mdates.DayLocator(bymonthday=[8,15,22]))
    ax[i].set_ylabel('Database ID')
    ax[i].set_title('Vehicle '+car)
fig.savefig('id_ts_'+car+dt.date.today().strftime('_%y%b%d')+'.png',dpi=300)
plt.show()
