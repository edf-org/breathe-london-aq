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
fig2,ax2 = plt.subplots()
fig2.set_size_inches(6,8)
deltat_list = []
car_list = ['27522','27533']
for i,car in enumerate(car_list):
    qry_str = (r"""
    with cte0 as (
    select vid, timestamp, parameter_id, 
    CAST(REPLACE(id,'\n','') AS int64) as intid from UK.raw_data
    where vid={0} AND parameter_id in (1,21,32,45,57,127)
    ),
    cte1 as (
    SELECT timestamp, parameter_id, intid, row_number() over (order by intid) as timeidx from cte0
    --WHERE timestamp>='2019-01-06T00:00:00'
    WHERE timestamp>='2019-04-12'
    ),
    cte2 as (
    select timestamp as loggertime, intid as loggerid, timeidx from cte1 
    where parameter_id<>127
    ),
    cte3 as (
    select timestamp as ozonetime, intid as ozoneid, timeidx-1 as timeidx --shift back 1
    from cte1
    where parameter_id=127
    )
    select loggertime, ozonetime, DATETIME_DIFF(loggertime, ozonetime, SECOND) AS deltat, loggerid, ozoneid
    ,cte2.timeidx 
    from cte2
    join cte3 on cte2.timeidx = cte3.timeidx
    order by cte2.timeidx
    """.format(car))
    #print(qry_str)

    qry_job = bqclient.query(qry_str,location='EU',job_config=bigquery.QueryJobConfig())
    rows = qry_job.result()
    loggertime, ozonetime, deltat, loggerid, ozoneid, timeid =zip(*rows)
    deltat_list.append(deltat)

    ax[i].plot(loggertime, deltat,'.')
    ax[i].plot([loggertime[0],loggertime[-1]],[1,1],'k--')
    ax[i].plot([loggertime[0],loggertime[-1]],[-1,-1],'k--')
    ax[i].set_ylabel('Logger minus ozone time (s)')
    ax[1].set_xlabel('Logger time')
    ax[i].set_title('Vehicle '+car)
    ax[i].xaxis.set_major_formatter(mdates.DateFormatter('%b'))
    ax[i].xaxis.set_minor_formatter(mdates.DateFormatter('%d'))
    ax[i].xaxis.set_major_locator(mdates.MonthLocator())
    ax[i].xaxis.set_minor_locator(mdates.DayLocator(bymonthday=[8,15,22]))
    ax[i].set_ylim(-20,20)

fig.savefig('ozoneshift_ts_'+car+dt.date.today().strftime('_%y%b%d')+'.png',dpi=300)

ax2.boxplot(deltat_list,whis=[1,99])
ax2.plot([0,4],[1,1],'k--')
ax2.plot([0,4],[-1,-1],'k--')
ax2.set_xticklabels(car_list)
ax2.set_ylim(-20,20)
ax2.set_ylabel('Logger minus ozone time (s)')
fig2.savefig('ozoneshift_boxplot_'+car+dt.date.today().strftime('_%y%b%d')+'.png',dpi=300)
plt.show()
