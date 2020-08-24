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
import numpy as np
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
dataset_ref = bqclient.dataset(dataset_str)
job_config = bigquery.QueryJobConfig()
job_config.write_disposition = 'WRITE_TRUNCATE' #overwrite table if exists

register_matplotlib_converters()
fig2,ax2 = plt.subplots()
fig2.set_size_inches(6,8)
dist_list = []
car_list = ['27522','27533']
for i,car in enumerate(car_list):
    #first get the times when the GPS and logger time are within 1 second
    #save to temp table
    destinationtable_str = 'timeeval_gps_'+car
    table_ref = dataset_ref.table(destinationtable_str)
    job_config.destination = table_ref
    #select gps and logger timestamps within x seconds, accounting for different clocks 
    qry_str = (r"""
        with cte0 as (
        select vid, timestamp, parameter_id, 
        CAST(REPLACE(id,'\n','') AS int64) as intid from UK.raw_data
        where vid={0} AND parameter_id in (1,21,32,45,57,144)
        ),
        cte1 as (
        SELECT timestamp, parameter_id, intid, row_number() over (order by intid) as timeidx from cte0
        WHERE timestamp>='2019-01-06T00:00:00'
        ),
        cte2 as (
        select timestamp as loggertime, intid as loggerid, timeidx from cte1 
        where parameter_id<>144
        ),
        cte3 as (
        select timestamp as gpstime, intid as gpsid, timeidx-1 as timeidx --shift back 1
        from cte1
        where parameter_id=144
        )
        select loggertime, gpstime, DATETIME_DIFF(loggertime, gpstime, SECOND) AS deltat, loggerid, gpsid
        ,cte2.timeidx 
        from cte2
        join cte3 on cte2.timeidx = cte3.timeidx
        WHERE DATETIME_DIFF(loggertime, gpstime, SECOND) == 0
        order by cte2.timeidx
        """.format(car))
    qry_job = bqclient.query(qry_str,location='EU',job_config=job_config)
    #join the logger and gps data in logger time
    qry_str = (r"""
        with ctei as (
        select timestamp as loggertime, value, parameter_id from UK.raw_data a
        join UK.{0} b on a.timestamp = b.loggertime
        where vid = {1} and parameter_id in (120, 121)
        union all
        --convert gps to logger time
        select loggertime, value, parameter_id from UK.raw_data a
        join UK.{0} b on a.timestamp = b.gpstime
        where vid = {1} and parameter_id in (144, 145)
        ),
        cte0 as (
        select loggertime, value as gps_lat from ctei
        where parameter_id = 144
        ),
        cte1 as (
        select a.loggertime, gps_lat, value as gps_lon from ctei a
        join cte0 on a.loggertime=cte0.loggertime
        where parameter_id = 145
        ),
        cte2 as (
        select loggertime, value as ae33_lat from ctei
        where parameter_id = 120
        ),
        cte3 as (
        select a.loggertime, ae33_lat, value as ae33_lon from ctei a
        join cte2 on a.loggertime=cte2.loggertime
        where parameter_id = 121
        )
        select cte3.loggertime
        ,gps_lat, gps_lon, ae33_lat, ae33_lon
        from cte3 join cte1 on cte3.loggertime = cte1.loggertime
            """.format(destinationtable_str,car))
    #print(qry_str)

    qry_job = bqclient.query(qry_str,location='EU',job_config=bigquery.QueryJobConfig())
    timestamp, gps_lat, gps_lon, ae33_lat, ae33_lon = zip(*qry_job.result())
    get_x = lambda lat,lon:111111*np.cos(lat*np.pi/180)*lon
    get_y = lambda lat:111111*lat
    ae33_x = np.array([get_x(z[0],z[1]) for z in zip(ae33_lat,ae33_lon)])
    gps_x = np.array([get_x(z[0],z[1]) for z in zip(gps_lat,gps_lon)])
    ae33_y = np.array([get_y(z) for z in ae33_lon])
    gps_y = np.array([get_y(z) for z in gps_lon])
    distance = np.sqrt((ae33_x-gps_x)**2+(ae33_y-gps_y)**2)
    dist_list.append(distance)
    df = pd.DataFrame(zip(timestamp, gps_lat, gps_lon, ae33_lat, ae33_lon,distance),columns=['timestamp','gps_lat','gps_lon','aer33_lat','ae33_lon','dist_m'])
    meandist_df = df[['gps_lat','gps_lon','dist_m']].groupby(['gps_lat','gps_lon']).quantile(.9).reset_index()
    meandist_df.to_csv(r'C:\Users\lpadilla\Documents\London\Data\Intermediate\timeshift_analysis\mean_ae33_gps_distance'+car+'.csv')
    
ax2.boxplot(dist_list,whis=[1,99])
ax2.plot([0,4],[45,45],'k--')
ax2.set_xticklabels(car_list)
ax2.set_ylim(-10,500)
ax2.set_ylabel('Difference between AE33 and GPS (m)')
fig2.savefig('gps_ae33_boxplot_'+car+dt.date.today().strftime('_%y%b%d')+'.png',dpi=300)
plt.show()
