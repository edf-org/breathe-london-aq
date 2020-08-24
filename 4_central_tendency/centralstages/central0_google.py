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

#Median mobile data for Breathe London Map
#Limit to Aug 31st and minimum of 10 drives

from google.cloud import bigquery
from google.oauth2 import service_account
import glob
import os
import sys
import datetime as dt

credentials = service_account.Credentials.from_service_account_file(r"..\..\..\pks\street-view-air-quality-london-dc8f329b26cf.json")
project_id = 'street-view-air-quality-london'
client = bigquery.Client(credentials=credentials,project=project_id)

#true for all queries in this process
dataset_ref = client.dataset('UK')
job_config = bigquery.QueryJobConfig()
#job_config.write_disposition = 'WRITE_TRUNCATE' #overwrite table if exists
location = 'EU'

###############################################################
 #centralx Placeholder for removing dead end segments
###############################################################
 #central0 Calculate median, stdev measurements and drive 1-hour counts over collection period
destinationtable_str = 'central0_drivesummarystats_4Google'
#job_config.destination = dataset_ref.table(destinationtable_str)
destinationcsv_str = os.path.join(r'..\..\..\..\Data\Intermediate\central_tendency',destinationtable_str+dt.date.today().strftime('_%y%b%d')+'.csv')
qry_str = ("""
with cte0 as (
select segid, parameter_id, median, stdev, drivehrct, passtime_arr, passmean_arr from (
select segid, parameter_id, ANY_VALUE(median) as median, STDDEV_SAMP(value_median) as stdev, COUNT(value_median) as drivehrct 
--aggregate hourend time and N-hr median pass avg values into arrays
,ARRAY_AGG(FORMAT_DATETIME("%Y-%m-%d %H:%M:%S",hour1stamp) order by hour1stamp) as passtime_arr
,ARRAY_AGG(value_median order by hour1stamp) as passmean_arr
from 
  (select segid, parameter_id, value_median, hour1stamp
  ,percentile_cont(value_median, 0.5) OVER (PARTITION BY segid, parameter_id) as median 
  from UK.snap4_drivepass_1hr_medians)
group by segid, parameter_id
)
where drivehrct >= 10
--order by segid, parameter_id
--then pivot out the parameters of interest
)
select a.segid as segid 
--, polyid
, a.geowkt as the_geom, a.function, a.name1
--, a.numbertoid
--,pm2_5_med as pm2_5_med_ugm3, pm2_5_drvct
--,co2_med, co2_drvct
--,no_med, no_drvct
--,bc_med, bc_drvct
--,o3_med, o3_drvct
,no2_med as no2_med_ppb
, no2_med*(1/0.52) as no2_med_ugm3, no2_drvct
--,noxasno2_med, noxasno2_drvct
,speedkmh_med, speedkmh_drvct
--,pm2_5time_arr, pm2_5mean_arr
--,no2time_arr, no2ugm3_arr
from UK.greaterlondon_roads_wgs84_split30m a
left join (select segid, median as pm2_5_med, stdev as pm2_5_std, drivehrct as pm2_5_drvct
,passtime_arr as pm2_5time_arr, passmean_arr as pm2_5mean_arr
from cte0
where parameter_id = 140) b
on a.segid=b.segid
left join (select segid, median as co2_med, stdev as co2_std, drivehrct as co2_drvct from cte0
where parameter_id = 32) c
on a.segid=c.segid
left join (select segid, median as no_med, stdev as no_std, drivehrct as no_drvct from cte0
where parameter_id = 45) d
on a.segid=d.segid
left join (select segid, median as bc_med, stdev as bc_std, drivehrct as bc_drvct from cte0
where parameter_id = 62) e
on a.segid=e.segid
left join (select segid, median as o3_med, stdev as o3_std, drivehrct as o3_drvct from cte0
where parameter_id = 127) f
on a.segid=f.segid
left join (select segid, ANY_VALUE(median) as no2_med, ANY_VALUE(stdev) as no2_std, ANY_VALUE(drivehrct) as no2_drvct
, ANY_VALUE(passtime_arr) as no2time_arr, ANY_VALUE(passmean_arr) as no2mean_arr 
--convert units from ppb to ugm3 in unnested arrays and then reaggregate arrays
, ARRAY_AGG(no2_tmp*(1/0.52) order by idx) as no2ugm3_arr
from cte0
cross join unnest(passmean_arr) no2_tmp WITH OFFSET AS idx
where parameter_id = 133
group by segid) g
on a.segid=g.segid
left join (select segid, median as speedkmh_med, stdev as speedkmh_std, drivehrct as speedkmh_drvct from cte0
where parameter_id = 147) h
on a.segid=h.segid
left join (select segid, median as noxasno2_med, stdev as noxasno2_std, drivehrct as noxasno2_drvct from cte0
where parameter_id = 163) i
on a.segid=i.segid
where pm2_5_drvct is not null or co2_drvct is not null or no_drvct is not null or bc_drvct is not null
or o3_drvct is not null or no2_drvct is not null or speedkmh_drvct is not null
order by segid
        """)
qry_job = client.query(qry_str,location=location,job_config=job_config)
rows = qry_job.result()
#print("Query results loaded into table {0}".format(destinationtable_str))
qry_job.to_dataframe().to_csv(destinationcsv_str,index=False)
print("Query results saved as csv {0}".format(destinationcsv_str))
