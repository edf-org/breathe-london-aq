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
import pandas as pd

credentials = service_account.Credentials.from_service_account_file(
    r"..\..\pks\street-view-air-quality-london-dc8f329b26cf.json")
project_id = 'street-view-air-quality-london'
bqclient = bigquery.Client(credentials=credentials,project=project_id)
job_config = bigquery.QueryJobConfig()

qry_str = ("""
    with cte0 as (
    --all data, median of ULEZ pods
    select date_UTC, pod_str, ANY_VALUE(no2_ppb) as no2_ppb
    from (select date_UTC, 'ulezmed_a' as pod_str, percentile_cont(no2_ppb,0.5) OVER (partition by date_UTC) as no2_ppb
    from AQMesh.NO2_scaled_hightimeres_ppb_20180901_20190630 a
    join AQMesh.NO2_site_metadata_v2_1_20180901_20190630 b
    on a.pod_id_location=b.pod_id_location
    where ULEZ = true and a.pod_id_location in 
      --limit to pods with at least 6000 hours
      (select pod_id_location from (select pod_id_location,count(date) as hr_ct
      from AQMesh.NO2_scaled_ppb_20180901_20190630
      where no2_ppb <> -999
      group by pod_id_location) where hr_ct >=6000))
    group by date_UTC, pod_str
    )
    ,cte1 as (
    --mobile timestamps and n drives by segid
    select segid, TIMESTAMP_TRUNC(TIMESTAMP_ADD(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',passtime), INTERVAL 30 SECOND),MINUTE) as mobiletime --format string to timestamp and round nearest 1 min
    from UK.snap3_drivepass_means
    cross join unnest(passtime_arr) passtime
    where parameter_id = 133 --no2
    )
    ,cte2 as (
    --calc medians for all and subset times
    select pod_str, segid, COUNT(med_subset) as n_passes --n matches
    , ANY_VALUE(med_a) as med_a, ANY_VALUE(med_subset) as med_subset, ANY_VALUE((med_subset-med_a)/med_a*100) as bias_pct
    from (select hour1stamp, pod_str
    , segid
    , percentile_cont(no2_ppb,0.5) OVER (partition by pod_str) as med_a
    , percentile_cont(no2_ppb,0.5) OVER (partition by pod_str, segid) as med_subset
    --take med of any 1hr periods with multiple measurements
    from (select hour1stamp, pod_str, segid, ANY_VALUE(no2_1hrmed) as no2_ppb from (select TIMESTAMP_TRUNC(date_UTC,HOUR) as hour1stamp, pod_str, cte1.segid, percentile_cont(no2_ppb,0.5) over (partition by pod_str, segid, TIMESTAMP_TRUNC(date_UTC,HOUR)) as no2_1hrmed from cte0
left join cte1 on cte0.date_UTC = cte1.mobiletime) group by hour1stamp, pod_str, segid)) 
    where segid is not null
    group by pod_str, segid
    )
    --join to geometry 
    select cte2.pod_str, cte2.segid, cte2.n_passes, cte2.med_a, cte2.med_subset, cte2.bias_pct, ST_ASTEXT(b.geowkt) as the_geom from cte2
    join UK.greaterlondon_roads_wgs84_split30m b
    on cte2.segid = b.segid
""")

qry_job = bqclient.query(qry_str,location='EU',job_config=job_config)
#save result as dataframe
df_a = qry_job.to_dataframe()
df_a.to_csv(r'.\charts\map.csv')

