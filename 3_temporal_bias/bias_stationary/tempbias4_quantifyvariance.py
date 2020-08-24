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
    --all data, indiv pods in ULEZ
    select cast(pod_id_location as STRING) as pod_str, row_number() over() as pod_idx, ANY_VALUE(med)/ANY_VALUE(med)*100 as med
    , ANY_VALUE(p25)/ANY_VALUE(med)*100 as p25
    , ANY_VALUE(p75)/ANY_VALUE(med)*100 as p75
    , ANY_VALUE(p05)/ANY_VALUE(med)*100 as p05
    , ANY_VALUE(p95)/ANY_VALUE(med)*100 as p95
    from
    (
        select pod_id_location
        , percentile_cont(no2_ppb,0.5) over (partition by pod_id_location) as med
        , percentile_cont(no2_ppb,0.25) over (partition by pod_id_location) as p25
        , percentile_cont(no2_ppb,0.75) over (partition by pod_id_location) as p75
        , percentile_cont(no2_ppb,0.05) over (partition by pod_id_location) as p05
        , percentile_cont(no2_ppb,0.95) over (partition by pod_id_location) as p95
        from
        (
            select date_UTC, a.pod_id_location, no2_ppb
            from AQMesh.NO2_scaled_hightimeres_ppb_20180901_20190630 a
            join AQMesh.NO2_site_metadata_v2_1_20180901_20190630 b
            on a.pod_id_location=b.pod_id_location
            where ULEZ = true and no2_ppb <> -999
        )
    )
    group by pod_id_location
""")
qry_job = bqclient.query(qry_str,location='EU',job_config=job_config)
#save result as dataframe
df = qry_job.to_dataframe()
df_long = df.melt(id_vars=['pod_str','pod_idx'],value_vars=['p05','p25','med','p75','p95'], var_name = 'yparam',value_name = 'value')
#plots
#plt1 = gg.ggplot(df, gg.aes(x='date_UTC',y='no2_ppb'))+gg.geom_line()+gg.xlab('Time')+gg.ylab('NO2 (ppb)')+gg.theme_bw()+gg.facet_wrap('pod_id_location',scales='free_y')
#plt1.save(filename = r'.\charts\ulezpodts.png', width=None, height=None, dpi=200)
plt2 = gg.ggplot(df_long, gg.aes(x='pod_str',y='value',color='yparam'))+gg.geom_point()+gg.xlab('pod')+gg.ylab('NO2 (as % of median)')+gg.theme_bw()+gg.theme(figure_size=(12,6))+gg.scale_x_discrete()
plt2.save(filename = r'.\charts\ulezpodvar.png', width=10, height=6, dpi=200)

#repeat for mobile data using segid instead of podid where N = 10 and N = 40
#repeat for stationary data at mobile times
qry_str = ("""
    with cte0 as (
    --all data, ULEZ pods with 6000 hrs
    select date_UTC, a.pod_id_location, no2_ppb
    from AQMesh.NO2_scaled_hightimeres_ppb_20180901_20190630 a
    join AQMesh.NO2_site_metadata_v2_1_20180901_20190630 b
    on a.pod_id_location=b.pod_id_location
    where ULEZ = true and no2_ppb <> -999
    and a.pod_id_location in 
      --limit to pods with at least 6000 hours
      (select pod_id_location from (select pod_id_location,count(date) as hr_ct
      from AQMesh.NO2_scaled_ppb_20180901_20190630
      where no2_ppb <> -999
      group by pod_id_location) where hr_ct >=6000)
    )
    ,cte1 as (
    --mobile timestamps and n drives by segid
    select a.segid, TIMESTAMP_TRUNC(TIMESTAMP_ADD(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',a.passtime), INTERVAL 30 SECOND),MINUTE) as mobiletime --format string to timestamp and round nearest 1 min
    , b.passmean from
        (select segid, passtime, idx from UK.snap3_drivepass_means a
        cross join unnest(passtime_arr) passtime with offset as idx
        where parameter_id = 133) a
        join 
        (select segid, passmean, idx from UK.snap3_drivepass_means a
        cross join unnest(passmean_arr) passmean with offset as idx
        where parameter_id = 133) b
        on a.segid = b.segid and a.idx = b.idx
    )
    ,cte2 as (
    --calc medians for all and subset times
    select pod_id_location, segid
    , COUNT(med_pod) as n_passes --n matches
    , ANY_VALUE(med_pod)/ANY_VALUE(med_pod)*100 as med
    , ANY_VALUE(p25_pod)/ANY_VALUE(med_pod)*100 as p25
    , ANY_VALUE(p75_pod)/ANY_VALUE(med_pod)*100 as p75
    , ANY_VALUE(p05_pod)/ANY_VALUE(med_pod)*100 as p05
    , ANY_VALUE(p95_pod)/ANY_VALUE(med_pod)*100 as p95
    , ANY_VALUE(med_mob)/ANY_VALUE(med_mob)*100 as med_mob
    , ANY_VALUE(p25_mob)/ANY_VALUE(med_mob)*100 as p25_mob
    , ANY_VALUE(p75_mob)/ANY_VALUE(med_mob)*100 as p75_mob
    , ANY_VALUE(p05_mob)/ANY_VALUE(med_mob)*100 as p05_mob
    , ANY_VALUE(p95_mob)/ANY_VALUE(med_mob)*100 as p95_mob
    from (select hour1stamp, pod_id_location, segid
        , percentile_cont(no2_pod,0.5) over (partition by pod_id_location, segid) as med_pod
        , percentile_cont(no2_pod,0.25) over (partition by pod_id_location, segid) as p25_pod
        , percentile_cont(no2_pod,0.75) over (partition by pod_id_location, segid) as p75_pod
        , percentile_cont(no2_pod,0.05) over (partition by pod_id_location, segid) as p05_pod
        , percentile_cont(no2_pod,0.95) over (partition by pod_id_location, segid) as p95_pod
        , percentile_cont(no2_mob,0.5) over (partition by pod_id_location, segid) as med_mob
        , percentile_cont(no2_mob,0.25) over (partition by pod_id_location, segid) as p25_mob
        , percentile_cont(no2_mob,0.75) over (partition by pod_id_location, segid) as p75_mob
        , percentile_cont(no2_mob,0.05) over (partition by pod_id_location, segid) as p05_mob
        , percentile_cont(no2_mob,0.95) over (partition by pod_id_location, segid) as p95_mob
        --take med of any 1hr periods with multiple measurements
        from (select hour1stamp, pod_id_location, segid
        , ANY_VALUE(no2_1hrmed_pod) as no2_pod
        , ANY_VALUE(no2_1hrmed_mob) as no2_mob
            from (
              select TIMESTAMP_TRUNC(date_UTC,HOUR) as hour1stamp, pod_id_location, cte1.segid
              , percentile_cont(no2_ppb,0.5) over (partition by pod_id_location, segid, TIMESTAMP_TRUNC(date_UTC,HOUR)) as no2_1hrmed_pod 
              , percentile_cont(passmean,0.5) over (partition by pod_id_location, segid, TIMESTAMP_TRUNC(date_UTC,HOUR)) as no2_1hrmed_mob 
              from cte0
              join cte1 on cte0.date_UTC = cte1.mobiletime
            ) 
        group by hour1stamp, pod_id_location, segid
        )
    ) 
    group by pod_id_location, segid
    )
    select row_number() over () as pod_idx, pod_id_location, segid, med, p25,p75, p05, p95 
    from 
    (
    select pod_id_location, segid, med, p25,p75, p05, p95 from cte2
    where n_passes >=100
    union all
    select distinct 0 as pod_id_location, segid, med_mob as med, p25_mob as p25, p75_mob as p75, p05_mob as p05, p95_mob as p95
    from cte2
    where n_passes >= 100
    )
""")
qry_job = bqclient.query(qry_str,location='EU',job_config=bigquery.QueryJobConfig())
#save result as dataframe
df = qry_job.to_dataframe()
df_long = df.melt(id_vars=['pod_id_location','segid','pod_idx'],value_vars=['p05','p25','med','p75','p95'], var_name = 'yparam',value_name = 'value')
df_long.to_csv(r'.\charts\subsetdistribs.csv')
#plots
#plt1.save(filename = r'.\charts\ulezpodts.png', width=None, height=None, dpi=200)
plt2 = gg.ggplot(df_long, gg.aes(x='pod_idx',y='value',color='yparam'))+gg.geom_point()+gg.geom_line()+gg.xlab('pod/segment')+gg.ylab('NO2 (as % of median)')+gg.theme_bw()
plt2.save(filename = r'.\charts\ulezsubsetvar.png', width=None, height=None, dpi=200)
