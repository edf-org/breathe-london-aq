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
    --all data, indiv pods in ULEZ
    select date_UTC, CONCAT(cast(a.pod_id_location as STRING),'_b') as pod_str, no2_ppb
    from AQMesh.NO2_scaled_hightimeres_ppb_20180901_20190630 a
    join AQMesh.NO2_site_metadata_v2_1_20180901_20190630 b
    on a.pod_id_location=b.pod_id_location
    where ULEZ = true and a.pod_id_location in 
      --limit to pods with at least 6000 hours
      (select pod_id_location from (select pod_id_location ,count(date) as hr_ct
      from AQMesh.NO2_scaled_ppb_20180901_20190630
      where no2_ppb <> -999
      group by pod_id_location) where hr_ct >=6000)
    union all 
    --all data, median of ULEZ pods
    select date_UTC, pod_str, ANY_VALUE(no2_ppb) as no2_ppb
    from (select date_UTC, 'ulezmed_b' as pod_str, percentile_cont(no2_ppb,0.5) OVER (partition by date_UTC) as no2_ppb
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
left join cte1 on cte0.date_UTC = cte1.mobiletime) group by hour1stamp, pod_str, segid) 
    --restrict stationary data to daytime, weekday
    where EXTRACT(HOUR from hour1stamp) not in (0.1,2,3) and EXTRACT(DAYOFWEEK from hour1stamp) not in (1,7))
    where segid is not null
    group by pod_str, segid
    )
    --summarize biases across segments at different # of passes and choice of stationary data
    select pod_str, n_passes, count(n_passes) as n_segments, ANY_VALUE(p05) as p05, ANY_VALUE(p25) as p25, ANY_VALUE(p50) as p50, ANY_VALUE(p75) as p75, ANY_VALUE(p95) as p95
    from (
    select pod_str, n_passes
    ,percentile_cont(bias_pct,0.01) OVER (partition by pod_str, n_passes) as p05
    ,percentile_cont(bias_pct,0.25) OVER (partition by pod_str, n_passes) as p25
    ,percentile_cont(bias_pct,0.50) OVER (partition by pod_str, n_passes) as p50
    ,percentile_cont(bias_pct,0.75) OVER (partition by pod_str, n_passes) as p75
    ,percentile_cont(bias_pct,0.99) OVER (partition by pod_str, n_passes) as p95
    from cte2
    )
    group by pod_str, n_passes
""")

qry_job = bqclient.query(qry_str,location='EU',job_config=job_config)
#save result as dataframe
df_a = qry_job.to_dataframe()
df_along = df_a.melt(id_vars=['pod_str','n_passes'],value_vars=['p05','p25','p50','p75','p95'], var_name = 'yparam',value_name = 'value')
#plots
plt1 = gg.ggplot(df_along, gg.aes(x='n_passes',y='value',color='pod_str'))+gg.geom_point()+gg.xlab('N drives')+gg.ylab('Bias (%)')+gg.theme_bw()+gg.xlim(0,100)+gg.facet_wrap('yparam',scales='free_y')
plt1.save(filename = r'.\charts\bias_restricted.png', width=None, height=None, dpi=200)
#n_segments
plt2 = gg.ggplot(df_a, gg.aes(x='n_passes',y='n_segments',color='pod_str'))+gg.geom_point()+gg.xlab('N drives')+gg.ylab('Sample size (number of segments)')+gg.theme_bw()+gg.xlim(0,100)+gg.ylim(0,500)
plt2.save(filename = r'.\charts\n_segments_restricted.png', width=None, height=None, dpi=200)
