#place to store common sql for repeat use

#1 min AQMesh NO2 data, starts in April 2019
def cte0_str_no2_aqmesh1min():
    return """
    --all data, indiv pods in ULEZ
    select date_UTC as tstamp, CONCAT(cast(a.pod_id_location as STRING),'_a') as site_str, no2_ppb as meas
    from AQMesh.NO2_scaled_hightimeres_ppb_20180901_20190630 a
    join AQMesh.NO2_site_metadata_v2_1_20180901_20190630 b
    on a.pod_id_location=b.pod_id_location
    where ULEZ = true and a.pod_id_location in 
      --limit to pods with at least 99% of minutes
      (select pod_id_location from (select pod_id_location ,count(date_UTC) as min_ct
      from AQMesh.NO2_scaled_hightimeres_ppb_20180901_20190630
      where no2_ppb <> -999 and date_UTC >= '2019-04-06'
      group by pod_id_location) where min_ct >=123717*0.99)
    and date_UTC >= '2019-04-06'
    union all 
    --all data, median of ULEZ pods
    select date_UTC as tstamp, pod_str as site_str, ANY_VALUE(no2_ppb) as meas
    from (select date_UTC, 'ulezmed_a' as pod_str, percentile_cont(no2_ppb,0.5) OVER (partition by date_UTC) as no2_ppb
    from AQMesh.NO2_scaled_hightimeres_ppb_20180901_20190630 a
    join AQMesh.NO2_site_metadata_v2_1_20180901_20190630 b
    on a.pod_id_location=b.pod_id_location
    where ULEZ = true and a.pod_id_location in 
      --limit to pods with at least 99% of minutes
      (select pod_id_location from (select pod_id_location ,count(date_UTC) as min_ct
      from AQMesh.NO2_scaled_hightimeres_ppb_20180901_20190630
      where no2_ppb <> -999 and date_UTC >= '2019-04-06'
      group by pod_id_location) where min_ct >=123717*0.99)
    and date_UTC >= '2019-04-06')
    group by tstamp, site_str
"""

#LAQN NO2 data for full period
def cte0_str_no2_laqn15min():
    return """
    --select sites in ULEZ
    select PARSE_TIMESTAMP('%d/%m/%Y %H:%M',ReadingDateTime) as tstamp, CONCAT(Site,'_a') as site_str, Value as meas
    from LAQN.no2_20180801_20190831 a
"""

#LAQN PM2.5 data for full period
def cte0_str_pm25_laqn15min():
    return """
    --select sites in ULEZ
    select PARSE_TIMESTAMP('%d/%m/%Y %H:%M',ReadingDateTime) as tstamp, CONCAT(Site,'_a') as site_str, Value as meas
    from LAQN.pm25_20180801_20190831 a
"""

#1 hour average AQMesh NO2 data for full period 
def cte0_str_no2_aqmesh1hr():
    return """
    --all data, indiv pods in ULEZ
    select date as tstamp, CONCAT(cast(a.pod_id_location as STRING),'_a') as site_str, no2_ppb as meas
    from AQMesh.NO2_scaled_ppb_20180901_20190630 a
    join AQMesh.NO2_site_metadata_v2_1_20180901_20190630 b
    on a.pod_id_location=b.pod_id_location
    where ULEZ = true and no2_ppb <> -999 and a.pod_id_location in 
      --limit to pods with at least 6000 hours
      (select pod_id_location from (select pod_id_location ,count(date) as hr_ct
      from AQMesh.NO2_scaled_ppb_20180901_20190630
      where no2_ppb <> -999
      group by pod_id_location) where hr_ct >=6000)
    union all 
    --all data, median of ULEZ pods
    select date as tstamp, pod_str as site_str, ANY_VALUE(no2_ppb) as meas
    from (select date, 'ulezmed_a' as pod_str, percentile_cont(no2_ppb,0.5) OVER (partition by date) as no2_ppb
    from AQMesh.NO2_scaled_ppb_20180901_20190630 a
    join AQMesh.NO2_site_metadata_v2_1_20180901_20190630 b
    on a.pod_id_location=b.pod_id_location
    where ULEZ = true and no2_ppb <> -999 and a.pod_id_location in 
      --limit to pods with at least 6000 hours
      (select pod_id_location from (select pod_id_location ,count(date) as hr_ct
      from AQMesh.NO2_scaled_ppb_20180901_20190630
      where no2_ppb <> -999
      group by pod_id_location) where hr_ct >=6000)
    )
    group by tstamp, site_str
"""
def qry_str_template_a():
    return """
    with cte0 as (
    {0}
    )
    ,cte1 as (
    --mobile timestamps and n drives by segid
    select segid, 
    {1} as mobiletime
    from UK.snap3_drivepass_means
    cross join unnest(passtime_arr) passtime
    where parameter_id = {2} --meas id
    )
    ,cte2 as (
    --calc medians for all and subset times
    select site_str, segid, COUNT(med_subset) as n_passes --n matches
    , ANY_VALUE(med_a) as med_a, ANY_VALUE(med_subset) as med_subset, ANY_VALUE((med_subset-med_a)/med_a*100) as bias_pct
    from (select hour1stamp, site_str
    , segid
    , percentile_cont(meas,0.5) OVER (partition by site_str) as med_a
    , percentile_cont(meas,0.5) OVER (partition by site_str, segid) as med_subset
    --take med of any 1hr periods with multiple measurements
    from (select hour1stamp, site_str, segid, ANY_VALUE(meas_1hrmed) as meas from (select TIMESTAMP_TRUNC(tstamp,HOUR) as hour1stamp, site_str, cte1.segid, percentile_cont(meas,0.5) over (partition by site_str, segid, TIMESTAMP_TRUNC(tstamp,HOUR)) as meas_1hrmed from cte0
left join cte1 on cte0.tstamp = cte1.mobiletime) group by hour1stamp, site_str, segid)) 
    where segid is not null
    group by site_str, segid
    )
    --summarize biases across segments at different # of passes and choice of stationary data
    select site_str, n_passes, count(n_passes) as n_segments, ANY_VALUE(p05) as p05, ANY_VALUE(p25) as p25, ANY_VALUE(p50) as p50, ANY_VALUE(p75) as p75, ANY_VALUE(p95) as p95
    from (
    select site_str, n_passes
    ,percentile_cont(bias_pct,0.05) OVER (partition by site_str, n_passes) as p05
    ,percentile_cont(bias_pct,0.25) OVER (partition by site_str, n_passes) as p25
    ,percentile_cont(bias_pct,0.50) OVER (partition by site_str, n_passes) as p50
    ,percentile_cont(bias_pct,0.75) OVER (partition by site_str, n_passes) as p75
    ,percentile_cont(bias_pct,0.95) OVER (partition by site_str, n_passes) as p95
    from cte2
    )
    group by site_str, n_passes
"""

def qry_str_template_b():
    return """
    with cte0 as (
    {0}
    )
    ,cte1 as (
    --mobile timestamps and n drives by segid
    select segid
    , {1} as mobiletime --format string to timestamp and round nearest X min
    from UK.snap3_drivepass_means
    cross join unnest(passtime_arr) passtime
    where parameter_id = {2} --meas
    )
    ,cte2 as (
    --calc medians for all and subset times
    select site_str, segid, COUNT(med_subset) as n_passes --n matches
    , ANY_VALUE(med_a) as med_a, ANY_VALUE(med_subset) as med_subset, ANY_VALUE((med_subset-med_a)/med_a*100) as bias_pct
    from (select hour1stamp, site_str
    , segid
    , percentile_cont(meas,0.5) OVER (partition by site_str) as med_a
    , percentile_cont(meas,0.5) OVER (partition by site_str, segid) as med_subset
    --take med of any 1hr periods with multiple measurements
    from (select hour1stamp, site_str, segid, ANY_VALUE(meas_1hrmed) as meas from (select TIMESTAMP_TRUNC(tstamp,HOUR) as hour1stamp, site_str, cte1.segid, percentile_cont(meas,0.5) over (partition by site_str, segid, TIMESTAMP_TRUNC(tstamp,HOUR)) as meas_1hrmed from cte0
left join cte1 on cte0.tstamp = cte1.mobiletime) group by hour1stamp, site_str, segid) 
    --restrict stationary data to daytime, weekday
    where EXTRACT(HOUR from hour1stamp) not in (0.1,2,3) and EXTRACT(DAYOFWEEK from hour1stamp) not in (1,7))
    where segid is not null
    group by site_str, segid
    )
    --summarize biases across segments at different # of passes and choice of stationary data
    select site_str, n_passes, count(n_passes) as n_segments, ANY_VALUE(p05) as p05, ANY_VALUE(p25) as p25, ANY_VALUE(p50) as p50, ANY_VALUE(p75) as p75, ANY_VALUE(p95) as p95
    from (
    select site_str, n_passes
    ,percentile_cont(bias_pct,0.01) OVER (partition by site_str, n_passes) as p05
    ,percentile_cont(bias_pct,0.25) OVER (partition by site_str, n_passes) as p25
    ,percentile_cont(bias_pct,0.50) OVER (partition by site_str, n_passes) as p50
    ,percentile_cont(bias_pct,0.75) OVER (partition by site_str, n_passes) as p75
    ,percentile_cont(bias_pct,0.99) OVER (partition by site_str, n_passes) as p95
    from cte2
    )
    group by site_str, n_passes
"""

