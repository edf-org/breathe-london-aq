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
dataset_str = 'UK'
srctable_str = dataset_str+".stage3_test"
dataset_ref = client.dataset(dataset_str)
job_config = bigquery.QueryJobConfig()
job_config.write_disposition = 'WRITE_TRUNCATE' #overwrite table if exists
location = 'EU'

#snap0 Determine distinct combinations of drive hours and points
destinationtable_str = 'snapz_distinctdrivepts'
job_config.destination = dataset_ref.table(destinationtable_str)
qry_str = ("""
--select distinct drive pts, efficiently create unique id, whereever valid data exists, lat and lon can't be null
select CONCAT(CAST(DATETIME_DIFF(timestamp,DATETIME(2018,1,1,0,0,0),SECOND) as STRING)
              ,'_',CAST(vid as STRING)) as pid
,vid ,timestamp ,ST_GEOGPOINT(longitude,latitude) as geopt from 
(select distinct vid, timestamp, latitude, longitude from {0}
where
--parameter_id in (144, 145) and
(latitude is not null) and (longitude is not null) and
(qc_flag in (0,2,21,22,23,24)) and (mmode=1 or mmode=2))
    """.format(srctable_str))
qry_job = client.query(qry_str,location=location,job_config=job_config)
rows = qry_job.result()
print("Query results loaded into table {0}".format(destinationtable_str))

###############################################################
#snap1 Find distance between drive hour points and road segments
destinationtable_str = 'snap1_distance_pt2road'
job_config.destination = dataset_ref.table(destinationtable_str)
qry_str = ("""
--distance between points and segments, restrict search to same polygon 
--and round pt lat/lon to within 60 to 100 m of segment centroid, HUGE timesaver
--from unknown hours to ~40s for 3 decimal matching and 18 min for 2 decimal matching
--200 m was the biggest minimum in the test polygons, happened near Millenium bridge across the river Thames when GPS seemed to go wild
select a.pid
, b.segid as segid
, ST_DISTANCE(geopt,geowkt) as distance_m 
from UK.snapz_distinctdrivepts a join UK.greaterlondon_roads_wgs84_split30m b
on ROUND(ST_X(a.geopt),3) = ROUND(ST_X(ST_CENTROID(b.geowkt)),3)
and ROUND(ST_Y(a.geopt),3) = ROUND(ST_Y(ST_CENTROID(b.geowkt)),3)
        """)
qry_job = client.query(qry_str,location=location,job_config=job_config)
rows = qry_job.result()
print("Query results loaded into table {0}".format(destinationtable_str))

###############################################################
 #snap2 Find segment with minimum distance to point and
 #assign a passid to unique passes of segments
destinationtable_str = 'snap2_ptclosestroad_lookup'
job_config.destination = dataset_ref.table(destinationtable_str)
qry_str = ("""
--find road that is closest to point
--this is the critical lookup table to get measurements on road segments that we can average by drive pass
--this was fast, 11 s for the select polygons
with cte as 
(
select a.pid
, min(a.segid) as segid, b.mindist_m, c.vid, c.timestamp from UK.snap1_distance_pt2road a
join 
  (
  select pid
  , min(distance_m) as mindist_m from UK.snap1_distance_pt2road
  group by pid
  ) b
on a.pid=b.pid 
and a.distance_m = b.mindist_m
join UK.snapz_distinctdrivepts c
on a.pid = c.pid
--this grouping is to remove points that matched to two segments, we choose minimum segid
group by pid
, mindist_m, vid, timestamp
)
--then assign drive pass id based on when the preceding segment does not equal the current segment
--i.e. the segid changed with the current point
select vid
, segid, pid, mindist_m, timestamp, preceding_segid,
sum(IF(sq.preceding_segid = sq.segid,0,1)) over(partition by vid, segid order by sq.timestamp) as passid
from (
    select vid, segid, pid, mindist_m, timestamp
    ,lag(segid, 1, null) over(partition by vid, FORMAT_DATETIME('%Y%m',timestamp) order by timestamp) as preceding_segid
    from cte
) sq
        """)
qry_job = client.query(qry_str,location=location,job_config=job_config)
rows = qry_job.result()
print("Query results loaded into table {0}".format(destinationtable_str))

###############################################################
 #snap3 Now that points are all matched to a segment and passid
 #calculate pass means 
destinationtable_str = 'snap3_drivepass_means'
job_config.destination = dataset_ref.table(destinationtable_str)
qry_str = ("""
--first get measurement data associated with points/segments/passes
with cte0 as (
select a.vid, c.segid, c.timestamp, c.passid, a.parameter_id, a.value from {0} a
join UK.snapz_distinctdrivepts b
on a.vid = b.vid and a.longitude = ST_X(b.geopt) and a.latitude = ST_Y(b.geopt) and a.timestamp = b.timestamp
join UK.snap2_ptclosestroad_lookup c
on b.pid = c.pid
where (a.qc_flag in (0,2,21,22,23,24)) and a.value is not null
)
--then make drive pass averages, grouping by segment and passid
--aggregate other information needed for subsequent statistics
, cte1 as (
select vid, segid, passid, parameter_id
,min(timestamp) as pass_starttime, max(timestamp) as pass_endtime
--change the following expressions if datatype datetime => timestamp type
,DATETIME_DIFF(max(timestamp),min(timestamp),SECOND) as passduration_s
,DATETIME_TRUNC(max(timestamp), HOUR) as hour1stamp
,DATETIME_ADD(DATETIME_TRUNC(max(timestamp), DAY),INTERVAL DIV(CAST(FORMAT_DATETIME('%H',max(timestamp)) as int64),4)*4 HOUR) as hour4stamp
,avg(value) as drivepassmean from cte0
group by vid, segid, passid, parameter_id
)
--then group by segment and parameter_id and aggregate end time and pass avg values into arrays
select segid, parameter_id
,ARRAY_AGG(FORMAT_DATETIME("%Y-%m-%d %H:%M:%S",pass_endtime) order by pass_endtime) as passtime_arr
,ARRAY_AGG(drivepassmean order by pass_endtime) as passmean_arr
from cte1
group by segid, parameter_id
--order by segid, parameter_id
        """.format(srctable_str))
qry_job = client.query(qry_str,location=location,job_config=job_config)
rows = qry_job.result()
print("Query results loaded into table {0}".format(destinationtable_str))
###############################################################
 #snap4 Calculate N-hour (e.g. 1 or 4 hour) medians over drive passes occurring in the same N-hour period
destinationtable_str = 'snap4_drivepass_1hr_medians'
job_config.destination = dataset_ref.table(destinationtable_str)
qry_str = ("""
--first get measurement data associated with points/segments/passes
with cte0 as (
select a.vid, c.segid, c.timestamp, c.passid, a.parameter_id, a.value from {0} a
join UK.snapz_distinctdrivepts b
on a.vid = b.vid and a.longitude = ST_X(b.geopt) and a.latitude = ST_Y(b.geopt) and a.timestamp = b.timestamp
join UK.snap2_ptclosestroad_lookup c
on b.pid = c.pid
where (a.qc_flag in (0,2,21,22,23,24)) and a.value is not null
)
--then make drive pass averages, grouping by segment and passid
--aggregate other information needed for subsequent statistics
, cte1 as (
select vid, segid, passid, parameter_id
,min(timestamp) as pass_starttime, max(timestamp) as pass_endtime
--change the following expressions if datatype datetime => timestamp type
,DATETIME_DIFF(max(timestamp),min(timestamp),SECOND) as passduration_s
,DATETIME_ADD(DATETIME_TRUNC(max(timestamp), HOUR), INTERVAL 1 HOUR) as hour1stamp --hour ending
,DATETIME_ADD(DATETIME_TRUNC(max(timestamp), DAY),INTERVAL DIV(CAST(FORMAT_DATETIME('%H',max(timestamp)) as int64),4)*4+4 HOUR) as hour4stamp
,avg(value) as drivepassmean from cte0
group by vid, segid, passid, parameter_id
)
--then group by segment and hour or 4 hour timestamp and parameter_id to get driveNhour means or medians
select segid, parameter_id, hour1stamp
,ANY_VALUE(Val_median) as value_median
,ANY_VALUE(dur_median) as passduration_median_s
,COUNT(drivepassmean) as drivepass_count from 
  (select segid, parameter_id, hour1stamp, drivepassmean
  ,percentile_cont(drivepassmean, 0.5) OVER (PARTITION BY segid, hour1stamp, parameter_id) as val_median 
  ,percentile_cont(passduration_s, 0.5) OVER (PARTITION BY segid, hour1stamp, parameter_id) as dur_median 
  from cte1)
group by segid, hour1stamp, parameter_id
--order by segid, parameter_id
        """.format(srctable_str))
qry_job = client.query(qry_str,location=location,job_config=job_config)
rows = qry_job.result()
print("Query results loaded into table {0}".format(destinationtable_str))
