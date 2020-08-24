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

#place to consolidate long sql queries

#rolling 1st percentile of 1 hour windows
def sql_str_0background_aqmesh(species):
    if (species == 'pm25') or (species == 'pm25_pdr'):
        field = 'pm2_5_ugm3'
        table = 'PM25_scaled_hightimeres_ugm3_20180901_20190930'
    elif species == 'no2':
        field = 'no2_ppb'
        table = 'NO2_scaled_hightimeres_ppb_20180901_20190930'
    return """
--calc regional background as 1% of 1 hr moving window
with cte0 as (
select date_UTC as timestamp
--array of concentrations in each centered 1 hr window
, array_agg({0}) OVER (order by UNIX_MICROS(date_UTC) RANGE BETWEEN 600000000 PRECEDING AND 600000000 FOLLOWING) as value_arr
from AQMesh.{1}
where {0} > 0
 )
select timestamp, background_value
from (
select timestamp
--1st percentile value in window (out of n pods x m 1 min or 15 min values)
, percentile_cont(k,0.01) over (partition by timestamp) as background_value
from cte0
,unnest(value_arr) k
)
group by timestamp, background_value
order by timestamp
""".format(field,table)

def sql_str_0background_laqn(species):
    if (species == 'pm25') or (species == 'pm25_pdr'):
        field = 'pm25_ugm3'
        table = 'pm25_ugm3_20180801_20191015'
    elif species == 'no2':
        field = 'no2_ugm3*0.52'
        table = 'no2_ugm3_20180801_20191015'
    return """
--calc regional background as 1% of 1 hr window (hourly data so moving window not applicable)
select timestamp, background_value
from (
select timestamp
--1st percentile value in window (out of n pods x 1 1 hr value)
, percentile_cont({0},0.01) over (partition by timestamp) as background_value
from LAQN.{1}
where {0} >= 0
)
group by timestamp, background_value
order by timestamp
""".format(field,table)
#select segments and calc enhancements and expected values
def sql_str_1selectpasses(species,srctb):
    if species == 'pm25':
        parameter_id = 3
    elif species == 'no2':
        parameter_id = 133
    elif species == 'pm25_pdr':
        parameter_id = 140
    return """
--select segments with >= 100 passes
with cte0 as (
select a.segid, idx, PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',endtime) as endtime, conc from 
(select segid, count(value_median) as N from UK.snap4_drivepass_1hr_medians
where parameter_id = {0}
group by segid) a
join 
(select u.segid, endtime, u.idx, conc 
  from 
  (select segid, endtime, idx from UK.snap3_drivepass_means, unnest(passtime_arr) endtime with offset as idx
   where parameter_id = {0}) u
  join 
  (select segid, conc, idx from UK.snap3_drivepass_means, unnest(passmean_arr) conc with offset as idx
   where parameter_id = {0}) v
  on
  u.segid = v.segid and u.idx = v.idx) b
on a.segid = b.segid
where N >= 100
)
--match drive times with background times
, cte1 as (
select segid, idx, endtime, conc, timestamp, background_value 
, TIMESTAMP_DIFF(endtime,timestamp, SECOND) as timedelta
, min(TIMESTAMP_DIFF(endtime,timestamp, SECOND)) over (partition by segid, idx) as smallestdelta
, TIMESTAMP_TRUNC(TIMESTAMP_ADD(endtime, INTERVAL 30 MINUTE),HOUR) as hour1stamp --nearest hour
from cte0
join UK.{1} b
--round to nearest hour
on TIMESTAMP_TRUNC(TIMESTAMP_ADD(endtime, INTERVAL 30 MINUTE),HOUR) = 
TIMESTAMP_TRUNC(TIMESTAMP_ADD(b.timestamp, INTERVAL 30 MINUTE),HOUR)
)
, cte2 as (
--group concentrations by 1 hr periods
select segid, hour1stamp, ANY_VALUE(conc_1hr) as conc_1hr, ANY_VALUE(background_1hr) as background_1hr
, ANY_VALUE(enhancement_1hr) as enhancement_1hr
from (
    --calculate enhancements
    select segid
    , hour1stamp
    --, idx, endtime, conc, timestamp, background_value, conc-background_value as enhancement
    --calc median over 1 hr periods
    , percentile_cont(conc,0.5) over (partition by segid, hour1stamp) as conc_1hr
    , percentile_cont(background_value,0.5) over (partition by segid, hour1stamp) as background_1hr
    , percentile_cont(conc-background_value,0.5) over (partition by segid, hour1stamp) as enhancement_1hr
    from cte1
    where timedelta = smallestdelta
)
group by segid, hour1stamp
)
--calc expected values using all N passes (~truth) and assign idx for subsampling
select segid, hour1stamp, conc_1hr, background_1hr, enhancement_1hr
,row_number() over (partition by segid) as idx
,percentile_cont(conc_1hr, 0.5) over (partition by segid) as conc_expected
,percentile_cont(background_1hr, 0.5) over (partition by segid) as background_expected
,percentile_cont(enhancement_1hr, 0.5) over (partition by segid) as enhancement_expected
from cte2
""".format(parameter_id,srctb)

#create subsample realizations
#option a (without replacement)
def sql_str_2subsamples_a(srctb):
    return """
with cte0 as (
select segid, N
,trial_id
--arrays of top n drive indices ordered by random number without replacement
,array_agg(idx) over (partition by segid,trial_id order by rid ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING) as sample_arr5
,array_agg(idx) over (partition by segid,trial_id order by rid ROWS BETWEEN CURRENT ROW AND 10 FOLLOWING) as sample_arr10
,array_agg(idx) over (partition by segid,trial_id order by rid ROWS BETWEEN CURRENT ROW AND 15 FOLLOWING) as sample_arr15
,array_agg(idx) over (partition by segid,trial_id order by rid ROWS BETWEEN CURRENT ROW AND 20 FOLLOWING) as sample_arr20
,array_agg(idx) over (partition by segid,trial_id order by rid ROWS BETWEEN CURRENT ROW AND 25 FOLLOWING) as sample_arr25
,array_agg(idx) over (partition by segid,trial_id order by rid ROWS BETWEEN CURRENT ROW AND 30 FOLLOWING) as sample_arr30
,array_agg(idx) over (partition by segid,trial_id order by rid ROWS BETWEEN CURRENT ROW AND 35 FOLLOWING) as sample_arr35
,rid
,min(rid) over (partition by segid,trial_id) as minrid
from (
  --make M copies of the passes for M trials, each trial will have a different random selection of n passes
  select segid, max(idx) over (partition by segid) as N --total number of passes
  ,idx
  ,rand() as rid --random number assigned to each record
  ,trial_id
  from UK.{0}
  ,unnest(generate_array(1,500)) trial_id --number of trials
  --order by segid, rid
  )  
--limit to segments with at least 100 drives
where N >= 100
)
--union results for differnt n
select segid, trial_id, N
, 5 as n_sub, sample_arr5 as sample_arr
from cte0
where rid = minrid --take first result
union all
select segid, trial_id, N
, 10 as n_sub, sample_arr10 as sample_arr
from cte0
where rid = minrid
union all
select segid, trial_id, N
, 15 as n_sub, sample_arr15 as sample_arr
from cte0
where rid = minrid
union all
select segid, trial_id, N
, 20 as n_sub, sample_arr20 as sample_arr
from cte0
where rid = minrid
union all
select segid, trial_id, N
, 25 as n_sub, sample_arr25 as sample_arr
from cte0
where rid = minrid
union all
select segid, trial_id, N
, 30 as n_sub, sample_arr30 as sample_arr
from cte0
where rid = minrid
union all
select segid, trial_id, N
, 35 as n_sub, sample_arr35 as sample_arr
from cte0
where rid = minrid

""".format(srctb)
#option b (with replacement)
def sql_str_2subsamples_b(srctb):
    return """
with cte0 as (
select segid, N
from (
  select segid, max(idx) as N
  from UK.{0}
  group by segid
  )
where N >= 100
limit 3
)
, cte1 as (
select segid, N, trial_id
from cte0, unnest(generate_array(1,10)) trial_id --number of trials
)
select segid, N, trial_id, GREATEST(cast(RAND()*N-0.5 as int64)+1,1) as sample_idx --don't let it be 0, would only happen if rand exactly 0.0
, k
from cte1, unnest(generate_array(1,100)) k
""".format(srctb)

#Summarize %bias statistics by n subsamples
def sql_str_3summarize(srctb1,srctb2):
    return """
--join concentration data to the expanded subsamples
with cte0 as (
select segid, n_sub, trial_id, conc_expected, background_expected, enhancement_expected
,ANY_VALUE(conc_sub_expected) as conc_sub_expected, ANY_VALUE(enhancement_sub_expected) as enhancement_sub_expected
from (
    select a.segid, n_sub, trial_id
    ,conc_expected
    ,background_expected
    ,enhancement_expected
    --calc expected values over subsample trials
    ,percentile_cont(conc_1hr,0.5) over (partition by a.segid, n_sub, trial_id) as conc_sub_expected
    ,percentile_cont(enhancement_1hr,0.5) over (partition by a.segid, n_sub, trial_id) as enhancement_sub_expected
    --concentration data
    from UK.{0} a
    join 
    --expanded subsample trials
    (select segid, trial_id, n_sub, sample_idx 
    from UK.{1}, unnest(sample_arr) sample_idx) b
    on a.segid = b.segid and a.idx = b.sample_idx
    )
group by segid, n_sub, trial_id, conc_expected, background_expected, enhancement_expected
)
--calc summary statistics on bias %
select n_sub
, ANY_VALUE(p05t) as p05t, ANY_VALUE(p25t) as p25t, ANY_VALUE(p50t) as p50t, ANY_VALUE(p75t) as p75t, ANY_VALUE(p95t) as p95t
, ANY_VALUE(p05te) as p05te, ANY_VALUE(p25te) as p25te, ANY_VALUE(p50te) as p50te, ANY_VALUE(p75te) as p75te, ANY_VALUE(p95te) as p95te
, ANY_VALUE(p05e) as p05e, ANY_VALUE(p25e) as p25e, ANY_VALUE(p50e) as p50e, ANY_VALUE(p75e) as p75e, ANY_VALUE(p95e) as p95e
from (
select n_sub
,percentile_cont((conc_sub_expected-conc_expected)/conc_expected*100,0.05) over (partition by n_sub) as p05t
,percentile_cont((enhancement_sub_expected+background_expected-conc_expected)/conc_expected*100,0.05) over (partition by n_sub) as p05te
,percentile_cont((enhancement_sub_expected-enhancement_expected)/enhancement_expected*100,0.05) over (partition by n_sub) as p05e

,percentile_cont((conc_sub_expected-conc_expected)/conc_expected*100,0.25) over (partition by n_sub) as p25t
,percentile_cont((enhancement_sub_expected+background_expected-conc_expected)/conc_expected*100,0.25) over (partition by n_sub) as p25te
,percentile_cont((enhancement_sub_expected-enhancement_expected)/enhancement_expected*100,0.25) over (partition by n_sub) as p25e

,percentile_cont((conc_sub_expected-conc_expected)/conc_expected*100,0.5) over (partition by n_sub) as p50t
,percentile_cont((enhancement_sub_expected+background_expected-conc_expected)/conc_expected*100,0.5) over (partition by n_sub) as p50te
,percentile_cont((enhancement_sub_expected-enhancement_expected)/enhancement_expected*100,0.5) over (partition by n_sub) as p50e

,percentile_cont((conc_sub_expected-conc_expected)/conc_expected*100,0.75) over (partition by n_sub) as p75t
,percentile_cont((enhancement_sub_expected+background_expected-conc_expected)/conc_expected*100,0.75) over (partition by n_sub) as p75te
,percentile_cont((enhancement_sub_expected-enhancement_expected)/enhancement_expected*100,0.75) over (partition by n_sub) as p75e

,percentile_cont((conc_sub_expected-conc_expected)/conc_expected*100,0.95) over (partition by n_sub) as p95t
,percentile_cont((enhancement_sub_expected+background_expected-conc_expected)/conc_expected*100,0.95) over (partition by n_sub) as p95te
,percentile_cont((enhancement_sub_expected-enhancement_expected)/enhancement_expected*100,0.95) over (partition by n_sub) as p95e
from cte0
)
group by n_sub
""".format(srctb1,srctb2)
