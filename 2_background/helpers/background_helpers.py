#query for background concentration as 1st percentile of rolling 1 hour windows
def background_testdays_str(species):
    if species == 'no2':
        mobfield = 'no2_ppb'
        aqmeshfield = 'no2_ppb'
        aqmeshtable = 'NO2_scaled_hightimeres_ppb_20180901_20190930'
        laqnfield = 'no2_ugm3*0.52'
        laqntable = 'no2_ugm3_20180801_20191015'
    elif species == 'pm_pdr':
        mobfield = 'pm_pdr_ugm3'
        aqmeshfield = 'pm2_5_ugm3'
        aqmeshtable = 'PM25_scaled_hightimeres_ugm3_20180901_20190930'
        laqnfield = 'pm25_ugm3'
        laqntable = 'pm25_ugm3_20180801_20191015'

    return """with cte0mob as 
(
select cast(timestamp as timestamp) timestamp, {0} as value, format_datetime('%m_%d',timestamp) as vidperiod, cast(vid as string) as type
from UK_partners.stage3_pivoted_2019Sep23 
where ((timestamp >= "2019-01-08 23:30:00" and timestamp < "2019-01-10 00:30:00" and vid = 27533)
    or (timestamp >= "2019-01-13 23:30:00" and timestamp < "2019-01-15 00:30:00" and vid = 27533)
    or (timestamp >= "2019-02-24 23:30:00" and timestamp < "2019-02-26 00:30:00" and vid = 27533)
    or (timestamp >= "2019-02-25 23:30:00" and timestamp < "2019-02-27 00:30:00" and vid = 27533)
    or (timestamp >= "2019-04-04 23:30:00" and timestamp < "2019-04-06 00:30:00" and vid = 27522)
    or (timestamp >= "2019-04-24 23:30:00" and timestamp < "2019-04-26 00:30:00" and vid = 27533)
    or (timestamp >= "2019-04-28 23:30:00" and timestamp < "2019-04-30 00:30:00" and vid = 27533)
    or (timestamp >= "2019-05-09 23:30:00" and timestamp < "2019-05-11 00:30:00" and vid = 27533)
    or (timestamp >= "2019-05-30 23:30:00" and timestamp < "2019-06-01 00:30:00" and vid = 27533)
    or (timestamp >= "2019-06-16 23:30:00" and timestamp < "2019-06-18 00:30:00" and vid = 27522)
    or (timestamp >= "2019-06-19 23:30:00" and timestamp < "2019-06-21 00:30:00" and vid = 27522)
      )
    and 
    ({0} is not null
    and {1}_qc in (0,2,21,22,23,24)
    and {1}_mmode = 1
    )
)
,cte0stat as (
select date_UTC as timestamp, {2} as value, format_timestamp('%m_%d',date_UTC) as vidperiod, 'AQMesh' as type
from AQMesh.{3}
where {2} >= 0
and ((date_UTC >= "2019-01-08 23:30:00" and date_UTC < "2019-01-10 00:30:00" )
    or (date_UTC >= "2019-01-13 23:30:00" and date_UTC < "2019-01-15 00:30:00" )
    or (date_UTC >= "2019-02-24 23:30:00" and date_UTC < "2019-02-26 00:30:00" )
    or (date_UTC >= "2019-02-25 23:30:00" and date_UTC < "2019-02-27 00:30:00" )
    or (date_UTC >= "2019-04-04 23:30:00" and date_UTC < "2019-04-06 00:30:00" )
    or (date_UTC >= "2019-04-24 23:30:00" and date_UTC < "2019-04-26 00:30:00" )
    or (date_UTC >= "2019-04-28 23:30:00" and date_UTC < "2019-04-30 00:30:00" )
    or (date_UTC >= "2019-05-09 23:30:00" and date_UTC < "2019-05-11 00:30:00" )
    or (date_UTC >= "2019-05-30 23:30:00" and date_UTC < "2019-06-01 00:30:00" )
    or (date_UTC >= "2019-06-16 23:30:00" and date_UTC < "2019-06-18 00:30:00" )
    or (date_UTC >= "2019-06-19 23:30:00" and date_UTC < "2019-06-21 00:30:00" )
      )
union all
select timestamp, {4} as value, format_timestamp('%m_%d',timestamp) as vidperiod, 'LAQN' as type
from LAQN.{5}
where {4} >= 0
and ((timestamp >= "2019-01-08 23:30:00" and timestamp < "2019-01-10 00:30:00" )
    or (timestamp >= "2019-01-13 23:30:00" and timestamp < "2019-01-15 00:30:00" )
    or (timestamp >= "2019-02-24 23:30:00" and timestamp < "2019-02-26 00:30:00" )
    or (timestamp >= "2019-02-25 23:30:00" and timestamp < "2019-02-27 00:30:00" )
    or (timestamp >= "2019-04-04 23:30:00" and timestamp < "2019-04-06 00:30:00" )
    or (timestamp >= "2019-04-24 23:30:00" and timestamp < "2019-04-26 00:30:00" )
    or (timestamp >= "2019-04-28 23:30:00" and timestamp < "2019-04-30 00:30:00" )
    or (timestamp >= "2019-05-09 23:30:00" and timestamp < "2019-05-11 00:30:00" )
    or (timestamp >= "2019-05-30 23:30:00" and timestamp < "2019-06-01 00:30:00" )
    or (timestamp >= "2019-06-16 23:30:00" and timestamp < "2019-06-18 00:30:00" )
    or (timestamp >= "2019-06-19 23:30:00" and timestamp < "2019-06-21 00:30:00" )
      )
)
,cte1 as
(
select vidperiod, type, timestamp, value
, array_agg(value) OVER (partition by type, vidperiod order by UNIX_MICROS(cast(timestamp as timestamp)) RANGE BETWEEN 600000000 PRECEDING AND 600000000 FOLLOWING) as value_arr
from cte0mob
union all 
select vidperiod, type, timestamp, value
, array_agg(value) OVER (partition by type, vidperiod order by UNIX_MICROS(cast(timestamp as timestamp)) RANGE BETWEEN 1800000000 PRECEDING AND 1800000000 FOLLOWING) as value_arr
from cte0stat
 )
select vidperiod, type, timestamp, AVG(value) as value, background_value
from (
select vidperiod, type, timestamp, value
--,k
, percentile_cont(k,0.01) over (partition by vidperiod, type, timestamp) as background_value
from cte1
,unnest(value_arr) k
where ((timestamp >= "2019-01-09 00:00:00" and timestamp < "2019-01-10 00:00:00")
    or (timestamp >= "2019-01-14 00:00:00" and timestamp < "2019-01-15 00:00:00")
    or (timestamp >= "2019-02-25 00:00:00" and timestamp < "2019-02-26 00:00:00")
    or (timestamp >= "2019-02-26 00:00:00" and timestamp < "2019-02-27 00:00:00")
    or (timestamp >= "2019-04-05 00:00:00" and timestamp < "2019-04-06 00:00:00")
    or (timestamp >= "2019-04-25 00:00:00" and timestamp < "2019-04-26 00:00:00")
    or (timestamp >= "2019-04-29 00:00:00" and timestamp < "2019-04-30 00:00:00")
    or (timestamp >= "2019-05-10 00:00:00" and timestamp < "2019-05-11 00:00:00")
    or (timestamp >= "2019-05-31 00:00:00" and timestamp < "2019-06-01 00:00:00")
    or (timestamp >= "2019-06-17 00:00:00" and timestamp < "2019-06-18 00:00:00")
    or (timestamp >= "2019-06-20 00:00:00" and timestamp < "2019-06-21 00:00:00"))
)
group by vidperiod, type, timestamp, background_value
order by vidperiod, type, timestamp""".format(mobfield,species,aqmeshfield,aqmeshtable,laqnfield,laqntable)
