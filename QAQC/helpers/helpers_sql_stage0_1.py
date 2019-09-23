#big sql queries for stage0_1_spatial uncertainty calculation

#stage tmp table of ids for given vehicle, device, id period
def make_tmp(srctb,d,where,vid,bqclient,location,job_config):
    qry_str = ("""
    --any_value is non-deterministic so we need to save the staging table as tmp 
    --to fix the intid for subsequent queries

    select ANY_VALUE(cast(id as int64)) as intid
    , vid
    , timestamp
    , cast(SPLIT(node_id, ':')[OFFSET(1)] as int64) as dev_id
    from {0} where cast(SPLIT(node_id, ':')[OFFSET(1)] as int64) in ({1}, 9)
    and {2}
    and vid = {3}
    group by vid, timestamp, dev_id 
    """).format(srctb,d,where,vid)
    qry_job = bqclient.query(qry_str,location=location,job_config=job_config)
    rows = qry_job.result()
    print("Query results for device {1}, vehicle {2} loaded into table {0}".format(job_config.destination,d,vid))

#calc clock uncertainty from gps timestamp to nearest timestamp of given device
def calc_clock_diff(dev,where,vid,bqclient, location, job_config):

    qry_str = ("""
    with cte0 as (
    --stage the data for given device, unique id period, vid
    select intid, vid, timestamp, dev_id from UK.tmp
    )
    ,cte1 as (
    --find the preceding and next ids and non-gps devices
    select intid, dev_id 
    , lag(dev_id, 1,null) over (order by intid) as preceding_dev
    , lag(intid, 1,null) over (order by intid) as preceding_id
    , lead(dev_id, 1,null) over (order by intid) as next_dev
    , lead(intid, 1,null) over (order by intid) as next_id
    from cte0
    )
    , cte2 as (
    --group the rows into packets that share the same nearest logger record backward in time
    --and assign them the id of that logger record 
    select dev_id, intid, sum(sub.pre_id) over(partition by sub.packet order by sub.intid) as pre_log_id
    from (select dev_id, intid, if(preceding_dev = {0}, 1,0)*preceding_id as pre_id, sum(if(preceding_dev = {0}, 1,0)) over(order by intid) as packet
    from cte1 where dev_id = 9) sub
    )
    , cte3 as (
    --group the rows into packets that share the same nearest logger record in future time
    --and assign them the id of that logger record
    select intid, sum(sub.post_id) over(partition by sub.packet order by sub.intid desc) as post_log_id
    from (select intid, if(next_dev = {0}, 1,0)*next_id as post_id, sum(if(next_dev = {0}, 1,0)) over(order by intid desc) as packet
    from cte1 where dev_id = 9) sub
    )
    --join the results for nearest pre and post log ids to retrieve timestamps
    --include vehicle and dev ids
    --will need them as join fields to calc uncertainty in meters
    select cte0.vid, {0} as dev_id, cte0.timestamp as gpstime, pre_log_time, post_log_time
    ,IFNULL(LEAST(ABS(DATETIME_DIFF(cte0.timestamp,pre_log_time,SECOND)),ABS(DATETIME_DIFF(cte0.timestamp,post_log_time,SECOND))),IFNULL(ABS(DATETIME_DIFF(cte0.timestamp,pre_log_time,SECOND)), ABS(DATETIME_DIFF(cte0.timestamp,post_log_time,SECOND)) )) as clock_diff_s 
    from cte0
    left join (select cte2.intid, timestamp as pre_log_time from cte0 join cte2 on cte0.intid=cte2.pre_log_id) a
    on cte0.intid = a.intid
    left join (select cte3.intid, timestamp as post_log_time from cte0 join cte3 on cte0.intid=cte3.post_log_id) b
    on cte0.intid = b.intid
    where cte0.dev_id = 9
    order by gpstime

    """.format(dev,where,vid))
    qry_job = bqclient.query(qry_str,location=location,job_config=job_config)
    rows = qry_job.result()
    print("Query results for device {1}, vehicle {2} loaded into table {0}".format(job_config.destination,dev,vid))

