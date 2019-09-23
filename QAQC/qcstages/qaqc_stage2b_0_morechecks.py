from google.cloud import bigquery
from google.oauth2 import service_account
import datetime as dt

credentials = service_account.Credentials.from_service_account_file(
    r"C:\Users\lpadilla\Documents\London\Scripts\pks\street-view-air-quality-london-dc8f329b26cf.json")
project_id = 'street-view-air-quality-london'
bqclient = bigquery.Client(credentials=credentials,project=project_id)

dataset_str = 'UK'
destinationtable_str = dataset_str+'.stage2_test'

#may not need this if only updating table not writing new table
temptable_str = 'temp'
dataset_ref = bqclient.dataset(dataset_str)
table_ref = dataset_ref.table(temptable_str)
job_config = bigquery.QueryJobConfig()
job_config.destination = table_ref
job_config.write_disposition = 'WRITE_TRUNCATE' #overwrite table if exists

#if the instrument out of service status occurs while the car is driving, 
#then someone forgot to turn the service mode off and data is valid
#must be the only code and
#not at NPL or AM
def service_check(coordlist): #[[n0,s0,e0,w0],...[ni,si,ei,wi]]
    where_str = ""
    for coords in coordlist:
        where_str+=" and (latitude > {0} or latitude < {1} or longitude > {2} or longitude < {3})".format(*coords)
    qry_str = ("""update {0} a
    set a.qc_flag = 0
    from UK.qaqc_hexstatus_lookup b
    where a.status = b.status and a.dev_id = b.dev_id and b.status_desc = 'Instrument out of service (or in diagnostic mode or PTF compensation or control loop disabled or comms debugging enabled)' 
    and a.dev_id = 4 and a.qc_flag = 9
    {1}
    """.format(destinationtable_str,where_str))

    qry_job = bqclient.query(qry_str,location='eu',job_config=bigquery.QueryJobConfig())
    rows = qry_job.result()
    print("query results loaded into table {0}".format(destinationtable_str))

#maximum detection limit checks
def limit_max():
    max_thresh_dict = {0:{'val':400000,'p':[140],'unit':'ug m-3'}\
            ,1:{'val':100000,'p':[57,58,59,60,61,62,63],'unit':'ng m-3'}\
            ,2:{'val':20,'p':[45,47],'unit':'ppmv'}
            ,3:{'val':3000,'p':[133],'unit':'ppbv'}\
            ,4:{'val':20000,'p':[21],'unit':'um2 cm-3'}\
            ,5:{'val':1500,'p':[2,3,4,5,6],'unit':'ug m-3'}\
            ,6:{'val':20000,'p':[1],'unit':'n cm-3'}\
            ,7:{'val':3000,'p':[32,33],'unit':'umol/mol'}\
            ,8:{'val':60,'p':[34],'unit':'mmol/mol'}\
            ,9:{'val':500,'p':[127],'unit':'ppbv'}}
    for k,v in max_thresh_dict.items():
        qry_str = ("""update {0} a
        set qc_flag = 5
        where a.parameter_id in ({1}) and value > {2} 
        and not STARTS_WITH(cast(qc_flag as string),'9')
        """.format(destinationtable_str,(',').join([str(p) for p in v['p']]),v['val']))

        qry_job = bqclient.query(qry_str,location='eu',job_config=bigquery.QueryJobConfig())
        rows = qry_job.result()
    print("query results loaded into table {0}".format(destinationtable_str))

#minimum measurement checks - some instruments allow noise around 0
def limit_min():
    min_thresh_dict = {0:{'val':-1,'p':[140],'unit':'ug m-3'}\
            ,1:{'val':-2000,'p':[57,58,59,60,61,62,63],'unit':'ng m-3'}\
            ,2:{'val':-0.05,'p':[45,47],'unit':'ppmv'}
            ,3:{'val':-60,'p':[133],'unit':'ppbv'}\
            ,4:{'val':0,'p':[21],'unit':'um2 cm-3'}\
            ,5:{'val':0,'p':[2,3,4,5,6],'unit':'ug m-3'}\
            ,6:{'val':0,'p':[1],'unit':'n cm-3'}\
            ,7:{'val':0,'p':[32,33],'unit':'umol/mol'}\
            ,8:{'val':60,'p':[34],'unit':'mmol/mol'}\
            ,9:{'val':-10,'p':[127],'unit':'ppbv'}}
    for k,v in min_thresh_dict.items():
        qry_str = ("""update {0} a
        set qc_flag = 5
        where a.parameter_id in ({1}) and value < {2} 
        and not STARTS_WITH(cast(qc_flag as string),'9')
        """.format(destinationtable_str,(',').join([str(p) for p in v['p']]),v['val']))

        qry_job = bqclient.query(qry_str,location='eu',job_config=bigquery.QueryJobConfig())
        rows = qry_job.result()
    print("query results loaded into table {0}".format(destinationtable_str))

#latitude and longitude checks (within greater london or at airmonitors)
def limit_latlon(n,s,e,w):
    qry_str = ("""update {0} a
    set qc_flag = 91
    where latitude > {1} or latitude < {2} 
    or longitude > {3} or longitude < {4}
    """.format(destinationtable_str,n,s,e,w))

    qry_job = bqclient.query(qry_str,location='eu',job_config=bigquery.QueryJobConfig())
    rows = qry_job.result()
    print("query results loaded into table {0}".format(destinationtable_str))


#humidity checks
def limit_rh():
    #dictionary of thresholds
    rh_thresh_dict = {95:{'d':[3,8],'p':[32,33,34,140]}, 80:{'d':[4],'p':[45,47]}}
    #loop over dictionary
    for k,v in rh_thresh_dict.items():
        qry_str = ("""update {0} a
        set qc_flag = 7
        from (select distinct vid, timestamp from {0}
        where parameter_id = 17 and value > {1}
        ) cte
        where a.parameter_id in ({2}) 
        and a.vid=cte.vid and a.timestamp=cte.timestamp
        and not STARTS_WITH(cast(qc_flag as string),'9')
        """.format(destinationtable_str, k,(',').join([str(p) for p in v['p']])))

        qry_job = bqclient.query(qry_str,location='eu',job_config=bigquery.QueryJobConfig())
        rows = qry_job.result()
    print("query results loaded into table {0}".format(destinationtable_str))

#precipitation checks for ae33 bc and aerodyne no2
#{5:[57,58,59,60,61,62,63],7:[133]}
#temperature checks
def limit_temp(p_list,mint=0,maxt=40):
    qry_str = ("""update {0} a
    set qc_flag = 6
    from (select distinct vid, timestamp from {0}
    where parameter_id = 15 and (value <= {1} or value >= {2})
    ) cte
    where a.parameter_id in ({2}) 
    and a.vid=cte.vid and a.timestamp=cte.timestamp
        and not STARTS_WITH(cast(qc_flag as string),'9')
    """.format(destinationtable_str, mint, maxt,(',').join([str(p) for p in p_list])))

    qry_job = bqclient.query(qry_str,location='eu',job_config=bigquery.QueryJobConfig())
    rows = qry_job.result()
    print("query results loaded into table {0}".format(destinationtable_str))

#warm up checks for 2btech ozone, warmup already flagged for serinus 40 nox
#default coordinates for npl parking lot
#must be only flag
def set_warmup(n=51.4257,s=51.4253,e=-0.3452,w=-0.3458):
    #lowscrubber and at npl
    qry_str = ("""update {0} a
    set qc_flag = 4
    where parameter_id = 127 
    and latitude < {1} and latitude > {2}
    and longitude < {3} and longitude > {4}
    and status in 
    (select status from UK.qaqc_hexstatus_lookup where dev_id = 6 and status_desc = 'Low scrubber temperature')
    """.format(destinationtable_str,n,s,e,w))
    qry_job = bqclient.query(qry_str,location='eu',job_config=bigquery.QueryJobConfig())
    rows = qry_job.result()
    print("query results loaded into table {0}".format(destinationtable_str))

    #lowscrubber and not at npl and prior to oct 1 2018 flag as okay
    #this was a period when the low scrubber temp flag was reflecting a temp that didn't belong to the scrubber
    #if flag is other than 9, make no change because a prior issue affected the data
    qry_str = ("""update {0} a
    set qc_flag = 0
    where parameter_id = 127 
    and ((latitude > {1} or latitude < {2})
    or (longitude > {3} or longitude < {4}))
    and timestamp < '2018-10-01'
    and qc_flag = 9
    and status in 
    (select status from UK.qaqc_hexstatus_lookup where dev_id = 6 and status_desc = 'Low scrubber temperature')
    """.format(destinationtable_str,n,s,e,w))
    qry_job = bqclient.query(qry_str,location='eu',job_config=bigquery.QueryJobConfig())
    rows = qry_job.result()
    print("query results loaded into table {0}".format(destinationtable_str))

#convert the pm measurements back to ug/m3 during the period of palas firmware that changed them to mg/m3
#on 2019 Apr 13
#Update: as of most recent firmware update on 2019 Aug 01, units for these 4 params still in mg/m3
#continue changing units to ug/m3 for consistency
def change_pm_units(vid,timestart='2019-04-13', timestop=dt.datetime.today().strftime('%y-%m-%d')):
    qry_str = ("""update {0} a
    set value = value*1000
    where parameter_id in (2,3,5,6) --pm1, pm2.5, pm10, pmt 
    and timestamp > '{1}' and timestamp < '{2}'
    and vid = {3}
    """.format(destinationtable_str,timestart, timestop,vid))
    qry_job = bqclient.query(qry_str,location='eu',job_config=bigquery.QueryJobConfig())
    rows = qry_job.result()
    print("query results loaded into table {0}".format(destinationtable_str))

#zero checks #21
def set_zero_outofrange():

    qry_str = ("""
    select car_id, date,measure  from 
    (select car_id, date, measure, unit, zero
    ,if(unit='ng m-3',abs(safe_cast(zero as float64))/1000,abs(safe_cast(zero as float64))) as diff 
    from UK.instrument_weekly_data)
    where diff > 5.5 and date is not null
    and not (car_id = 27522 and measure = 'CO2' and date = '2019-01-10') --OKed by Stef as not affecting data
    and not (car_id = 27522 and measure = 'PM 2.5' and date = '2019-01-10') --OKed by Stef as not affecting data
    order by date
    """)
    qry_job = bqclient.query(qry_str,location='eu',job_config=bigquery.QueryJobConfig())
    #parameters to invalidate based on out-of-range zero
    p_dict = {'nano':[21],'co2':[32,33,34],'bc':list(range(57,64))+[69,159]+list(range(78,120)),\
            'pm 2.5':[140], 'ozone':[127],'no2':[133],'pmx':list(range(1,7)),'no':[45,47]}
    for row in qry_job.result():
        discoveredinvalid = row.date
        #find the latest passing check to get date range
        qry_str = ("""
        select max(date) as last_passing_date from 
        (select car_id, date, measure
        ,if(unit='ng m-3',abs(safe_cast(zero as float64))/1000,abs(safe_cast(zero as float64))) as diff 
        from UK.instrument_weekly_data)
        where car_id={0} and date<'{1}' and measure='{2}' and diff<=5.5
        """.format(row.car_id,discoveredinvalid,row.measure))
        qry_job = bqclient.query(qry_str,location='eu',job_config=bigquery.QueryJobConfig())
        lastvalid = list(qry_job.result())[0].last_passing_date 
        print(lastvalid)
        #update qc_flag between discovery of invalid zero and last valid zero
        qry_str = ("""update {0} a
        set qc_flag = if(qc_flag = 22 or qc_flag = 23,23,21) --if span also out of range, set 23 (span and zero)
        where vid = {4} and parameter_id in ({1}) 
        and timestamp <= '{2}'
        and timestamp > '{3}'
        and not STARTS_WITH(cast(qc_flag as string),'9')
        """.format(destinationtable_str,','.join([str(int(p)) for p in p_dict[row.measure.lower()]]),discoveredinvalid.strftime('%Y-%m-%d'),lastvalid.strftime('%Y-%m-%d'),row.car_id))
        qry_job = bqclient.query(qry_str,location='eu',job_config=bigquery.QueryJobConfig())
        rows = qry_job.result()
        print("qc flags updated in table {0} for {1} zero out of range between {2} and {3}".format(destinationtable_str,row.measure,lastvalid,discoveredinvalid))
    
#span checks #22
def set_span_outofrange():
    qry_str = ("""
    select car_id, date,measure from 
    (select car_id, date, measure
    ,safe_cast(span_actual as float64) as span_actual, safe_cast(span_cert as float64) as span_cert
    from UK.instrument_weekly_data where span_actual is not null)
    where abs(span_actual-span_cert)/span_cert > 0.1 and date is not null
    order by date
    """)
    qry_job = bqclient.query(qry_str,location='eu',job_config=bigquery.QueryJobConfig())
    #parameters to invalidate based on out-of-range span
    p_dict = {'nano':[21],'co2':[32,33,34],'bc':list(range(57,64))+[69,159]+list(range(78,120)),\
            'pm 2.5':[140], 'ozone':[127],'no2':[133],'pmx':list(range(1,7)),'no':[45,47]}
    for row in qry_job.result():
        discoveredinvalid = row.date
        #find the latest passing check to get date range
        qry_str = ("""
        select max(date) as last_passing_date from 
        (select car_id, date, measure
        ,safe_cast(span_actual as float64) as span_actual, safe_cast(span_cert as float64) as span_cert
        from UK.instrument_weekly_data where span_actual is not null)
        where car_id={0} and date<'{1}' and measure='{2}' and abs(span_actual-span_cert)/span_cert <= 0.1
        """.format(row.car_id,discoveredinvalid,row.measure))
        qry_job = bqclient.query(qry_str,location='eu',job_config=bigquery.QueryJobConfig())
        lastvalid = list(qry_job.result())[0].last_passing_date 
        #update qc_flag between discovery of invalid span and last valid span
        qry_str = ("""update {0} a
        set qc_flag = if(qc_flag = 21 or qc_flag = 23,23,22) --if zero also out of range, set 23 (span and zero)
        where vid = {4} and parameter_id in ({1}) 
        and timestamp <= cast('{2}' as datetime)
        and timestamp > cast('{3}' as datetime)
        and not STARTS_WITH(cast(qc_flag as string),'9')
        """.format(destinationtable_str,','.join([str(int(p)) for p in p_dict[row.measure.lower()]]),discoveredinvalid.strftime('%Y-%m-%d'),lastvalid.strftime('%Y-%m-%d'),row.car_id))
        qry_job = bqclient.query(qry_str,location='eu',job_config=bigquery.QueryJobConfig())
        rows = qry_job.result()
        print("qc flags updated in table {0} for {1} span out of range between {2} and {3}".format(destinationtable_str,row.measure,lastvalid,discoveredinvalid))

#leak checks #24
#review uk.instrument_weekly_data manually, discuss with team for details on location and severity
#flag affected data here
def flag_suspect_leaks():
    #periods of ambient air mixed with zero air during baselining
    qry_str = ("""update {0} a
    set qc_flag = 24
    where vid = 27522 and parameter_id = 133 --no2
    and timestamp < '2019-06-07'
        and not STARTS_WITH(cast(qc_flag as string),'9')
    """.format(destinationtable_str))
    qry_job = bqclient.query(qry_str,location='eu',job_config=bigquery.QueryJobConfig())
    rows = qry_job.result()
    print("query results loaded into table {0}".format(destinationtable_str))

def flag_known_leaks():
    #period of major leak 
    qry_str = ("""update {0} a
    set qc_flag = 9
    where vid = 27522 and parameter_id = 133 --no2
    and timestamp >= '2019-04-25' 
    and timestamp < '2019-06-07'
    """.format(destinationtable_str))
    qry_job = bqclient.query(qry_str,location='eu',job_config=bigquery.QueryJobConfig())
    rows = qry_job.result()
    print("query results loaded into table {0}".format(destinationtable_str))

    #periods of ambient air mixed with zero air during baselining
    #when baseline was outside of +/- 3 ppb
    qry_str = ("""UPDATE {0} a
        SET
          qc_flag = 93
        FROM (
            --unnest all timestamps (t)
            SELECT
            redact_id,
            redactstamp
            FROM (
              --generate array of all timestamps in a redaction period (u) 
              SELECT redact_id
              ,generate_timestamp_array(MIN(timestamp),MAX(timestamp),INTERVAL 1 SECOND) AS timearray
              --,min(timestamp), max(timestamp), timestamp_diff(max(timestamp),min(timestamp),SECOND)
              FROM (
                --assign each redaction period (>+3 to last <-3) a unique id (v)
                SELECT timestamp, idx, last2_flag, baseline_id, no2_avg
                ,SUM(sign_chg_flag) OVER(ORDER BY timestamp ASC) AS redact_id
                FROM (
                  --identify sign changes between baseline periods (w)
                  SELECT timestamp, no2_avg, idx, baseline_id
                  ,IF(baseline_id <> LAG(baseline_id) OVER(ORDER BY timestamp) AND idx = 1 AND LAG(idx) OVER(ORDER BY timestamp) <> 1,1,0) AS sign_chg_flag
                  ,IF(idx = 2 AND LAG(idx) OVER(ORDER BY timestamp desc)<>2,1,0) as last2_flag 
                  FROM (
                   --make index assignment based on average value (x)
                   SELECT timestamp, baseline_id, AVG(value) OVER(PARTITION BY baseline_id) as no2_avg
                   ,IF(AVG(value) OVER(PARTITION BY baseline_id)>3,1,IF(AVG(value) OVER(PARTITION BY baseline_id)<-3,2,3)) as idx
                   FROM (
                    --give each bad baseline period a unique id (y)
                    SELECT timestamp
                    ,SUM(baseline_chg_flag) OVER(ORDER BY timestamp) AS baseline_id
                    ,value
                    FROM (
                      --find just the baselining no2 data for affected vehicle (z)
                      SELECT CAST(timestamp AS timestamp) AS timestamp, value
                      ,IF(datetime_diff(timestamp,LAG(timestamp) OVER(ORDER BY timestamp),SECOND)>1,1,0) AS baseline_chg_flag
                      FROM UK.stage2_test
                      WHERE status = '32104' AND vid = 27522 AND parameter_id = 133
                    ) z
                  ) y
                 ) x
                ) w
              ) v
            WHERE idx =1 or last2_flag = 1
            GROUP BY redact_id
            ) u
            CROSS JOIN
            UNNEST(u.timearray) AS redactstamp
        ) t
        WHERE cast(t.redactstamp as datetime) = a.timestamp and a.vid = 27522 and a.parameter_id = 133
    and timestamp < '2019-04-25' 
    """.format(destinationtable_str))
    qry_job = bqclient.query(qry_str,location='eu',job_config=bigquery.QueryJobConfig())
    rows = qry_job.result()
    print("query results loaded into table {0}".format(destinationtable_str))

#other daily instrumentation checks
#any action needed when the status is broken?
"""
select * from UK.instrument_daily_data 
where in_position is not null and in_position not in ('repaired', 'ok')
or (running is not null and running not in ('repaired', 'ok'))
or (sample_lines is not null and sample_lines not in ('repaired', 'ok'))
or (comms is not null and comms not in ('repaired', 'ok'))
or (display is not null and display not in ('repaired', 'ok'))
"""

#flag MOT tests
def flag_MOTs(vid,starttime,endtime):
    qry_str = ("""update {0} a
    set qc_flag = 92
    where vid = {1}
    and timestamp >= '{2}' 
    and timestamp < '{3}'
    """.format(destinationtable_str,vid,starttime,endtime))
    qry_job = bqclient.query(qry_str,location='eu',job_config=bigquery.QueryJobConfig())
    rows = qry_job.result()
    print("query results loaded into table {0}".format(destinationtable_str))

#spatial uncertainty
def flag_spatial_resolution(lim):
    qry_str = ("""update {0} a
    set qc_flag = 25
    where spatial_uncertainty_m > {1}
    and not STARTS_WITH(cast(qc_flag as string),'9')
    """.format(destinationtable_str, lim))
    qry_job = bqclient.query(qry_str,location='eu',job_config=bigquery.QueryJobConfig())
    rows = qry_job.result()
    print("query results loaded into table {0}".format(destinationtable_str))

#exceptional event checks
#guts tickets
#review manually
"""
print('flagging concentrations above maximum detection limits')
limit_max()
print('flagging concentrations below low limits')
limit_min()
print('flagging relative humidity outside operating conditions')
limit_rh()
print('limiting lat/lon to london bounds including am')
limit_latlon(52.06, 51.24, 0.31, -2.2) #n,s,e,w
"""
print('recovering accidental use of service check')
service_check([[52.01,52.00,-2.12,-2.14],[51.4257,51.4253,-0.3452,-0.3458]]) #am and npl
print('flagging temperatures outside -10-50 C for pdr and naneos')
limit_temp([140,141,142,143,21,22,23,25,31],-10,50)
print('flagging temperatures outside 10-40 C for magee ae33')
limit_temp([57,58,59,60,61,62,63,69,83,90,97,118,159],10,40)
print('flagging temperatures outside 0-40 C for eco serinus')
limit_temp([45,47,49],0,40)
print('flagging temperatures outside 0-45 C for aerodyne caps')
limit_temp([133],0,45)
print('flagging temperatures outside 0-35 C for palas fidas')
limit_temp([1,2,3,4,5,6,7,15,16,17,160,161,162],0,35)
print('flagging temperatures outside -25-50 C for licor')
limit_temp([32,33,34,41],-25,50)
print('flagging temperatures outside 10-50 C for 2b tech ozone')
limit_temp([127],10,50)
print('identifying 2BTech warmup and unflagging period with scrubber temp issue')
set_warmup()
print('correcting pm units between palas firmware updates')
change_pm_units(27522,timestart='2019-04-12 14:53:34', timestop=(dt.datetime.today()+dt.timedelta(1)).strftime('%Y-%m-%d'))
change_pm_units(27533,timestart='2019-04-12 14:15:52', timestop=(dt.datetime.today()+dt.timedelta(1)).strftime('%Y-%m-%d'))
print('flagging zero out of range periods')
set_zero_outofrange()

#print('flagging span out of range periods')
set_span_outofrange()

print('flagging suspect leak periods')
flag_suspect_leaks()
print('flagging known leak periods')
flag_known_leaks()
print('flagging MOT test periods')
flag_MOTs(27522,'2019-01-21','2019-02-05')
flag_MOTs(27533,'2019-01-23','2019-02-08')
print('flagging suspect spatial resolution')
flag_spatial_resolution(90)


