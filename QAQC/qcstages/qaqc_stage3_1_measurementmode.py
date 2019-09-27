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

credentials = service_account.Credentials.from_service_account_file(
    r"..\..\..\pks\street-view-air-quality-london-dc8f329b26cf.json")
project_id = 'street-view-air-quality-london'
bqclient = bigquery.Client(credentials=credentials,project=project_id)

dataset_str = 'UK'
destinationtable_str = dataset_str+'.stage3_test'

#measurement mode 1
#mobile monitoring (based on lat/lon coordinates outside of NPL) 
#representative of on-road conditions in London
#default coordinates for NPL parking lot
def set_mobile(n=51.4257,s=51.4253,e=-0.3452,w=-0.3458):
    #not on road
    qry_str = ("""update {0} a
    set mmode = 1
    where 
    latitude >= {1} or latitude <= {2}
    or longitude >= {3} or longitude <= {4}
    """.format(destinationtable_str,n,s,e,w))

    qry_job = bqclient.query(qry_str,location='EU',job_config=bigquery.QueryJobConfig())
    rows = qry_job.result()
    print("Query results loaded into table {0}".format(destinationtable_str))

#measurement mode 2
#stationary at NPL or AirMonitors (e.g. overnight, off-day) based on GPS coordinate
#bounding box around NPL
def set_nonmobile(n=51.4257,s=51.4253,e=-0.3452,w=-0.3458):
    #at NPL
    qry_str = ("""update {0} a
    set mmode = 2
    where 
    latitude < {1} and latitude > {2}
    and longitude < {3} and longitude > {4}
    """.format(destinationtable_str,n,s,e,w))

    qry_job = bqclient.query(qry_str,location='EU',job_config=bigquery.QueryJobConfig())
    rows = qry_job.result()
    print("Query results loaded into table {0}".format(destinationtable_str))

#measurement mode 3
#calibration period at NPL 
#not instrument-specific
#inferred based on times of CO2 > 800 ppm or no2 > 450 ppb (indicating span check in progress)
#subtract 1 hour for calibration start time until car leaves NPL or 7PM same day when NPL closes
def set_calibrationmode(vid,tstart,tstop,n=51.4257,s=51.4253,e=-0.3452,w=-0.3458):
    qry_str = ("""
        --update mmode where
        UPDATE {0} z
        SET mmode = 3
        FROM (
            --unnest all timestamps
            select vid, calstamp
            from (
                --generate timestamp array between min timestamp minus 1hr up to leave NPL or 7PM (NPL closed) whichever occurs first
                select vid, span_id
                , generate_timestamp_array(timestamp_sub(min(timestamp),interval 1 hour),IF(max(timestamp)=min(timestamp),TIMESTAMP(FORMAT_TIMESTAMP('%Y-%m-%d 19:00:00',min(timestamp))),LEAST(max(timestamp),TIMESTAMP(FORMAT_TIMESTAMP('%Y-%m-%d 19:00:00',min(timestamp))))), INTERVAL 1 SECOND) as timearray
                --, timestamp_sub(min(timestamp),interval 1 hour),IF(max(timestamp)=min(timestamp),TIMESTAMP(FORMAT_TIMESTAMP('%Y-%m-%d 19:00:00',min(timestamp))),LEAST(max(timestamp),TIMESTAMP(FORMAT_TIMESTAMP('%Y-%m-%d 19:00:00',min(timestamp)))))
                from
                    --sum over spanstarts to give each span period a unique id
                    (select vid, cast(timestamp as timestamp) as timestamp, sum(spanstart_flag) OVER(partition by vid order by timestamp) as span_id
                    --,value, spanstart_flag, leaveNPL_flag, atNPL, latitude, longitude
                    from
                        --flag first span timestamp and first time leaving npl
                        (select vid, timestamp
                        ,if(inspan=1 and atNPL=1 and timestamp = min(timestamp) OVER (partition by vid, daystamp, inspan order by timestamp),1,0) as spanstart_flag
                        ,if(atNPL=1 and not lead(atNPL) OVER(partition by vid order by timestamp)=1,1,0) as leaveNPL_flag
                        from 
                            --flag times at NPL
                            (select vid, timestamp
                            , datetime_trunc(timestamp,DAY) as daystamp
                            , if(((parameter_id = 33 and value >= 800) or (parameter_id = 133 and value >= 400)) 
                              and (FORMAT_DATETIME('%A',timestamp) not in ('Saturday','Sunday') ) 
                              and (FORMAT_DATETIME('%H',timestamp) not in ('00','01','02','03','04','05','06','19','20','21','22','23')),1,0) as inspan
                            , if((latitude < {1} and latitude > {2}
                            and longitude < {3} and longitude > {4}),1,0) as atNPL
                            from UK.stage3_test
                            where vid={5} and timestamp > '{6}' and timestamp <= '{7}' and parameter_id in (33,133) and latitude is not null and longitude is not null
                            ) a
                        ) b
                    where spanstart_flag = 1 or leaveNPL_flag = 1

                    ) c 
                group by vid, span_id
                --order by vid, span_id
                ) d
            cross join unnest(d.timearray) as calstamp
            ) e
        where cast(e.calstamp as datetime) = z.timestamp and e.vid = z.vid
    """).format(destinationtable_str,n,s,e,w,vid,tstart,tstop)
    qry_job = bqclient.query(qry_str,location='EU',job_config=bigquery.QueryJobConfig())
    rows = qry_job.result()
    print("Measurement mode updated in table {0} for vehicle {1} calibration periods".format(destinationtable_str,vid))

def eco_serinus_calibration():    
    #Ecotech Serinus also has a span mode, flag any times in span mode
    qry_str = ("""UPDATE {0} a
    SET mmode = 3
    from UK.qaqc_hexstatus_lookup b
    WHERE a.status = b.status and a.dev_id = b.dev_id and status_desc like '%Currently in span mode%'
    AND a.dev_id = 4
    """.format(destinationtable_str))
    qry_job = bqclient.query(qry_str,location='EU',job_config=bigquery.QueryJobConfig())
    rows = qry_job.result()
    print("Measurement mode updated in table {0} for Eco Serinus span periods".format(destinationtable_str))

#Maintenance periods at Air Monitors also considered calibration periods
def flag_maintenance(vid,starttime,endtime):
    qry_str = ("""update {0} a
    set mmode = 3
    where vid = {1}
    and timestamp >= '{2}' 
    and timestamp < '{3}'
    """.format(destinationtable_str,vid,starttime,endtime))
    qry_job = bqclient.query(qry_str,location='eu',job_config=bigquery.QueryJobConfig())
    rows = qry_job.result()
    print("query results loaded into table {0}".format(destinationtable_str))

#measurement mode 4
#zero periods (scrubber/filter time series) 
#(includes both zero checks and longer gas zero calibrations
#is instrument-specific
#will only have 5 min zeros for more recent data after we changed the log format to collect times
def set_zero_periods():
    #Gas zero periods
    qry_str = ("""UPDATE {0} a
    SET mmode = 4
    WHERE vid = 27533
    AND dev_id in (1,2,4,5,6,7,8)
    AND timestamp > '2019-05-23 11:04:00'
    AND timestamp < '2019-05-23 12:48:00'
    """.format(destinationtable_str))
    qry_job = bqclient.query(qry_str,location='EU',job_config=bigquery.QueryJobConfig())
    rows = qry_job.result()
    
    qry_str = ("""UPDATE {0} a
    SET mmode = 4
    WHERE vid = 27533
    AND dev_id = 3
    AND timestamp > '2019-05-23 11:04:00'
    AND timestamp < '2019-05-23 11:50:00'
    """.format(destinationtable_str))
    qry_job = bqclient.query(qry_str,location='EU',job_config=bigquery.QueryJobConfig())
    rows = qry_job.result()
    
    qry_str = ("""UPDATE {0} a
    SET mmode = 4
    WHERE vid = 27522
    AND dev_id in (1,2,5,8)
    AND timestamp > '2019-06-05 10:02:00'
    AND timestamp < '2019-06-05 11:42:00'
    """.format(destinationtable_str))
    qry_job = bqclient.query(qry_str,location='EU',job_config=bigquery.QueryJobConfig())
    rows = qry_job.result()
    
    qry_str = ("""UPDATE {0} a
    SET mmode = 4
    WHERE vid = 27522
    AND dev_id in (3,4,6,7)
    AND timestamp > '2019-06-06 09:18:00'
    AND timestamp < '2019-06-06 10:58:00'
    """.format(destinationtable_str))
    qry_job = bqclient.query(qry_str,location='EU',job_config=bigquery.QueryJobConfig())
    rows = qry_job.result()
    print("Measurement mode updated in table {0} for gas zero periods".format(destinationtable_str))
    
    #no2 Aerodyne CAPS device automatically zeros/baselines, flag each of these periods as well
    qry_str = ("""UPDATE {0} a
    SET mmode = 4
    from UK.qaqc_hexstatus_lookup b
    WHERE a.status = b.status and a.dev_id = b.dev_id and status_desc like '%Baseline on - measurement period%'
    AND a.dev_id = 7
    """.format(destinationtable_str))
    qry_job = bqclient.query(qry_str,location='EU',job_config=bigquery.QueryJobConfig())
    rows = qry_job.result()
    print("Measurement mode updated in table {0} for CAPS baseline periods".format(destinationtable_str))
    
    #Ecotech Serinus also has a calibration mode, flag any times in zero mode
    qry_str = ("""UPDATE {0} a
    SET mmode = 4
    from UK.qaqc_hexstatus_lookup b
    WHERE a.status = b.status and a.dev_id = b.dev_id and status_desc like '%Currently in zero mode%'
    AND a.dev_id = 4
    """.format(destinationtable_str))
    qry_job = bqclient.query(qry_str,location='EU',job_config=bigquery.QueryJobConfig())
    rows = qry_job.result()
    print("Measurement mode updated in table {0} for Eco Serinus zero periods".format(destinationtable_str))

    #Weekly ~5 min zero periods
    qry_str = ("""
    UPDATE {0} a
    set mmode = 4
    from (select car_id, dev_id, meter, measure, zerostamp
          from (select car_id, j.dev_id, i.meter, measure
                , generate_timestamp_array(start_time_zero, end_time_zero, interval 1 SECOND) as zeroarray
                from UK.instrument_weekly_data i
                join UK.device_mapping j on i.meter=j.meter
                where start_time_zero is not null and end_time_zero is not null) z
          cross join unnest(z.zeroarray) as zerostamp
          ) x
    where cast(x.zerostamp as datetime) = a.timestamp and x.car_id = a.vid and x.dev_id = a.dev_id
    """.format(destinationtable_str))
    qry_job = bqclient.query(qry_str,location='EU',job_config=bigquery.QueryJobConfig())
    rows = qry_job.result()
    print("Measurement mode updated in table {0} for weekly 5 min zero periods".format(destinationtable_str))

#measurement mode 5
#colocation at stationary site other than NPL (AQMesh or reference monitoring sites)
#NA for this dataset

print('set mobile measurement mode')
set_mobile()
print('setting stationary at npl measurement mode')
set_nonmobile()
print('setting stationary at air monitors measurement mode')
set_nonmobile(n=52.006,s=52.004,e=-2.1226,w=-2.126)
print('setting calibration measurement mode')
#break into smaller chunks of time to avoid BQ memory issues
set_calibrationmode(27522,'2018-01-01','2019-01-01')
set_calibrationmode(27522,'2019-01-01','2020-01-01')
set_calibrationmode(27533,'2018-01-01','2019-01-01')
set_calibrationmode(27533,'2019-01-01','2020-01-01')
print('setting calibration measurement mode during maintenance periods')
flag_maintenance(27533,'2019-05-21','2019-05-24')
flag_maintenance(27522,'2019-06-04','2019-06-07')
print('setting gas zero period measurement mode')
set_zero_periods()

