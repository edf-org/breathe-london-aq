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
import sys
sys.path.append(r'..\helpers')
import helpers_sql_stage0_1 as sq

credentials = service_account.Credentials.from_service_account_file(
    r"..\..\..\pks\street-view-air-quality-london-dc8f329b26cf.json")
project_id = 'street-view-air-quality-london'
bqclient = bigquery.Client(credentials=credentials,project=project_id)

srctable_str = 'UK.stage0_test'
location = 'EU'
dataset_ref = bqclient.dataset('UK')
job_config1 = bigquery.QueryJobConfig()
job_config1.destination = dataset_ref.table('tmp')
job_config1.write_disposition = 'WRITE_TRUNCATE' #overwrite table if exists
job_config2 = bigquery.QueryJobConfig()
job_config2.destination = dataset_ref.table('stage0_clock_uncertainty_test')
job_config2.write_disposition = 'WRITE_TRUNCATE' #overwrite table if exists on initial pass

loop_dict={0 : [{'vid':27522,'where':"timestamp<'2019-01-01T00:00:00'"},
                {'vid':27533,'where':"timestamp<'2018-12-10T00:00:00' and timestamp>='2018-08-16T15:55:20'"}]
          ,1 : [{'vid':27522,'where':"timestamp>='2019-01-03T13:02:56'"},
                {'vid':27533,'where':"timestamp >= '2018-12-11T10:49:28'"}]}
dev_list = [1,2,3,4,5,6,7,8]
#note device 6 (o3) only logging every 2s, this code should still find the nearest gps time
#for each unique id period (before and after database reset)
for k,v_list in loop_dict.items():
    #for each vehicle (they have different reset timestamps)
    for v in v_list:
        vid = v['vid']
        where = v['where']
        #for each device
        for d in dev_list:
            #stage tmp table of ids for given vehicle, device, id period
            sq.make_tmp(srctable_str,d,where,vid,bqclient,location,job_config1)
            #calc clock uncertainty from gps timestamp to nearest timestamp of given device
            sq.calc_clock_diff(d,where,vid,bqclient,location,job_config2)
            job_config2.write_disposition = 'WRITE_APPEND' #after initial pass append to output table

#output is an intermediate table 
#to be joined at stage 1 on gps_time, dev_id, and vid  
#after measurement times are re-aligned to account for instrument response times
#and after aligned measurements are matched with gps coordinates
