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
sourcetable_str = dataset_str+'.stage3_test'
destinationtable_str = 'stage3_test'
dataset_ref = bigquery.DatasetReference(project_id,dataset_str)
job_config = bigquery.QueryJobConfig()
job_config.destination = bigquery.TableReference(dataset_ref,destinationtable_str)
job_config.write_disposition = 'WRITE_APPEND'
#calc nox as no2 and append to stage3 table
#only do ONCE!
def noxasno2(queryjobconfig):
    qry_str = ("""

    with cte0 as (
    --no2 ppb
    select vid, timestamp, parameter_id, value as no2, status, qc_flag, dev_id, latitude, longitude, speed_kmh, mmode from UK.stage3_test
    where parameter_id = 133 and mmode = 1 and qc_flag in (0,2,21,22,23,24,25,26,27,28)
    )
    ,cte1 as (
    --no ppm
    select vid, timestamp, parameter_id, value as no_, status, qc_flag, dev_id, latitude, longitude, speed_kmh, mmode from UK.stage3_test
    where parameter_id = 45 and mmode = 1 and qc_flag in (0,2,21,22,23,24,25,26,27,28)
    )
    select a.vid, a.timestamp, 163 as parameter_id, (no2+1000*no_)*(1/0.52) as value, a.status, GREATEST(a.qc_flag,b.qc_flag) as qc_flag, null as dev_id, a.latitude, a.longitude, a.speed_kmh, a.mmode
    from cte0 a join cte1 b on a.vid = b.vid and a.timestamp = b.timestamp and a.latitude = b.latitude and a.longitude = b.longitude 
    """.format(destinationtable_str))

    qry_job = bqclient.query(qry_str,location='EU',job_config=queryjobconfig)
    rows = qry_job.result()
    print("Query results loaded into table {0}".format(destinationtable_str))

noxasno2(job_config)

