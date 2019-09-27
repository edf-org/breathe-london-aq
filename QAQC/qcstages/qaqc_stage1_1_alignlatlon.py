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
destinationtable_str = 'stage1_test'
srctable_str = dataset_str+'.stage0_test'
clocktable_str = dataset_str+'.stage0_clock_uncertainty_test'
dataset_ref = bqclient.dataset(dataset_str)
table_ref = dataset_ref.table(destinationtable_str)
job_config = bigquery.QueryJobConfig()
job_config.destination = table_ref
job_config.write_disposition = 'WRITE_TRUNCATE' #overwrite table if exists

qry_str = ("""
with cte0 AS (
--translate the measurement timestamps by the instrument offset
--offsets differ pre and post filter and firmware changes
--offset is 0 for parameters not in lagtime_lookup
SELECT datetime_sub(timestamp, INTERVAL if(timestamp < ifnull(filter_change_date,timestamp), pre_filter_alignment_s,if(timestamp < ifnull(palas_change_date,timestamp), post_filter_alignment_s,ifnull(post_palas_alignment_s,0))) SECOND) AS timestamp_aligned
,timestamp AS measurementtime_prealigned
,b.dev_id, a.parameter_id, status, value, a.vid 
FROM {0} a
JOIN UK.qaqc_parameters_lookup b ON a.parameter_id = b.parameter_id
JOIN UK.qaqc_devices_lookup c ON b.dev_id = c.dev_id
LEFT JOIN UK.qaqc_lagtimes_lookup d ON a.vid = d.vid AND a.parameter_id = d.parameter_id
WHERE a.parameter_id not in (144,145)
),
cte1 AS (
SELECT vid, timestamp, value AS latitude 
FROM {0}
WHERE parameter_id = 144
),
cte2 AS (
SELECT vid, timestamp, value AS longitude 
FROM {0}
where parameter_id = 145
),
cte3 AS (
SELECT vid, timestamp, value AS speed_kmh 
FROM {0}
where parameter_id = 147
)
SELECT cte0.vid, cte0.measurementtime_prealigned, cte0.timestamp_aligned as timestamp
,cte0.parameter_id, cte0.value, cte0.dev_id, cte0.status
,cte1.latitude, cte2.longitude, cte3.speed_kmh, z.clock_diff_s, z.clock_diff_s*cte3.speed_kmh*0.277778 as clock_spatial_uncertainty_m
FROM cte0
LEFT JOIN cte1 ON cte0.vid = cte1.vid AND cte0.timestamp_aligned = cte1.timestamp
LEFT JOIN cte2 ON cte0.vid = cte2.vid AND cte0.timestamp_aligned = cte2.timestamp
LEFT JOIN cte3 ON cte0.vid = cte3.vid AND cte0.timestamp_aligned = cte3.timestamp
LEFT JOIN {1} z on cte0.vid = z.vid AND cte0.dev_id = z.dev_id AND cte0.timestamp_aligned = z.gpstime
""".format(srctable_str,clocktable_str))
qry_job = bqclient.query(qry_str,location='EU',job_config=job_config)


rows = qry_job.result()
print("Query results loaded into table {0}".format(table_ref.path))
