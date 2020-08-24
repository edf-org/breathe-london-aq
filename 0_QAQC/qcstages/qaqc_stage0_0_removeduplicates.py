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
destinationtable_str = 'stage0_test'
srctable_str = dataset_str+'.raw_data'
dataset_ref = bqclient.dataset(dataset_str)
table_ref = dataset_ref.table(destinationtable_str)
job_config = bigquery.QueryJobConfig()
job_config.destination = table_ref
job_config.write_disposition = 'WRITE_TRUNCATE' #overwrite table if exists

qry_str = ("""
--remove total duplicates
WITH cte0 AS (
SELECT DISTINCT * FROM {0}
),
--find duplicate timestamps
cte1 AS (
SELECT vid, node_id, timestamp, t_ct, last_in, min_value FROM
(SELECT vid, node_id, timestamp, count(timestamp) AS t_ct, max(id) AS last_in, min(value) as min_value 
FROM cte0 GROUP BY vid, node_id, timestamp)
WHERE t_ct = 2 --if there were more than 2, indicates a bigger issue, want to ignore this data
),
--find unique timestamps
cte2 AS (
SELECT vid, node_id, timestamp, t_ct 
FROM
(SELECT vid, node_id, timestamp, count(timestamp) AS t_ct 
FROM cte0 GROUP BY vid, node_id, timestamp)
WHERE t_ct = 1 --unique timestamps
)
--union duplicate and non-duplicate records
SELECT timestamp, node_id, parameter_id, status, value, id, vid
FROM 
(SELECT a.timestamp, a.node_id, a.parameter_id, a.status, a.value, a.id, a.vid FROM cte0 a
JOIN cte1 b ON a.vid=b.vid AND a.node_id = b.node_id AND a.timestamp = b.timestamp AND a.id = b.last_in
AND a.value = b.min_value
UNION ALL
SELECT a.timestamp, a.node_id, a.parameter_id, a.status, a.value, a.id, a.vid FROM cte0 a
JOIN cte2 b ON a.vid=b.vid AND a.node_id = b.node_id AND a.timestamp = b.timestamp 
)
""".format(srctable_str))
qry_job = bqclient.query(qry_str,location='EU',job_config=job_config)


rows = qry_job.result()
print("Query results loaded into table {0}".format(table_ref.path))

