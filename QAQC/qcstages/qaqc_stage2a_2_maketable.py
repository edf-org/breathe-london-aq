# join raw_data to status lookup on status and dev
# select into initial stage 2 table
from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    r"C:\Users\lpadilla\Documents\London\Scripts\pks\street-view-air-quality-london-dc8f329b26cf.json")
project_id = 'street-view-air-quality-london'
bqclient = bigquery.Client(credentials=credentials,project=project_id)

dataset_str = 'UK'
destinationtable_str = 'stage2_test'
srctable_str = dataset_str+'.stage1_test'
dataset_ref = bqclient.dataset(dataset_str)
table_ref = dataset_ref.table(destinationtable_str)
job_config = bigquery.QueryJobConfig()
job_config.destination = table_ref
job_config.write_disposition = 'WRITE_TRUNCATE' #overwrite table if exists

qry_str = ("""
SELECT distinct a.vid, a.timestamp, a.parameter_id, a.value, a.status, IFNULL(b.qc_flag,0) as qc_flag, a.dev_id, a.latitude, a.longitude
,a.speed_kmh, a.clock_spatial_uncertainty_m
--account for 10s rolling averages on Palas measurements before firmware update and in PM4 and cn for all time
--this is also a place where we could account for the spatial uncertainty 
--due to the width of the impulse response for individual parameters
,if((a.dev_id = 1 and a.timestamp < '2019-04-13') or (a.parameter_id in (1,4) ),GREATEST(a.clock_spatial_uncertainty_m,10*a.speed_kmh*.277778),a.clock_spatial_uncertainty_m) as spatial_uncertainty_m
FROM {0} a
LEFT JOIN UK.qaqc_hexstatus_lookup b
ON a.dev_id = b.dev_id AND a.status = b.status
""").format(srctable_str)

qry_job = bqclient.query(qry_str,location='EU',job_config=job_config)


rows = qry_job.result()
print("Query results loaded into table {0}".format(table_ref.path))
