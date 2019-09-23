# set null values where flagged, add measurement mode column
# select into initial stage 3 table
from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    r"C:\Users\lpadilla\Documents\London\Scripts\pks\street-view-air-quality-london-dc8f329b26cf.json")
project_id = 'street-view-air-quality-london'
bqclient = bigquery.Client(credentials=credentials,project=project_id)

dataset_str = 'UK'
destinationtable_str = 'stage3_test'
srctable_str = dataset_str+'.stage2_test'
dataset_ref = bqclient.dataset(dataset_str)
table_ref = dataset_ref.table(destinationtable_str)
job_config = bigquery.QueryJobConfig()
job_config.destination = table_ref
job_config.write_disposition = 'WRITE_TRUNCATE' #overwrite table if exists

qry_str = ("""
SELECT a.vid, a.timestamp, a.parameter_id
,if(qc_flag in (4,5,6,7,9,91,92,93,10), null, value) as value
,a.status, a.qc_flag, a.dev_id, a.latitude, a.longitude, a.speed_kmh
,null as mmode
FROM {0} a
""").format(srctable_str)

qry_job = bqclient.query(qry_str,location='EU',job_config=job_config)


rows = qry_job.result()
print("Query results loaded into table {0}".format(table_ref.path))
