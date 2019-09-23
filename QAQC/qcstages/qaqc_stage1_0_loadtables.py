from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    r"C:\Users\lpadilla\Documents\London\Scripts\pks\street-view-air-quality-london-dc8f329b26cf.json")
project_id = 'street-view-air-quality-london'
client = bigquery.Client(credentials=credentials,project=project_id)

dataset_str = 'UK'
dataset_ref = client.dataset(dataset_str)
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.CSV
job_config.skip_leading_rows = 1
job_config.autodetect = True
job_config.write_disposition = 'WRITE_TRUNCATE' #overwrite table if exists

#load device table

filename_str = r"C:\Users\lpadilla\Documents\London\Data\Intermediate\QAQC\devices.csv"
table_str = 'qaqc_devices_lookup'
table_ref = dataset_ref.table(table_str)
job_config.schema = [bigquery.SchemaField('dev_id','INTEGER'), \
        bigquery.SchemaField('dev_desc','STRING'),\
        bigquery.SchemaField('hexstart','INTEGER'),\
        bigquery.SchemaField('hexlen','INTEGER'),\
        bigquery.SchemaField('status_type','STRING')] 

with open(filename_str, 'rb') as source_file:
    job = client.load_table_from_file(
        source_file,
        table_ref,
        location='EU',  # Must match the destination dataset location.
        job_config=job_config)  # API request

job.result()  # Waits for table load to complete.

print('Loaded {} rows into {}:{}.'.format(
    job.output_rows, dataset_str, table_str))

#load parameters table

filename_str = r"C:\Users\lpadilla\Documents\London\Data\Intermediate\QAQC\parameters.csv"
table_str = 'qaqc_parameters_lookup'
table_ref = dataset_ref.table(table_str)
job_config.schema = [bigquery.SchemaField('field_name','STRING'), \
        bigquery.SchemaField('data_type','STRING'),\
        bigquery.SchemaField('mode','STRING'),\
        bigquery.SchemaField('parameter_id','INTEGER'),\
        bigquery.SchemaField('name','STRING'),\
        bigquery.SchemaField('units','STRING'),\
        bigquery.SchemaField('sampling_timebase_us','INTEGER'),\
        bigquery.SchemaField('logging_timebase_us','INTEGER'),\
        bigquery.SchemaField('dev_id','INTEGER'),\
        bigquery.SchemaField('log_method','STRING'),\
        bigquery.SchemaField('included','INTEGER'),\
        bigquery.SchemaField('data_class','STRING')] 

with open(filename_str, 'rb') as source_file:
    job = client.load_table_from_file(
        source_file,
        table_ref,
        location='EU',  # Must match the destination dataset location.
        job_config=job_config)  # API request

job.result()  # Waits for table load to complete.

print('Loaded {} rows into {}:{}.'.format(
    job.output_rows, dataset_str, table_str))

#load lagtimes table

filename_str = r"C:\Users\lpadilla\Documents\London\Data\Intermediate\QAQC\lagtimes.csv"
table_str = 'qaqc_lagtimes_lookup'
table_ref = dataset_ref.table(table_str)
job_config.schema = [bigquery.SchemaField('node_id','STRING'),\
        bigquery.SchemaField('vid','INTEGER'), \
        bigquery.SchemaField('parameter_id','INTEGER'),\
        bigquery.SchemaField('post_palas_offset_s','FLOAT'),\
        bigquery.SchemaField('post_filter_offset_s','FLOAT'),\
        bigquery.SchemaField('pre_filter_offset_s','FLOAT'),\
        bigquery.SchemaField('post_palas_alignment_s','INTEGER'),\
        bigquery.SchemaField('post_filter_alignment_s','INTEGER'),\
        bigquery.SchemaField('pre_filter_alignment_s','INTEGER'),\
        bigquery.SchemaField('filter_change_date','DATETIME'),\
        bigquery.SchemaField('palas_change_date','DATETIME')] 

with open(filename_str, 'rb') as source_file:
    job = client.load_table_from_file(
        source_file,
        table_ref,
        location='EU',  # Must match the destination dataset location.
        job_config=job_config)  # API request

job.result()  # Waits for table load to complete.

print('Loaded {} rows into {}:{}.'.format(
    job.output_rows, dataset_str, table_str))
