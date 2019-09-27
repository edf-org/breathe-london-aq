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
client = bigquery.Client(credentials=credentials,project=project_id)

filename_str = r"..\..\..\..\Data\Intermediate\QAQC\hexstatus_lookup.csv"
dataset_str = 'UK'
table_str = 'qaqc_hexstatus_lookup'

dataset_ref = client.dataset(dataset_str)
table_ref = dataset_ref.table(table_str)
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.CSV
job_config.skip_leading_rows = 1
job_config.autodetect = True
job_config.write_disposition = 'WRITE_TRUNCATE' #overwrite table if exists
job_config.schema = [bigquery.SchemaField('dev_id','INTEGER'), \
        bigquery.SchemaField('status','STRING'), \
        bigquery.SchemaField('bin_str','STRING'), \
        bigquery.SchemaField('status_flag','INTEGER'), \
        bigquery.SchemaField('qc_flag','INTEGER'), \
        bigquery.SchemaField('status_desc','STRING')] 


with open(filename_str, 'rb') as source_file:
    job = client.load_table_from_file(
        source_file,
        table_ref,
        location='EU',  # Must match the destination dataset location.
        job_config=job_config)  # API request

job.result()  # Waits for table load to complete.

print('Loaded {} rows into {}:{}.'.format(
    job.output_rows, dataset_str, table_str))
