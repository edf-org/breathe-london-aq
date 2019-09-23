from google.cloud import bigquery
from google.oauth2 import service_account
import ggplot as gg
import pandas as pd

credentials = service_account.Credentials.from_service_account_file(
    r"..\..\pks\street-view-air-quality-london-dc8f329b26cf.json")
project_id = 'street-view-air-quality-london'
bqclient = bigquery.Client(credentials=credentials,project=project_id)
job_config = bigquery.QueryJobConfig()

qry_str = ("""
select hourofday, ct, sum(ct) over () as tot, ct/(sum(ct) over ())*100 as pct from 
(
select EXTRACT(HOUR from hour1stamp) as hourofday, count(hour1stamp) as ct from UK.snap4_drivepass_1hr_medians
where parameter_id = 133
group by hourofday
order by ct desc
)
        """)

qry_job = bqclient.query(qry_str,location='EU',job_config=bigquery.QueryJobConfig())
#save result as dataframe
df = qry_job.to_dataframe()
df.to_csv(r'.\charts\hourdistrib.csv')
print("Hour distribution")
print(df)


qry_str = ("""
select dayofweek, ct, sum(ct) over () as tot, ct/(sum(ct) over ())*100 as pct from 
(
select EXTRACT(DAYOFWEEK from hour1stamp) as dayofweek, count(hour1stamp) as ct from UK.snap4_drivepass_1hr_medians
where parameter_id = 133
group by dayofweek
order by ct desc
)
        """)

qry_job = bqclient.query(qry_str,location='EU',job_config=bigquery.QueryJobConfig())
#save result as dataframe
dfw = qry_job.to_dataframe()
dfw.to_csv(r'.\charts\weekdaydistrib.csv')
print("Weekday distribution")
print(dfw)

