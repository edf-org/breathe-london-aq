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
import ggplot as gg
import pandas as pd

credentials = service_account.Credentials.from_service_account_file(
    r"..\..\..\pks\street-view-air-quality-london-dc8f329b26cf.json")
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
df.to_csv(r'..\charts\hourdistrib.csv')
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
dfw.to_csv(r'..\charts\weekdaydistrib.csv')
print("Weekday distribution")
print(dfw)

