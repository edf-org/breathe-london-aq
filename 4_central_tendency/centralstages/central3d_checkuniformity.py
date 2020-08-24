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
import glob
import os
import sys
import datetime as dt
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

credentials = service_account.Credentials.from_service_account_file(r"..\..\..\pks\street-view-air-quality-london-dc8f329b26cf.json")
project_id = 'street-view-air-quality-london'
client = bigquery.Client(credentials=credentials,project=project_id)

#true for all queries in this process
dataset_ref = client.dataset('UK')
location = 'EU'
destinationcsv_str = os.path.join(r'..\..\..\..\Data\Intermediate\central_tendency','central_checkuniformity'+dt.date.today().strftime('_%Y%b%d')+'.csv')
job_config = bigquery.QueryJobConfig()

qry_str = ("""
SELECT segid, n, dminus, hour1stamp
, round(datetime_diff(datetime(2019,11,1,0,0,0),datetime(2019,4,8,0,0,0), DAY)/n,0) as uniform_period_d
, datetime_diff(hour1stamp,dminus,DAY) as actual_period_d
FROM (
SELECT segid, hour1stamp
, count(hour1stamp) over (partition by segid) as n
, lag(hour1stamp) over ( partition by segid order by hour1stamp) as dminus
--, lead(hour1stamp) over (partition by segid order by hour1stamp) as dplus
FROM `street-view-air-quality-london.UK.snap4_drivepass_1hr_medians`
where hour1stamp >= '2019-04-08' and parameter_id = 133
)
where n >= 5 and dminus is not null
order by segid, hour1stamp""")

#qry_job = client.query(qry_str,location=location,job_config=job_config)
#df = qry_job.to_dataframe()
#df.to_csv(destinationcsv_str,index=False)
print("Query results saved as csv {0}".format(destinationcsv_str))
df = pd.read_csv(destinationcsv_str)

plt.rcParams.update({'font.size':14})
bplt_list = []
label_list = []
fig,ax = plt.subplots(1,1,figsize=(8,6))
for period in df.sort_values(by=['n'])['uniform_period_d'].unique():
    bplt_list.append(df[df['uniform_period_d']==period]['actual_period_d'])
    label_list.append('{0:.0f}\n{1:.0f}'.format(period,df[df['uniform_period_d']==period]['n'].iloc[0]))
ax.boxplot(bplt_list, labels=label_list, showfliers=False,whis=[5,95])
ax.set_ylabel('Days between drives')

fig.savefig(r'..\charts\central3d_checkuniformity_{0}.png'.format(dt.date.today().strftime('%Y%b%d')))
