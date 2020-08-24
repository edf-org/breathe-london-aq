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
destinationcsv_str = os.path.join(r'..\..\..\..\Data\Intermediate\central_tendency','central3di_checktrialuniformity'+dt.date.today().strftime('_%Y%b%d')+'.csv')
job_config = bigquery.QueryJobConfig()

qry_str = ("""
    with cte0 as 
    (
    --calc expected values using all N passes (~truth) and assign idx for subsampling
    select segid, hour1stamp, value_median as conc_1hr
    ,row_number() over (partition by segid) as idx
    ,percentile_cont(value_median, 0.5) over (partition by segid) as conc_expected
    from UK.snap4_drivepass_1hr_medians
    where hour1stamp >= '2019-04-08' and parameter_id = 133
    --generate subsample index arrays, M per segment and n samples
    )
    , cte1 as 
    (
    select segid, n
    ,trial_id
    --arrays of top n drive indices ordered by random number without replacement
    ,array_agg(idx) over (partition by segid,trial_id order by rid rows between current row and 4 following) as sample_arr5
    ,array_agg(idx) over (partition by segid,trial_id order by rid rows between current row and 9 following) as sample_arr10
    ,array_agg(idx) over (partition by segid,trial_id order by rid rows between current row and 14 following) as sample_arr15
    ,array_agg(idx) over (partition by segid,trial_id order by rid rows between current row and 19 following) as sample_arr20
    ,array_agg(idx) over (partition by segid,trial_id order by rid rows between current row and 24 following) as sample_arr25
    ,array_agg(idx) over (partition by segid,trial_id order by rid rows between current row and 29 following) as sample_arr30
    ,array_agg(idx) over (partition by segid,trial_id order by rid ROWS BETWEEN CURRENT ROW AND 34 FOLLOWING) as sample_arr35
    ,rid
    ,min(rid) over (partition by segid,trial_id) as minrid
    from (
      --make M copies of the passes for M trials, each trial will have a different random selection of n passes
      select segid, max(idx) over (partition by segid) as N --total number of passes
      ,idx
      ,rand() as rid --random number assigned to each record
      ,trial_id
      from cte0
      ,unnest(generate_array(1,100)) trial_id --number of trials
      --order by segid, rid
      )  
    --limit to segments with at least 100 drives
    where N >= 75
    )
    , cte2 as
    (
    --union results for different n
    select segid, trial_id, N
    , 5 as n_sub, sample_arr5 as sample_arr
    from cte1
    where rid = minrid --take first result
    union all
    select segid, trial_id, N
    , 10 as n_sub, sample_arr10 as sample_arr
    from cte1
    where rid = minrid
    union all
    select segid, trial_id, N
    , 15 as n_sub, sample_arr15 as sample_arr
    from cte1
    where rid = minrid
    union all
    select segid, trial_id, N
    , 20 as n_sub, sample_arr20 as sample_arr
    from cte1
    where rid = minrid
    union all
    select segid, trial_id, N
    , 25 as n_sub, sample_arr25 as sample_arr
    from cte1
    where rid = minrid
    union all
    select segid, trial_id, N
    , 30 as n_sub, sample_arr30 as sample_arr
    from cte1
    where rid = minrid
    union all
    select segid, trial_id, N
    , 35 as n_sub, sample_arr35 as sample_arr
    from cte1
    where rid = minrid
    )
    , cte3 as 
    (
    --join concentration data to the expanded subsamples
    select a.segid, a.hour1stamp
    , b.n_sub, b.trial_id, a.conc_expected, a.conc_1hr
    --concentration data
    from cte0 a
    join 
    --expanded subsample trials
    (select segid, trial_id, n_sub, sample_idx 
    from cte2, unnest(sample_arr) sample_idx) b
    on a.segid = b.segid and a.idx = b.sample_idx
    group by segid, hour1stamp, n_sub, trial_id, conc_expected, conc_1hr
    )
--finally calc period between samples
SELECT segid, trial_id, n_sub, n, dminus, hour1stamp
, round(datetime_diff(datetime(2019,11,1,0,0,0),datetime(2019,4,8,0,0,0), DAY)/n,0) as uniform_period_d
, datetime_diff(hour1stamp,dminus,DAY) as actual_period_d
FROM (
SELECT segid,trial_id,n_sub,hour1stamp
, count(hour1stamp) over (partition by segid, trial_id, n_sub) as n
, lag(hour1stamp) over ( partition by segid, trial_id, n_sub order by hour1stamp) as dminus
--, lead(hour1stamp) over (partition by segid, trial_id, n_sub order by hour1stamp) as dplus
FROM cte3
)
where n >= 5 and dminus is not null
order by segid,trial_id,n_sub,hour1stamp""")

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
ax.set_ylabel('Days between drives, random trials')

fig.savefig(r'..\charts\central3di_checktrialuniformity_{0}.png'.format(dt.date.today().strftime('%Y%b%d')))
