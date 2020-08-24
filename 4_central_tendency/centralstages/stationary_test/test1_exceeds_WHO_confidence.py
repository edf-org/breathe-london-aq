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

credentials = service_account.Credentials.from_service_account_file(r"..\..\..\..\pks\street-view-air-quality-london-dc8f329b26cf.json")
project_id = 'street-view-air-quality-london'
client = bigquery.Client(credentials=credentials,project=project_id)

parameter_id = 133 #NO2 in ppb
min_drives = 5
who_threshold = 40 #ug/m3
#daytime/weekday medians were ~ 4.27% greater than 24/7 medians as a result of diurnal cycle of NO2
#daytime_weekday_correction = 1/1.043 #see temporal_bias\NO2_daytime_weekday_bias.xlsx 
daytime_weekday_correction = 1 #see temporal_bias\NO2_daytime_weekday_bias.xlsx 
#true for all queries in this process
dataset_ref = client.dataset('UK')
location = 'EU'
destinationcsv_str = os.path.join(r'..\..\..\..\..\Data\Intermediate\central_tendency\stationary_test','test1_exceeds_WHO_confidence_no_daytimeweekdaycorr'+dt.date.today().strftime('_%Y%b%d')+'.csv')
job_config = bigquery.QueryJobConfig()
#save intermediate table
#destinationtable_str = 'central3_postulez_summaryno2'
#job_config.destination = dataset_ref.table(destinationtable_str)
#job_config.write_disposition = 'WRITE_TRUNCATE' #overwrite table if exists

qry_str = ("""
    with cte0 as 
    (
    --100 random segids as 'trials'
    select segid, pass_count
    from 
        --filter pass_count > 5, add random id
        (select segid, rand() as rid, pass_count
        from 
            --find segments with passes post ULEZ, inside ULEZ
            (select a.segid, count(hour1stamp) as pass_count
            from UK.snap4_drivepass_1hr_medians a
            join UK.greaterlondon_roads_wgs84_split30m b
            on a.segid = b.segid
            cross join (select WKTgeom from UK.lcczone where BOUNDARY = 'CSS Area 1') c
            where parameter_id = {0} and hour1stamp > '2019-04-08'
            and ST_WITHIN(b.geowkt,ST_GEOGFROMTEXT(WKTgeom))
            group by segid
             ) a
        where pass_count >= {1}
        )
    order by rid limit 100
    ),
    cte1 as 
    (
    --join segids, hours, pod data to get estimated medians for 100 drive patterns (segids)
    select segid, pod_id_location, ANY_VALUE(pass_count) as pass_count, ANY_VALUE(median) as median
    from
        (
        select cte0.segid, pod_id_location
        , count(date_UTC) over (partition by pod_id_location, cte0.segid) as pass_count
        , percentile_cont(no2_ugm3,0.5) over (partition by pod_id_location, cte0.segid) as median
        from cte0
        join UK.snap4_drivepass_1hr_medians a
        on cte0.segid = a.segid
        join AQMesh.NO2_AQMesh_Scaled_Dataset_UGM3_20180901_20200112 b
        on a.hour1stamp = DATETIME(b.date_UTC)
        where parameter_id = {0} and hour1stamp > '2019-04-08' and no2_ugm3 > -999
        )
    where pass_count >= {1}
    group by segid, pod_id_location
    ),
    cte2 as
    (
    --get the actual exceedance boolean by pod_id
    select pod_id_location, ANY_VALUE(actual_median) as actual_median
    from (
        select pod_id_location, percentile_cont(no2_ugm3,0.5) over (partition by pod_id_location) as actual_median
        from AQMesh.NO2_AQMesh_Scaled_Dataset_UGM3_20180901_20200112
        where date_UTC > '2019-04-08' and no2_ugm3 > -999
        )
    group by pod_id_location
    )
    select cte1.pod_id_location, segid
    ,pass_count, median, actual_median, if(actual_median>{2},1,0) as actually_exceeded_bool
    from cte1
    join cte2 
    on cte1.pod_id_location = cte2.pod_id_location
    order by median desc
        """).format(parameter_id,min_drives,who_threshold)
qry_job = client.query(qry_str,location=location,job_config=job_config)
df = qry_job.to_dataframe()

def get_confidence(thresh, df, n, med, corr=1):
    distrib = df[df['n_sub']==n]
    pct_diff = np.array([distrib['p{0:02}t'.format(a)].iloc[0] for a in range(1,100)])
    x = (pct_diff/100+1)*med*corr #apply daytime weekday correction if needed
    #find exceedance percentile of x closest to threhold
    y = 1-np.array(range(1,100))/100
    c = np.interp(thresh,x,y)
    return c

#load the sampling uncertainty distributions
dfuncert = pd.read_csv(r'..\..\..\..\..\Data\Intermediate\central_tendency\stationary_test\test0_NO2_after_sampleuncertainty_2020Apr08.csv')
confidence = []
for i,row in df.iterrows():
    n = min(int(row['pass_count']/5)*5,35)
    med = row['median']
    print(n,med)
    confidence.append(get_confidence(who_threshold,dfuncert,n,med,corr=daytime_weekday_correction))
df['confidence'] = confidence
df.to_csv(destinationcsv_str,index=False)
print("Query results saved as csv {0}".format(destinationcsv_str))
