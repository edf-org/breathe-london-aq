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

credentials = service_account.Credentials.from_service_account_file(r"..\..\..\pks\street-view-air-quality-london-dc8f329b26cf.json")
project_id = 'street-view-air-quality-london'
client = bigquery.Client(credentials=credentials,project=project_id)

central_stat = 'mean' #choose mean or median
parameter_id = 133 #NO2 in ppb
min_drives = 5
threshold = 40*0.52 #ug/m3 to ppb
if central_stat == 'mean':
    #daytime/weekday medians were ~ 8% greater than 24/7 medians as a result of diurnal cycle of NO2
    daytime_weekday_correction = .92 #see central2a_ulezeval_sampleuncertainty 
    central_calc = 'avg(value_median)'
    samp_uncert_csv = r'..\..\..\..\Data\Intermediate\central_tendency\central2_ulezeval_NO2_after_avg(value_median)_sampleuncertainty_2020Aug17.csv'
if central_stat == 'median':
    daytime_weekday_correction = .9 #see central2a_ulezeval_sampleuncertainty 
    central_calc = 'percentile_cont(value_median,0.5)'
    samp_uncert_csv = r'..\..\..\..\Data\Intermediate\central_tendency\central2_ulezeval_NO2_after_percentile_cont(value_median,0.5)_sampleuncertainty_2020Aug17.csv'

#true for all queries in this process
dataset_ref = client.dataset('UK')
location = 'EU'
destinationcsv_str = os.path.join(r'..\..\..\..\Data\Intermediate\central_tendency','central_'+central_stat+'_exceeds_WHO_confidence_post_ulez'+'_'+str(int(threshold/0.52))+'ugm3'+dt.date.today().strftime('_%Y%b%d')+'.csv')
#save intermediate table
destinationtable_str = 'central3_postulez_{0}no2'.format(central_stat)
job_config = bigquery.QueryJobConfig()
job_config.destination = dataset_ref.table(destinationtable_str)
job_config.write_disposition = 'WRITE_TRUNCATE' #overwrite table if exists

qry_str = ("""
    with cte0 as 
    (
    --calc medians over segment and before/after timeperiod
    select distinct a.segid, ulez_ineffect
    , count(value_median) over (partition by a.segid, ulez_ineffect) as pass_count
    , {2} over (partition by a.segid, ulez_ineffect) as central_tendency
    from 
        --determine whether ulez period in/not-in effect
        (select a.segid, value_median, if(hour1stamp<'2019-04-08',0,1) as ulez_ineffect
        from UK.snap4_drivepass_1hr_medians a
        where parameter_id = {0}
         ) a
    ),
    cte1 as 
    (
    --join before and after stats for segments with minimum number of drives
    select b.segid, pass_count, central_tendency
    from
        (select segid, pass_count, central_tendency from cte0
        where ulez_ineffect = 1 and pass_count >= {1}
        ) b
    )
    select ST_ASTEXT(geowkt) as the_geom, cte1.segid
    ,pass_count,central_tendency,function,ST_LENGTH(geowkt) as length_m
    ,ST_WITHIN(geowkt,ST_GEOGFROMTEXT(WKTgeom)) as inULEZ
    from cte1
    join UK.greaterlondon_roads_wgs84_split30m b
    on cte1.segid = b.segid
    cross join (select WKTgeom from UK.lcczone where BOUNDARY='CSS Area 1') c
    order by central_tendency desc
        """).format(parameter_id,min_drives,central_calc)
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
dfuncert = pd.read_csv(samp_uncert_csv)
confidence = []
for i,row in df.iterrows():
    n = min(int(row['pass_count']/5)*5,35)
    med = row['central_tendency']
    print(n,med)
    confidence.append(get_confidence(threshold,dfuncert,n,med,daytime_weekday_correction))
df['confidence'] = confidence
df.to_csv(destinationcsv_str,index=False)
print("Query results saved as csv {0}".format(destinationcsv_str))
