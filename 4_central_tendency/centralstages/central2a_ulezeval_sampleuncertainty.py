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
import numpy as np
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt
credentials = service_account.Credentials.from_service_account_file(r"..\..\..\pks\street-view-air-quality-london-dc8f329b26cf.json")
project_id = 'street-view-air-quality-london'
client = bigquery.Client(credentials=credentials,project=project_id)
location = 'EU'
plot_list = [{'beforeafter':'<','beforeafter_str':'before'},\
             {'beforeafter':'>=','beforeafter_str':'after'}]
pct_list = range(1,100) #every 1 percentile
####! for mean
central_stat = 'avg(value_median)'
####! for median
#central_stat = 'percentile_cont(value_median,0.5)'

####!for percent error
percentile_str = ''.join([",percentile_cont(SAFE_DIVIDE(conc_expected-conc_sub_expected,conc_sub_expected)*100,{0}) over (partition by n_sub) as p{1:02}t".format(pct/100, pct) for pct in pct_list])
field_str = ''.join([', ANY_VALUE(p{0:02}t) as p{0:02}t'.format(pct) for pct in pct_list])
ylabel = 'Sampling uncertainty (%)'

#####!for total error
#percentile_str = ''.join([",percentile_cont((conc_expected-conc_sub_expected),{0}) over (partition by n_sub) as p{1:02}t".format(pct/100, pct) for pct in pct_list])
#field_str = ''.join([', ANY_VALUE(p{0:02}t) as p{0:02}t'.format(pct) for pct in pct_list])
#ylabel = 'Sampling uncertainty (conc units)'

#for species in ['NO2', 'NOx', 'PM2.5 PDR','CO2 dry','O3']:
for species in ['NO2', 'PM2.5 Palas']:
    if species == 'NO2':
        pid = 133
        unit = 'ppb'
    elif species == 'NOx':
        pid = 163
        unit = 'ug/m3'
    elif species == 'PM2.5 PDR':
        pid = 140
        unit = 'ug/m3'
    elif species == 'PM2.5 Palas':
        pid = 3
        unit = 'ug/m3'
    elif species == 'CO2 dry':
        pid = 33
        unit = 'ppm'
        ymin = 400
    elif species == 'O3':
        pid = 127
        unit = 'ppb'
    plt.rcParams.update({'font.size':14})
    fig,axes = plt.subplots(1,2,figsize=(12,6))
    for i,p in enumerate(plot_list):
        ###############################################################
        #central2 Evaluate segment medians before and after ULEZ in-effect
        #include only segments with min 75 drives in period
        #separate by road function and inside-ulez vs outside-ulez? not sure will have enough segments
        #select segments
        qry_str = """
        with cte0 as 
        (
        --calc expected values using all N passes (~truth) and assign idx for subsampling
        select segid, hour1stamp, value_median
        ,row_number() over (partition by segid) as idx
        ,{4} over (partition by segid) as conc_expected
        from UK.snap4_drivepass_1hr_medians
        where hour1stamp {1} '2019-04-08' and parameter_id = {0}
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
          ,unnest(generate_array(1,500)) trial_id --number of trials
          --order by segid, rid
          )  
        --limit to segments with at least 75 drives
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
        select segid, n_sub, trial_id, conc_expected 
        ,ANY_VALUE(conc_sub_expected) as conc_sub_expected
        from (
            select a.segid, b.n_sub, b.trial_id
            , a.conc_expected
            --calc expected values over subsample trials
            ,{4} over (partition by a.segid, b.n_sub, b.trial_id) as conc_sub_expected
            --concentration data
            from cte0 a
            join 
            --expanded subsample trials
            (select segid, trial_id, n_sub, sample_idx 
            from cte2, unnest(sample_arr) sample_idx) b
            on a.segid = b.segid and a.idx = b.sample_idx
            ) 
        group by segid, n_sub, trial_id, conc_expected
        )
        --calc summary statistics on bias %
        select n_sub
        {2}
        , max(trial_id) as n_trial, ANY_VALUE(n_unique)/max(trial_id) as n_seg
        from (
        select n_sub, trial_id
        {3}
        ,count(segid) over (partition by n_sub) as n_unique
        from cte3
        )
        group by n_sub
        order by n_sub
        """.format(pid,p['beforeafter'],field_str,percentile_str,central_stat)
        print(species)
        destinationtable_str = 'central2_ulezeval_{0}_{1}_{3}_sampleuncertainty_{2}.csv'.format(species.replace(' ','_'),p['beforeafter_str'],dt.date.today().strftime('%Y%b%d'),central_stat)
        destinationcsv_str = os.path.join(r'..\..\..\..\Data\Intermediate\central_tendency',destinationtable_str)
        qry_job = client.query(qry_str,location=location,job_config=bigquery.QueryJobConfig())
        df = qry_job.to_dataframe()
        df.to_csv(destinationcsv_str,index=False)
        #df = pd.read_csv(destinationcsv_str)
        
        #plots
        ax = axes.flat[i]
        ax.plot(df['n_sub'],df['p05t'],'b',label='5th %')
        ax.plot(df['n_sub'],df['p95t'],'k',label='95th %')

        ax.set_xlabel('n drives')
        ax.set_ylabel(ylabel)
        ax.legend(loc='right')
        ax.title.set_text('{0} uncertainty \n({1} trials for {2} segments \nwith >75 passes {3} ULEZ\n{4})'.format(species,df['n_trial'][0],df['n_seg'][0],p['beforeafter_str'],central_stat))
    fig.tight_layout()
    fig.savefig(r'..\charts\ulezeval_{0}_{2}_sampleuncertainty_{1}.png'.format(species.replace(' ','_'),dt.date.today().strftime('%Y%b%d'),central_stat))
