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

#true for all queries in this process
dataset_ref = client.dataset('UK')
location = 'EU'
min_drives = 5
inpolygon = False
for species in ['NO2', 'NOx', 'PM2.5 PDR','CO2 dry','O3']:
    if species == 'NO2':
        pid = 133
        unit = 'ppb'
        efunc_before = lambda n : {5:0.8,10:0.6,15:0.45,20:0.4,25:0.375,30:0.35,35:0.3}[min(round(n/5)*5,35)]
        efunc_after = lambda n : {5:0.7,10:0.5,15:0.4,20:0.35,25:0.3,30:0.25,35:0.2}[min(round(n/5)*5,35)]
    elif species == 'NOx':
        pid = 163
        unit = 'ug/m3'
    elif species == 'PM2.5 PDR':
        pid = 140
        unit = 'ug/m3'
    elif species == 'CO2 dry':
        pid = 33
        unit = 'ppm'
    elif species == 'O3':
        pid = 127
        unit = 'ppb'
    ###############################################################
    #central2 Evaluate sampling uncertainty in medians before and after ULEZ in-effect
    #include only segments with min n drives in BOTH periods

    print(species)
    destinationtable_str = 'central2_ulezeval_{0}_{1}drivesb4andafter'.format(species.replace(' ','_'),min_drives)
    destinationcsv_str = os.path.join(r'..\..\..\..\Data\Intermediate\central_tendency',destinationtable_str+dt.date.today().strftime('_%Y%b%d')+'.csv')
    qry_str = ("""
    with cte0 as 
    (
    --calc medians over segment and before/after timeperiod
    select distinct a.segid, ulez_ineffect, function, inulez, inpolygon
    , count(value_median) over (partition by a.segid, ulez_ineffect) as pass_count
    , percentile_cont(value_median,0.5) over (partition by a.segid, ulez_ineffect) as median
    from 
        --determine whether segment inside/outside ulez and ulez period in/not-in effect
        (select a.segid, value_median, if(hour1stamp<'2019-04-08',0,1) as ulez_ineffect, function
        ,ST_WITHIN(geowkt, ST_GEOGFROMTEXT(WKTgeom)) as inulez
        from UK.snap4_drivepass_1hr_medians a
        join UK.greaterlondon_roads_wgs84_split30m b
        on a.segid = b.segid
        , (select WKTgeom from UK.lcczone where BOUNDARY = 'CSS Area 1')
        where parameter_id = {0}
         ) a
    join 
         --determine whether segment inside/outside polygon boundaries
         (select a.segid, LOGICAL_OR(ST_WITHIN(a.geowkt,b.geowkt)) as inpolygon 
         FROM UK.greaterlondon_roads_wgs84_split30m a,
         UK.london_drive_polygons b
         group by segid
         ) b
    on a.segid = b.segid
    )
    --join before and after stats for segments with minimum number of drives
    select a.segid, a.function, a.inulez, a.inpolygon, before_count, before_median, after_count, after_median
    from
        (select segid, function, inulez, inpolygon, pass_count as before_count, median as before_median from cte0
        where ulez_ineffect = 0 and pass_count >= {1} 
        ) a
    join
        (select segid, pass_count as after_count, median as after_median from cte0
        where ulez_ineffect = 1 and pass_count >= {1}
        ) b
    on a.segid=b.segid
            """.format(pid,min_drives))
    qry_job = client.query(qry_str,location=location,job_config=bigquery.QueryJobConfig())
    df = qry_job.to_dataframe()
    df.to_csv(destinationcsv_str,index=False)
    print("Query results saved as csv {0}".format(destinationcsv_str))
    df = pd.read_csv(destinationcsv_str)

    #boxplots
    plt.rcParams.update({'font.size':14})
    fig,axes = plt.subplots(2,2,figsize=(12,12))
    plot_list = [{'function':'A Road','inulez': False, 'title': 'A Roads Non-ULEZ'},\
            {'function': 'A Road', 'inulez': True, 'title': 'A Roads ULEZ'},\
            {'function': 'Local Road', 'inulez': False, 'title': 'Local Roads Non-ULEZ'},\
            {'function': 'Local Road', 'inulez': True, 'title': 'Local Roads ULEZ'}\
            ]
    ymax = df['before_median'].max()
    for i, p in enumerate(plot_list):
        ax = axes.flat[i]
        if inpolygon:
            dftmp=df[(df['function']==p['function']) & (df['inulez']==p['inulez']) & (df['inpolygon']==True)]
        else: 
            dftmp=df[(df['function']==p['function']) & (df['inulez']==p['inulez'])]
        n_seg = len(dftmp['segid'])
        label_list = ['Before\nn={0}'.format(n_seg),'After\nn={0}'.format(n_seg)]
        ax.boxplot([dftmp['before_median'],dftmp['after_median']], labels=label_list, showfliers=False,whis=[5,95])
        #,notch=True,bootstrap=5000)
        before_mean = np.mean(np.array(dftmp['before_median']))
        after_mean = np.mean(np.array(dftmp['after_median']))
        before_error = np.sqrt(np.sum((np.array([efunc_before(n) for n in dftmp['before_count']])*before_mean)**2))/n_seg
        after_error = np.sqrt(np.sum((np.array([efunc_after(n) for n in dftmp['after_count']])*after_mean)**2))/n_seg
        ax.plot([1,2],[before_mean,after_mean],'.',label='mean \nbefore: {0:.1f}+/-{1:.1f};\n  after: {2:.1f}+/-{3:.1f}'.format(before_mean,before_error,after_mean,after_error))
        ax.set_ylim(0,ymax)
        ax.set_ylabel('{0} ({1})'.format(species,unit))
        ax.legend(loc='lower left')
        ax.title.set_text(p['title'])

    #plt.show()
    fig.tight_layout()
    fig.savefig(r'..\charts\ulezeval_{0}_{1}drivesb4andafter_inpoly{3}_{2}.png'.format(species.replace(' ','_'),min_drives,dt.date.today().strftime('%Y%b%d'),inpolygon))


