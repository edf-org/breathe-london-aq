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
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()

credentials = service_account.Credentials.from_service_account_file(r"..\..\..\pks\street-view-air-quality-london-dc8f329b26cf.json")
project_id = 'street-view-air-quality-london'
client = bigquery.Client(credentials=credentials,project=project_id)

#Plots monthly trend over many years for mobile compared to AURN sites. AURN network has longest record

#true for all queries in this process
location = 'EU'
min_drives = 10
pid = 133 #ppb
unit = 'ppb'
species = 'Nitrogen dioxide' #stationary unit = 'ug/m3'
model_param = 'Conc_ug_m3_NO2_All_sources_1hr*0.52' #model unit = ug/m3 so convert to ppb
inpolygon = True

#sites
#in ulez are: CLL2, HORS
#MY1 on ulez boundary
sites = {'MY1':{'name':'London Marlybone Road','yrs':list(range(1999,2019))},\
        'REA1':{'name':'Reading New Town','yrs': list(range(2003,2019))},\
        'TED2':{'name': 'London Teddington Bushy Park','yrs': list(range(2013,2019))},\
        'CLL2':{'name': 'London Bloomsbury','yrs': list(range(1992,2019))},\
        'HORS':{'name':'London Westminster','yrs': list(range(2001,2019))},\
        'SK5':{'name':'Southwark A2 Old Kent Road','yrs': list(range(2011,2019))},\
        'KC1':{'name':'London N. Kensington','yrs': list(range(1996,2019))},\
        'TH2':{'name':'Tower Hamlets Roadside','yrs': list(range(1996,2019))},\
        'CA1':{'name':'Camden Kerbside','yrs': list(range(1996,2019))},\
        }
site = 'KC1'
allyr_mon_arr = []
allyr_hour_arr = []

#get stationary data, long-term average
for yr in sites[site]['yrs']:
    print(yr)
    csv_str = r'..\..\..\..\Data\Source\AURN\{0}_{1}.csv'.format(site,yr)
    #Read each year of data
    dateparse = lambda x: pd.datetime.strptime(x,'%d-%m-%Y')
    df = pd.read_csv(csv_str,parse_dates=['Date'],skiprows = [0,1,2,3,5], date_parser=dateparse)
    #change 24:00 to 23:59
    df['correctedtime'] = df['time'].replace('24:00','23:59')
    #combined timestamp field
    df['timestamp'] = df.apply(lambda x: pd.datetime.combine(x['Date'],pd.datetime.strptime(x['correctedtime'],'%H:%M').time()),1)
    #daily average
    df_dayavg = df.groupby('Date').mean().reset_index()
    #print(df_dayavg[df['Date']>'2019-11-15'])
    #add monthly column then monthly average
    df_dayavg['month'] = df_dayavg['Date'].dt.strftime('%m')
    df_monavg = df_dayavg.groupby('month').mean().reset_index().sort_values('month')
    #add hour column then hourly average
    df['hour'] = df['timestamp'].dt.strftime('%H')
    df_houravg = df.groupby('hour').mean().reset_index().sort_values('hour')
    allyr_mon_arr.append(df_monavg[species])
    allyr_hour_arr.append(df_houravg[species])

avg_mon_ppb_arr = 0.52*np.nanmean(allyr_mon_arr,0) #average all years and convert to ppb
avg_hour_ppb_arr = 0.52*np.nanmean(allyr_hour_arr,0) #average all years and convert to ppb
#get the mobile and model data
qry_str = ("""
with cte0 as
(
--calc medians over segment and before/after timeperiod
select a.segid, a.hour1stamp, value_median, value_model, ulez_ineffect, function, inulez, inpolygon, ST_CENTROID(geowkt) as centroid
, count(value_median) over (partition by a.segid, ulez_ineffect) as pass_count
from
    --determine whether segment inside/outside ulez and ulez period in/not-in effect
    (select a.segid, hour1stamp, value_median, if(hour1stamp<'2019-04-08',0,1) as ulez_ineffect, function
    ,ST_WITHIN(geowkt, ST_GEOGFROMTEXT(WKTgeom)) as inulez
    , geowkt
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
left join
--join
    --CERC model data, average over multiple receptors per segment
    (select a.segid
    , DATETIME_ADD(DATETIME_ADD(DATETIME(b.Year,1,1,0,0,0),INTERVAL b.Hour HOUR), INTERVAL b.Day-1 DAY) as hour1stamp
    , avg(b.{1}) as value_model 
    from UK.cerc_receptor_segment_lookup a 
    left join UK.cerc_model_data_mobile_receptors b
    on a.Receptor = replace(b.Receptor_name, ' ','')
    group by segid, hour1stamp
    ) c
on a.segid = c.segid and a.hour1stamp = c.hour1stamp
)
select segid, hour1stamp, EXTRACT(month from hour1stamp) as mon,
function, inulez, inpolygon, ulez_ineffect, pass_count, value_median, value_model, 
ST_X(centroid) as longitude, ST_Y(centroid) as latitude
from cte0
where (segid in (select segid from cte0 where ulez_ineffect = 0 and pass_count >= {2}))
and (segid in (select segid from cte0 where ulez_ineffect = 1 and pass_count >= {2}))
order by hour1stamp
        """.format(pid,model_param,min_drives))
qry_job = client.query(qry_str,location=location,job_config=bigquery.QueryJobConfig())
df = qry_job.to_dataframe()
destinationtable_str = 'central2_ulezeval_mobile_seasonality_{0}_{1}drivesb4andafter'.format(species.replace(' ','_'),min_drives)
destinationcsv_str = os.path.join(r'..\..\..\..\Data\Intermediate\central_tendency',destinationtable_str+dt.date.today().strftime('_%Y%b%d')+'.csv')
#destinationcsv_str = os.path.join(r'..\..\..\Data\Intermediate\central_tendency',destinationtable_str+'_20Jan27.csv')
df.to_csv(destinationcsv_str,index=False)
print("Query results saved as csv {0}".format(destinationcsv_str))
#read saved results if already executed
df = pd.read_csv(destinationcsv_str,parse_dates=['hour1stamp'])

#save just the distinct road segments without the individual drive period info
destinationcsv_str2 = os.path.join(r'..\..\..\..\Data\Intermediate\central_tendency',destinationtable_str+'_segments'+dt.date.today().strftime('_%Y%b%d')+'.csv')
df.groupby('segid').first().reset_index()[['segid','function','inulez','inpolygon','longitude','latitude']].to_csv(destinationcsv_str2,index=False)

#plot
plot_list = [{'function':'A Road','inulez': False, 'title': 'A Roads Non-ULEZ'},\
        {'function': 'A Road', 'inulez': True, 'title': 'A Roads ULEZ'}\
#        {'function': 'Local Road', 'inulez': False, 'title': 'Local Roads Non-ULEZ'},\
#        {'function': 'Local Road', 'inulez': True, 'title': 'Local Roads ULEZ'}\
        ]
#ymax = df['value_median'].max()
ymax = 100
plt.rcParams.update({'font.size':14})
#fig,axes = plt.subplots(2,2,figsize=(12,12))
#fig2,axes2 = plt.subplots(2,2,figsize=(12,12))
fig,axes = plt.subplots(1,2,figsize=(12,6))
fig2,axes2 = plt.subplots(1,2,figsize=(12,6))
for i, p in enumerate(plot_list):
    ax = axes.flat[i]
    ax2 = axes2.flat[i]
    if inpolygon:
        dftmp=df[(df['function']==p['function']) & (df['inulez']==p['inulez']) & (df['inpolygon']==True) & (df['hour1stamp']>='2018-09-01')].sort_values('hour1stamp')
    else: 
        dftmp=df[(df['function']==p['function']) & (df['inulez']==p['inulez']) & (df['hour1stamp']>='2018-09-01')].sort_values('hour1stamp')
    #monthly summary
    dftmp['date'] = [dt.datetime(b.year,b.month,1) for b in dftmp['hour1stamp']]
    dftmp_monavg = dftmp.groupby('date').mean().reset_index()
    dftmp_monmed = dftmp.groupby('date').median().reset_index()
    dftmp_mon05 = dftmp.groupby('date').quantile(0.05).reset_index()
    dftmp_mon95 = dftmp.groupby('date').quantile(0.95).reset_index()
    dftmp_monct = dftmp.groupby('date').count().reset_index()
    #plot mobile monthly median of drive period medians
    #ax.plot(dftmp['date'],dftmp['value_median'],'.',markersize=1)
    ax.fill_between(dftmp_mon05['date'],dftmp_mon05['value_median'],dftmp_mon95['value_median'],facecolor='lightgray',alpha=0.5)
    #ax.plot(dftmp_monavg['date'],dftmp_monavg['value_median'],'r-')
    ax.plot(dftmp_monmed['date'],dftmp_monmed['value_median'],'darkgray',label='meas')
    mon_list = [dt.datetime(2018,a,1) for a in range(1,13)]+[dt.datetime(2019,a,1) for a in range(1,13)]
    for v,tick in enumerate(dftmp_monct['date']):
        ax.text(tick,0.95*ymax,'{0}'.format(dftmp_monct['value_median'][v]),rotation=45,fontsize=10)
    #plot model monthly median of receptor means in mobile drive periods
    ax.fill_between(dftmp_mon05['date'],dftmp_mon05['value_model'],dftmp_mon95['value_model'],facecolor='lightblue',alpha=0.5)
    #ax.plot(dftmp_monavg['date'],dftmp_monavg['value_model'],'b-')
    ax.plot(dftmp_monmed['date'],dftmp_monmed['value_model'],'b',label='model')

    #plot stationary long-term trend, repeat in  2018 and 2019
    ax.plot(mon_list,np.hstack((avg_mon_ppb_arr,avg_mon_ppb_arr)),'k',label=sites[site]['name'])
    ax.set_ylim(0,ymax)
    ax.set_ylabel('{0} ({1})'.format(species,unit))
    ax.legend(loc='lower left')
    ax.title.set_text(p['title'])
#ax.set_ylim(ymin,ymax)
    #ax.set_xlim(ymin,ymax)
    #ax.axis('equal')
    #ax.set_xlabel('{0} ({1}) Before ULEZ'.format(species,unit))
#axes[0,0].legend(loc='upper left',bbox_to_anchor=(-0.2,1,.2,1),ncol=1)
    
    #hourly summary
    dftmp['hour'] = [b.hour for b in dftmp['hour1stamp']]
    dftmp_houravg = dftmp.groupby('hour').mean().reset_index()
    dftmp_hourmed = dftmp.groupby('hour').median().reset_index()
    dftmp_hour05 = dftmp.groupby('hour').quantile(0.05).reset_index()
    dftmp_hour95 = dftmp.groupby('hour').quantile(0.95).reset_index()
    dftmp_hourct = dftmp.groupby('hour').count().reset_index()
    #plot mobile hourly median of drive period medians
    ax2.fill_between(dftmp_hour05['hour'],dftmp_hour05['value_median'],dftmp_hour95['value_median'],facecolor='lightgray',alpha=0.5)
    #ax2.plot(dftmp_houravg['hour'],dftmp_houravg['value_median'],'r-')
    ax2.plot(dftmp_hourmed['hour'],dftmp_hourmed['value_median'],'darkgray',label='meas')
    hour_list = range(0,23)
    for v,tick in enumerate(dftmp_hourct['hour']):
        ax2.text(tick,0.95*ymax,'{0}'.format(dftmp_hourct['value_median'][v]),rotation=45,fontsize=10)
    #plot model hourly median of receptor means in mobile drive periods
    ax2.fill_between(dftmp_hour05['hour'],dftmp_hour05['value_model'],dftmp_hour95['value_model'],facecolor='lightblue',alpha=0.5)
    #ax2.plot(dftmp_houravg['hour'],dftmp_houravg['value_model'],'b-')
    ax2.plot(dftmp_hourmed['hour'],dftmp_hourmed['value_model'],'b',label='model')
    #plot stationary long-term trend, repeat in  2018 and 2019
    ax2.plot(hour_list,avg_hour_ppb_arr,'k',label=sites[site]['name'])
    ax2.set_ylim(0,ymax)
    ax2.set_ylabel('{0} ({1})'.format(species,unit))
    ax2.legend(loc='lower left')
    ax2.title.set_text(p['title'])
fig.autofmt_xdate()
fig.tight_layout()
fig.savefig(r'..\charts\ulezeval_mobile_seasonality_{0}_{1}drivesb4andafter_inpoly{2}{3}.png'.format(species.replace(' ','_'),min_drives,inpolygon,dt.date.today().strftime('_%Y%b%d')))

fig2.autofmt_xdate()
fig2.tight_layout()
fig2.savefig(r'..\charts\ulezeval_mobile_diurnalcycle_{0}_{1}drivesb4andafter_inpoly{2}{3}.png'.format(species.replace(' ','_'),min_drives,inpolygon,dt.date.today().strftime('_%Y%b%d')))

#also plot all driveperiods
plt.rcParams.update({'font.size':14})
fig,axes = plt.subplots(1,1,figsize=(6,6))
dftmp = df[(df['hour1stamp']>='2018-09-01')].sort_values('hour1stamp')
dftmp['date'] = [dt.datetime(b.year,b.month,1) for b in dftmp['hour1stamp']]
#plot mobile drive period medians
ax = axes
#ax.plot(dftmp['date'],dftmp['value_median'],'.',markersize=1)
dftmp_monavg = dftmp.groupby('date').mean().reset_index()
dftmp_monmed = dftmp.groupby('date').median().reset_index()
dftmp_mon05 = dftmp.groupby('date').quantile(0.05).reset_index()
dftmp_mon95 = dftmp.groupby('date').quantile(0.95).reset_index()
dftmp_monct = dftmp.groupby('date').count().reset_index()
ax.fill_between(dftmp_mon05['date'],dftmp_mon05['value_median'],dftmp_mon95['value_median'],facecolor='lightgray',alpha=0.5)
ax.plot(dftmp_monavg['date'],dftmp_monavg['value_median'],'darkgray')
ax.fill_between(dftmp_mon05['date'],dftmp_mon05['value_model'],dftmp_mon95['value_model'],facecolor='lightblue',alpha=0.5)
ax.plot(dftmp_monmed['date'],dftmp_monmed['value_model'],'b')
ax.set_ylim(0,ymax)
ax.set_ylabel('{0} ({1})'.format(species,unit))
ax.title.set_text('All drive periods for segments\n with >= {0} drives before and after ULEZ'.format(min_drives))
fig.autofmt_xdate()
fig.tight_layout()
fig.savefig(r'..\charts\ulezeval_mobile_seasonality_onechart_{0}_{1}drivesb4andafter_inpoly{2}{3}.png'.format(species.replace(' ','_'),min_drives,inpolygon,dt.date.today().strftime('_%Y%b%d')))
