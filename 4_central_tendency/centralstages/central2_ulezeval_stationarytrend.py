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

#Plots monthly and annual trend over many years for AURN sites. AURN network has longest record

#sites
#in ulez are: CLL2, HORS
#MY1 on ulez boundary
sites = {'MY1':{'name':'London Marlybone Road','yrs':list(range(1999,2020))},\
        #'REA1':{'name':'Reading New Town','yrs': list(range(2003,2020))},\
        #'TED2':{'name': 'London Teddington Bushy Park','yrs': list(range(2013,2020))},\
        'CLL2':{'name': 'London Bloomsbury','yrs': list(range(1992,2020))},\
        'HORS':{'name':'London Westminster','yrs': list(range(2001,2020))},\
        'SK5':{'name':'Southwark A2 Old Kent Road','yrs': list(range(2011,2020))},\
        'KC1':{'name':'London N. Kensington','yrs': list(range(1996,2020))},\
        'TH2':{'name':'Tower Hamlets Roadside','yrs': list(range(1996,2020))},\
        'CA1':{'name':'Camden Kerbside','yrs': list(range(1996,2020))}
        }
#species = 'Nitrogen dioxide'
species = 'Nitrogen oxides as nitrogen dioxide'
calc_enhancement = False
unit = 'ug/m3'
plt.rcParams.update({'font.size':14})
if calc_enhancement:
    fig,axes = plt.subplots(2,2,figsize=(12,12))
else:
    fig,axes = plt.subplots(1,2,figsize=(12,6))
fig2,axes2 = plt.subplots(1,1,figsize=(8,6))
#for yr in [2019]:
for site, sitedata in sites.items():
    print(site)
    yrx = []
    yravg = []
    for yr in sitedata['yrs']:
        print(yr)
        csv_str = r'..\..\..\Data\Source\AURN\{0}_{1}.csv'.format(site,yr)
        #Read each year of data
        dateparse = lambda x: pd.datetime.strptime(x,'%d-%m-%Y')
        df = pd.read_csv(csv_str,parse_dates=['Date'],skiprows = [0,1,2,3,5], date_parser=dateparse)
        #print(df[df['Date']>'2019-11-15'])
        #print(df.dtypes)
        #print(df[0:3])
        #change 24:00 to 23:59
        df['correctedtime'] = df['time'].replace('24:00','23:59')
        #combined timestamp field
        df['timestamp'] = df.apply(lambda x: pd.datetime.combine(x['Date'],pd.datetime.strptime(x['correctedtime'],'%H:%M').time()),1)
        #background and enhancement
        #df['background'] = df.rolling('24H',closed='neither',min_periods=24,on='timestamp')[species].quantile(0.05)
        ##!!!need to change this to a normalization, not a good background method for removing seasonality!!!
        df['background'] = df.rolling('48H',closed='neither',min_periods=24,on='timestamp')[species].min()
        #df['enhancement'] = df.apply(lambda x: max(0,x[species]-x['background']))
        df['enhancement'] = df[species]-df['background']
        df.loc[df['enhancement']<0,'enhancement']=0 #don't allow negative numbers
        #daily average
        df_dayavg = df.groupby('Date').mean().reset_index()
        #print(df_dayavg[df['Date']>'2019-11-15'])
        #add monthly column then monthly average
        df_dayavg['month'] = df_dayavg['Date'].dt.strftime('%m')
        df_monavg = df_dayavg.groupby('month').mean().reset_index()
        #print(df_dayavg[df_dayavg['month']=='Dec'][0:5])
        print(df_monavg[['month',species,'background','enhancement']])
        yrx.append(yr)
        yravg.append(df_monavg[species].mean())
        #Plot monthly average
        try:
            ax = axes.flat[0]
            ax.plot(df_monavg['month'],df_monavg[species],'.',label='{0}'.format(yr))
            ax.set_ylabel('Monthly average {0} ({1})'.format(species,unit))
            ax.title.set_text(sitedata['name'])
            
            ax = axes.flat[1]
            ax.plot([dt.datetime(yr,int(m),1) for m in df_monavg['month']],df_monavg[species])
            ax.set_ylabel('Monthly average {0} ({1})'.format(species,unit))
            ax.title.set_text(sitedata['name'])
            
            if calc_enhancement:
                ax = axes.flat[2]
                ax.plot(df_monavg['month'],df_monavg['enhancement'],'.',label='{0}'.format(yr))
                ax.title.set_text('Enhancements')
                ax.set_ylabel('Monthly average {0} ({1})'.format(species,unit))
                
                ax = axes.flat[3]
                ax.plot([dt.datetime(yr,int(m),1) for m in df_monavg['month']],df_monavg['enhancement'])
        except KeyError:
            print('Species: {0} missing in year: {1}'.format(species,yr))
            continue
        #ax.set_ylim(ymin,ymax)
        #ax.set_xlim(ymin,ymax)
        #ax.axis('equal')
        #ax.set_xlabel('{0} ({1}) Before ULEZ'.format(species,unit))
    axes2.plot(yrx,yravg,label=sitedata['name'])
    axes2.set_xlim(1990,2020)
#    axes2.set_ylim(0,140)
    axes2.set_ylabel('Annual average {0} ({1})'.format(species,unit))
#fig.tight_layout()
    if calc_enhancement:
        fig.savefig(r'.\charts\ulezeval_AURN_monthly_{0}_{1}_wenh.png'.format(species.replace(' ','_'),site))
    else:
        fig.savefig(r'.\charts\ulezeval_AURN_monthly_{0}_{1}.png'.format(species.replace(' ','_'),site))
#axes2.legend(loc='upper left',bbox_to_anchor=(0,-0.4,1,.4),ncol=2)
axes2.legend(loc='best',ncol=1, prop={'size':10})
fig2.savefig(r'.\charts\ulezeval_AURN_yearly_{0}.png'.format(species.replace(' ','_')))
