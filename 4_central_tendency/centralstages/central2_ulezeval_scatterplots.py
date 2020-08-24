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

min_drives = 10
inpolygon = True
plot_list = [{'function':'A Road','inulez':False,'title':'A Roads Non-ULEZ'},\
             {'function':'A Road','inulez':True,'title':'A Roads ULEZ'},\
             {'function':'Local Road','inulez':False,'title':'Local Roads Non-ULEZ'},\
             {'function':'Local Road','inulez':True,'title':'Local Roads ULEZ'}]
for species in ['NO2', 'NOx', 'PM2.5 PDR','CO2 dry','O3']:
    ymin = 0
    if species == 'NO2':
        pid = 133
        unit = 'ppb'
        efuncx = lambda n : {5:0.8,10:0.6,15:0.45,20:0.4,25:0.375,30:0.35,35:0.3}[min(round(n/5)*5,35)]
        efuncy = lambda n : {5:0.7,10:0.5,15:0.4,20:0.35,25:0.3,30:0.25,35:0.2}[min(round(n/5)*5,35)]
    elif species == 'NOx':
        pid = 163
        unit = 'ug/m3'
    elif species == 'PM2.5 PDR':
        pid = 140
        unit = 'ug/m3'
    elif species == 'CO2 dry':
        pid = 33
        unit = 'ppm'
        ymin = 400
    elif species == 'O3':
        pid = 127
        unit = 'ppb'
    ###############################################################
    #central2 Evaluate segment medians before and after ULEZ in-effect
    #include only segments with min n drives in BOTH periods
    #separate by road function and inside-ulez vs outside-ulez
    print(species)
    destinationtable_str = 'central2_ulezeval_{0}_{1}drivesb4andafter'.format(species.replace(' ','_'),min_drives)
    destinationcsv_str = os.path.join(r'..\..\..\..\Data\Intermediate\central_tendency',destinationtable_str+'_2020Mar16.csv')
    df = pd.read_csv(destinationcsv_str)
    ymax = df['before_median'].max()
    print(ymin)
    #scatterplots
    #medians after ULEZ in effect vs medians before
    plt.rcParams.update({'font.size':14})
    fig,axes = plt.subplots(2,2,figsize=(12,12))
    for i,ax in enumerate(axes.flat):
        if inpolygon:
            dftmp = df[(df['function']==plot_list[i]['function']) & (df['inulez']==plot_list[i]['inulez']) & (df['inpolygon']==True)]
        else:
            dftmp = df[(df['function']==plot_list[i]['function']) & (df['inulez']==plot_list[i]['inulez'])]
        
        nseg = len(dftmp['before_median'])
        xerror = np.array([efuncx(npass) for npass in dftmp['before_count']])*np.array(dftmp['before_median'])
        yerror = np.array([efuncy(npass) for npass in dftmp['after_count']])*np.array(dftmp['after_median'])
        ax.errorbar(dftmp['before_median'],dftmp['after_median'],xerr=xerror,yerr=yerror,fmt='.k', ecolor='lightgray',elinewidth=1,label='segment median ({0} segments)'.format(nseg))

        before_mean = np.mean(np.array(dftmp['before_median']))
        after_mean = np.mean(np.array(dftmp['after_median']))
        ax.plot([before_mean],[after_mean],'.',label='mean of segment medians ({0:.2f} ratio)'.format(after_mean/before_mean))
        ax.plot([ymin,ymax],[ymin,ymax],'k',label='1:1 line') #1:1 line

        ax.set_ylim(ymin,ymax)
        ax.set_xlim(ymin,ymax)
        ax.axis('equal')
        ax.set_ylabel('{0} ({1}) After ULEZ'.format(species,unit))
        ax.set_xlabel('{0} ({1}) Before ULEZ'.format(species,unit))
        ax.legend(loc='upper left')
        ax.title.set_text(plot_list[i]['title'])
    fig.tight_layout()
    fig.savefig(r'..\charts\ulezeval_{0}_scatter_{1}drivesb4andafter_inpoly{2}_{3}.png'.format(species.replace(' ','_'),min_drives,inpolygon,dt.date.today().strftime('%Y%b%d')))

    #ratio of before/after medians vs count
    plt.rcParams.update({'font.size':14})
    fig,axes = plt.subplots(2,2,figsize=(12,12))
    for i,ax in enumerate(axes.flat):
        if inpolygon:
            dftmp = df[(df['function']==plot_list[i]['function']) & (df['inulez']==plot_list[i]['inulez']) & (df['inulez']==True)]
        else:
            dftmp = df[(df['function']==plot_list[i]['function']) & (df['inulez']==plot_list[i]['inulez'])]
        ratio = dftmp['after_median']/dftmp['before_median']
        cscale = lambda r: 0.25 if r <=1 else 0.75 
        scaled_ratio = [cscale(r) for r in ratio]
        colors = plt.cm.coolwarm(scaled_ratio)
        ax.scatter(dftmp['before_count'],dftmp['after_count'],marker='.',c=scaled_ratio, cmap=plt.cm.coolwarm)
        
        ax.axis('equal')
        ax.set_ylim(0,100)
        ax.set_xlim(0,100)
        ax.set_xlabel('{0} period count before ULEZ'.format(species))
        ax.set_ylabel('{0} period count after ULEZ'.format(species))
        ax.title.set_text(plot_list[i]['title'])
    fig.tight_layout()
    fig.savefig(r'..\charts\ulezeval_{0}_count_{1}drivesb4andafter_inpoly{2}_{3}.png'.format(species.replace(' ','_'),min_drives,inpolygon,dt.date.today().strftime('%Y%b%d')))


