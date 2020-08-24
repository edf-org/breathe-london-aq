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

#daytime/weekday medians were ~ 4.27% greater than 24/7 medians as a result of diurnal cycle of NO2
daytime_weekday_correction = 1/1.043 #see temporal_bias\NO2_daytime_weekday_bias.xlsx 

def get_distrib_confidence(thresh, df, n, med, corr=1):
    distrib = df[df['n_sub']==n]
    pct_diff = np.array([distrib['p{0:02}t'.format(a)].iloc[0] for a in range(1,100)])
    x = (pct_diff/100+1)*med*corr #apply daytime weekday correction if needed
    #find exceedance percentile of x closest to threhold
    y = 1-np.array(range(1,100))/100
    c = np.interp(thresh,x,y)
    return x,y,c

#load the sampling uncertainty distributions
dfuncert = pd.read_csv(r'..\..\..\..\Data\Intermediate\central_tendency\central2_ulezeval_NO2_after_sampleuncertainty_2020Mar20.csv')
#choose parameters for illustration
n = 10
med = 50
tolerance = [.333,.05]
threshold = [40,60] #ug/m3 
print(n,med)
#illustration of varying threshold and uncertainty tolerance
plt.rcParams.update({'font.size':12})
fig,axes = plt.subplots(2,2,figsize=(8,5))
for i,tol in enumerate(tolerance):
    for j,thr in enumerate(threshold):
        x,y,c = get_distrib_confidence(thr,dfuncert,n,med,daytime_weekday_correction)
        ax = axes[i,j]
        ax.fill([0,140,140,0],[tol,tol,0,0],'deepskyblue',[0,140,140,0],[1-tol,1-tol,1,1],'r',\
                [0,140,140,0],[tol,tol,1-tol,1-tol],'gray', alpha = 0.25)
        ax.plot(x,y,'k',linewidth=2)
        ax.plot([thr,thr],[0,c],color='gray',linestyle='--')
        ax.plot([0,thr],[c,c],color='gray',linestyle='--')
        ax.set_xlim([0,140])
        ax.xaxis.set_ticks(range(0,150,20))
        ax.set_ylim([0,1])
        if j!=0:
            #clear y tick labels
            ax.tick_params(labelleft=False)
        if i==0:
            ax.tick_params(labelbottom=False)
            ax.title.set_text('Threshold = {0}'.format(thr))
        if i!=0:
            ax.set_xlabel('Concentration')
        if j==0:
            ax.set_ylabel('Uncertainty = {0}%\nExceedance probability'.format(int(tol*100)))


        
fig.tight_layout()
fig.savefig(r'..\charts\exceedance_conceptual_illustration_{0}.png'.format(dt.date.today().strftime('%Y%b%d')))

#key
plt.rcParams.update({'font.size':12})
fig,ax = plt.subplots(1,1,figsize=(3,2))
tol = .33
ax.fill([0,140,140,0],[tol,tol,0,0],'deepskyblue',[0,140,140,0],[1-tol,1-tol,1,1],'r',\
        [0,140,140,0],[tol,tol,1-tol,1-tol],'gray', alpha = 0.25)
ax.text(20,0.8,'likely to exceed',fontsize=12)
ax.text(20,0.5,'indeterminate',fontsize=12)
ax.text(20,0.2,'unlikely to exceed',fontsize=12)
ax.tick_params(labelleft=False)
ax.tick_params(labelbottom=False)
fig.tight_layout()
fig.savefig(r'..\charts\exceedance_conceptual_key_{0}.png'.format(dt.date.today().strftime('%Y%b%d')))

f = 1-.0756
median_list = [{'median':45,'label':'Uncorrected','color':'red','ls':'--'},{'median':45*f,'label':'Corrected','color':'k','ls':'--'}]
thr = 40
tol = 0.333
plt.rcParams.update({'font.size':12})
fig,ax = plt.subplots(1,1,figsize=(5,4))
#illustration of bias correction
for i,med in enumerate(median_list):
    x,y,c = get_distrib_confidence(thr,dfuncert,n,med['median'],daytime_weekday_correction)
    ax.fill([0,140,140,0],[tol,tol,0,0],'deepskyblue',[0,140,140,0],[1-tol,1-tol,1,1],'r',\
            [0,140,140,0],[tol,tol,1-tol,1-tol],'gray', alpha = 0.25)
    ax.plot(x,y,'k',linewidth=2,label=med['label'],color=med['color'],linestyle=med['ls'])
    ax.plot([thr,thr],[0,c],color='gray',linestyle='--')
    ax.plot([0,thr],[c,c],color='gray',linestyle='--')
    ax.set_xlim([0,140])
    ax.xaxis.set_ticks(range(0,150,20))
    ax.set_ylim([0,1])
    ax.set_xlabel('Concentration')
    ax.set_ylabel('Exceedance probability')
    ax.legend()
fig.tight_layout()
fig.savefig(r'..\charts\exceedance_conceptual_bias_correction_{0}.png'.format(dt.date.today().strftime('%Y%b%d')))
