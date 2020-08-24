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


df = pd.read_csv(r"C:\Users\lpadilla\Documents\London\Data\Intermediate\central_tendency\central_checkuniformity_2020Mar20.csv")
df_trial = pd.read_csv(r"C:\Users\lpadilla\Documents\London\Data\Intermediate\central_tendency\central3di_checktrialuniformity_2020Mar20.csv")

plt.rcParams.update({'font.size':14})
bplt_list = []
bplt_trial_list = []
label_list = ['T\nn']
fig,ax = plt.subplots(1,1,figsize=(8,6))
#plot actual drive shifts
for n in range(5,41):
    bplt_list.append(df[df['n']==n]['actual_period_d'])
    label_list.append('{0:.0f}\n{1:.0f}'.format(df[df['n']==n]['uniform_period_d'].iloc[0],n))
ax.boxplot(bplt_list,positions=np.array(range(len(bplt_list)))*2.0-0.4,widths=0.6, showfliers=False,whis=[5,95])
#plot random trial drive shifts
for n in df_trial.sort_values(by=['n'])['n'].unique():
    bplt_trial_list.append(df_trial[df_trial['n']==n]['actual_period_d'])
bpt = ax.boxplot(bplt_trial_list,positions=np.array([0,10,20,30,40,50,60])+0.4,widths=0.6, showfliers=False,whis=[5,95])
plt.setp(bpt['boxes'],color='#2C7BB6')
plt.setp(bpt['whiskers'],color='#2C7BB6')
plt.setp(bpt['caps'],color='#2C7BB6')

#legend
plt.plot([], c='black', label='Actual')
plt.plot([], c='#2C7BB6', label='Random trials')
plt.legend()

ax.set_ylabel('Days between drives')
ax.set_xticks(range(-2,len(label_list)*2-2,2))
ax.set_xticklabels(label_list)
ax.tick_params(axis='x',labelsize=10,labelrotation=20)
ax.set_xlim(-2,len(label_list)*2-2)

fig.savefig(r'..\charts\central3dii_compareuniformity_{0}.png'.format(dt.date.today().strftime('%Y%b%d')))
