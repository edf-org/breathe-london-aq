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

import glob
import os
import sys
import numpy as np
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt

####! mean
#central_stat = 'mean'
####! median
central_stat = 'median'

full_yr_247 = '{0}_no2_ugm3_Apr8fullyear'.format(central_stat)
part_yr_dywk = '{0}_no2_ugm3_Apr8_Oct31_weekdayonly_from5amto8pm'.format(central_stat)

dfin = pd.read_csv(r'..\..\..\..\Data\Intermediate\central_tendency\Dan\AQMesh_LAQN_pod_median_update_lp.csv')
df = dfin[dfin['Type']!='Urban Background']
#error plots
plt.rcParams.update({'font.size':12})
fig,ax = plt.subplots(1,1,figsize=(10,5))

yerror = (df[full_yr_247]-df[part_yr_dywk])/df[part_yr_dywk]*100
ax.errorbar(range(len(df['pod_id_location'])),[0]*len(df['pod_id_location']),yerr=[[0]*len(df['pod_id_location']),yerror],fmt='.k', ecolor='black',elinewidth=1)
ax.plot([0,len(df['pod_id_location'])],[yerror.mean(), yerror.mean()],'k--')
print('mean error:',yerror.mean(),'min:',yerror.min(),'max:',yerror.max(),'#sites:',len(yerror))
ax.set_ylabel('% errror')
ax.title.set_text('Error as [X(24/7/annual) - X(day/M-F/part-yr)]/X(day/M-F/part-yr)*100%')
ax.set_xlabel('Stationary Site')
ax.set_xticks(range(len(df['pod_id_location'])))
ax.set_xticklabels(df['pod_id_location'],fontdict={'fontsize':8,'rotation':'vertical'})

#ax.title.set_text(plot_list[i]['title'])
fig.tight_layout()
fig.savefig(r'..\charts\daytimeweekdaypartyr_bias_{0}_{1}.png'.format(central_stat,dt.date.today().strftime('%Y%b%d')))

