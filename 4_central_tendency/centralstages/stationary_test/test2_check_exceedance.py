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


df = pd.read_csv(r"..\..\..\..\..\Data\Intermediate\central_tendency\stationary_test\test1_exceeds_WHO_confidence_post_ulez_2020Apr08.csv")
#df = pd.read_csv(r"..\..\..\..\..\Data\Intermediate\central_tendency\stationary_test\test1_exceeds_WHO_confidence_no_daytimeweekdaycorr_2020Apr09.csv")

#round confidence to thirds
df['conf_bin'] = np.ceil(df['confidence']/0.33)*0.33 #upper bound
dfagg = df.groupby(['conf_bin','segid']).agg({'actually_exceeded_bool':['sum','count']}).reset_index()
print(dfagg.head())
dfagg['actual_fraction']=dfagg[('actually_exceeded_bool','sum')]/dfagg[('actually_exceeded_bool','count')]
print(dfagg.head())
#dfagg.to_csv(r'..\..\..\..\..\Data\Intermediate\central_tendency\stationary_test\test2_check_exceedance_2020Apr08.csv')
#dfagg.to_csv(r'..\..\..\..\..\Data\Intermediate\central_tendency\stationary_test\test2_check_exceedance_no_daytimeweekdaycorr_2020Apr09.csv')
plt.rcParams.update({'font.size':14})
bplt_list = []
fig,ax = plt.subplots(1,1,figsize=(8,6))
label_list = ['<33%','33-66%','>66%']
#plot distribution of actual fraction of pods exceeding threshold for each target probability
for c in [.33,.66,.99]:
    bplt_list.append(dfagg[dfagg['conf_bin']==c]['actual_fraction']*100)
ax.boxplot(bplt_list,showfliers=False,whis=[5,95])
ax.plot([-1,4],[33, 33],'--',color='dimgray')
ax.plot([-1,4],[66, 66],'--',color='dimgray')

ax.set_ylabel('Sites that actually exceeded (%)')
ax.set_xlabel('Predicted exceedance probability')
ax.set_xticklabels(label_list)
ax.tick_params(axis='x',labelsize=10,labelrotation=0)

fig.savefig(r'.\charts\test2_check_exceedance_{0}.png'.format(dt.date.today().strftime('%Y%b%d')))
#fig.savefig(r'.\charts\test2_check_exceedance_no_daytimeweekdaycorr_{0}.png'.format(dt.date.today().strftime('%Y%b%d')))
