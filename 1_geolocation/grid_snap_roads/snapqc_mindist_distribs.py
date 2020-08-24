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

import matplotlib.pyplot as plt
import matplotlib.dates as md
import pandas as pd
import datetime as dt
import numpy as np
import sys
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()
#df = pd.read_csv(fpath,parse_dates=['timestamp'])
df=pd.read_csv(r"..\..\..\..\Data\Intermediate\geolocation\testmindistance\bq-results-20200624-211526-d9nmi4juqpt.csv")
period_list = df.sort_values(by=['dt'])['dt'].unique()
#plots
n_row = 3
n_col = 3
n_plot = n_col*n_row
n_period = len(period_list)
#loop making new figures
for m in range(int(np.ceil(n_period/n_plot))): 
    print(m)
    plt.rcParams.update({'font.size':14})
    fig,axes = plt.subplots(n_row,n_col,figsize=(18,12))
    ax_list = axes.flatten()
    #loop making new charts
    for i,p in enumerate(period_list[n_plot*m:min(n_plot*(m+1),len(period_list))]):
        ax = ax_list[i]
        df_tmp1 = df[(df['dt']==p) & (df['vid']==27522)]
        df_tmp2 = df[(df['dt']==p) & (df['vid']==27533)]
        #first plot is vid 1
        ax.plot(df_tmp1['mindist_int_m'],df_tmp1['pt_frac'],'b',label='27522')
        #second plot is vid 2
        ax.plot(df_tmp2['mindist_int_m'],df_tmp2['pt_frac'],'r',label='27533')
        ax.set_title(p)
    ax.legend()
    fig.tight_layout()
    fig.savefig(r'..\..\..\..\Data\Intermediate\geolocation\testmindistance\snapqc_mindist_distribs{1}_{0}.png'.format(dt.datetime.today().strftime('%Y%b%d'),m), dpi=300)
    plt.close(fig)
