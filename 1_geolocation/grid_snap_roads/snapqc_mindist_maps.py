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

import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot as plt
import matplotlib.dates as md
import cartopy.crs as ccrs
from cartopy.io.img_tiles import OSM
import pandas as pd
import datetime as dt
import numpy as np
import sys
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()

#read drive days
#df = pd.read_csv(fpath,parse_dates=['timestamp'])
dfdates=pd.read_csv(r"..\..\..\..\Data\Intermediate\geolocation\testmindistance\bq-results-20200624-211526-d9nmi4juqpt.csv")
#param = 'satellite_accuracy_horizontal'; pmin = 0; pmax = 30; cmap = plt.cm.viridis
param = 'satellite_tracking'; pmin = 0; pmax = 8; cmap = plt.cm.viridis.reversed()
imagery = OSM()
period_list = dfdates.sort_values(by=['dt'])['dt'].unique()
period_list = ['2018-09-20','2018-10-08','2019-04-20','2019-05-06','2019-07-11','2019-07-14','2019-07-16','2019-07-21']
#plots
n_row = 2
n_col = 2
n_plot = n_col*n_row
n_period = len(period_list)

#loop making new figures
for m in range(int(np.ceil(n_period/n_plot))): 
#for m in range(53,int(np.ceil(n_period/n_plot))): 
#for m in range(5): 
    print(m)
    plt.rcParams.update({'font.size':14})
    fig,axes = plt.subplots(n_row,n_col,figsize=(18,12),subplot_kw={'projection':imagery.crs})
    ax_list = axes.flatten()
    #loop making new charts
    for i,p in enumerate(period_list[n_plot*m:min(n_plot*(m+1),len(period_list))]):
    #for i,p in enumerate(period_list[0:3]):
        print(p)
        df = pd.read_csv(r'..\..\..\..\Data\Intermediate\geolocation\testmindistance\driveshifts\bq_results_points_{0}.csv'.format(p))
        #pmin = df[param].min(); pmax = df[param].max()
        df['cscale'] = [min(a,pmax) for a in df[param]]
        df_tmp1 = df[(df['vid']==27522)]
        df_tmp2 = df[(df['vid']==27533)]

        ax = ax_list[i]
        #ax.set_extent((df['longitude'].min(),df['longitude'].max(),df['latitude'].min(),df['latitude'].max()),crs=ccrs.Geodetic())
        #ULEZ-ish
        #ax.set_extent((-0.185220,-0.068527,51.483407,51.533020),crs=ccrs.Geodetic())
        #NPL-ish
        ax.set_extent((-0.354722,-0.328436,51.420865,51.432411),crs=ccrs.Geodetic())
        ax.add_image(imagery,14) #integer is grid resolution
        #first plot is vid 1
        ax.scatter(df_tmp1['longitude'],df_tmp1['latitude'],edgecolors=None,s=1,alpha=0.5,label='27522',transform=ccrs.PlateCarree(),c=df_tmp1['cscale'], cmap=cmap,vmin=pmin,vmax=pmax)
        #second plot is vid 2
        pcm = ax.scatter(df_tmp2['longitude'],df_tmp2['latitude'],edgecolors=None,s=1,alpha=0.5,label='27533',transform=ccrs.PlateCarree(),c=df_tmp2['cscale'], cmap=cmap,vmin=pmin,vmax=pmax)
        ax.set_title('{0}'.format(p))
#        cbar = fig.colorbar(pcm,ax=ax,shrink = 0.6)
#        cbar.set_label(param)
    cbar = fig.colorbar(pcm,ax=axes[:,:],location='right',shrink = 0.6)
    cbar.set_label(param)
    #ax.legend()
#    fig.tight_layout()
    fig.savefig(r'..\charts\snapqc_{2}_maps{1}_{0}.png'.format(dt.datetime.today().strftime('%Y%b%d'),m,param), dpi=300)
    plt.close(fig)
