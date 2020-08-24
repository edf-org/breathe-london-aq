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

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from pandas.plotting import register_matplotlib_converters
import datetime as dt
import os
import sys
import glob

register_matplotlib_converters()

in_str = r'C:\Users\lpadilla\Documents\London\Data\Source\logger_timeshift'
in2_str = r"C:\Users\lpadilla\Documents\London\Data\Source\driverdailystatus.csv"
car_list = ['27522','27533']
fig2,ax2 = plt.subplots(2,1)
fig2.set_size_inches(6,10)
ht = {'27522':{'min':-15,'max':7},'27533':{'min':-35,'max':15}}
for i,car in enumerate(car_list):

    #read driver daily status data
    files_list = glob.glob(in2_str)
    driverdata_list = []
    for f in files_list:
        print(f)
        with open(f,'r') as fh:
            driverdata_list.extend(fh.read().splitlines())

    driverdata_list = [tuple(row.split(',')) for row in driverdata_list[1:] if len(row.split(','))==3]
    status_list, drivedate_list, vehicle_list = zip(*driverdata_list)
    drivedate_di = [dt.datetime.strptime(d,'%m/%d/%Y') for d in drivedate_list]
    driverdata_df = pd.DataFrame(zip(drivedate_di,status_list,vehicle_list),columns=['datestamp','status','vehicle'])
    up_df = driverdata_df[(driverdata_df['status']=='Up') & (driverdata_df['vehicle'].str.contains(car))]['datestamp']
    
    #read time shift log data
    files_list = glob.glob(os.path.join(in_str,car+'*.txt'))
    longdata_list = []
    for f in files_list:
        #print(f)
        with open(f,'r') as fh:
            longdata_list.extend(fh.read().splitlines())

    longdata_list = [tuple(row.split(',')) for row in longdata_list if len(row.split(','))==2]
    clean_list = []
    for row in longdata_list:
        try:
            clean_list.append(tuple([np.datetime64(row[0]),float(row[1].replace('s',''))]))
        except ValueError:
            continue
    data_arr = np.array(clean_list)
    #,dtype=[('jiboxtime','datetime64'),('deltat','float32')])
    #longdata_arr['jiboxtime'] = np..astype('datetime64')
    data_arr[data_arr[:,0].argsort()]
    jiboxtimes_list, deltat_list = zip(*data_arr)
    jiboxtimes_arr = np.array(jiboxtimes_list)
    jiboxweekday_idx = pd.DatetimeIndex(jiboxtimes_arr).day_name()
    jiboxhour_idx = pd.DatetimeIndex(jiboxtimes_arr).hour
    jiboxdate_idx = pd.DatetimeIndex(jiboxtimes_arr).date
    deltat_arr = np.array(deltat_list) #seconds
    timebtwnshifts_arr = (jiboxtimes_arr[1:]-jiboxtimes_arr[:-1]).astype('float64')/1.e9/3600. #hours
    print(min(timebtwnshifts_arr))
    data_df = pd.DataFrame(zip(jiboxtimes_arr,jiboxweekday_idx,jiboxhour_idx,jiboxdate_idx,deltat_arr),columns=['timestamp','dayofweek','hourofday','datestamp','timeshift'])
    day_list = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday']
    abbrday_list = [s[0:3] for s in day_list]
    dayboxplot_list = []
    for day in day_list:
        dayboxplot_list.append(data_df[data_df['dayofweek']==day]['timeshift'])

    hourboxplot_list = []
    for hour in range(24):
        hourboxplot_list.append(data_df[data_df['hourofday']==hour]['timeshift'])

    #fit line for rate of drift
    fit = np.polyfit(timebtwnshifts_arr,abs(deltat_arr[1:]),1)
    #fit forcing origin
    #fit2 = np.linalg.lstsq(timebtwnshifts_arr.reshape(-1,1),abs(deltat_arr[1:]))
    print(fit)
    #print(fit2)
    fity = fit[0]*timebtwnshifts_arr+fit[1]
    #fity2 = fit2[0][0]*timebtwnshifts_arr

    shiftcount_df = data_df.groupby(['datestamp']).size()

    fig,ax = plt.subplots(2,2)
    fig.set_size_inches(12,8)
    fig.suptitle('Vehicle '+car)
    ax[0,0].plot(timebtwnshifts_arr,abs(deltat_arr[1:]),'.')
    ax[0,0].plot(timebtwnshifts_arr,fity)
    #ax[0,0].plot(timebtwnshifts_arr,fity2)
    ax[0,0].text(400,4,'drift ={0:.4f} s/h'.format(fit[0]))
    #ax[0,0].text(400,3,'drift ={0:.4f} s/h'.format(fit2[0][0]))
    ax[0,0].set_ylabel('Abs(Jibox minus logger time) (s)')
    ax[0,0].set_xlabel('Elapsed time between syncs (h)')
    ax[1,0].plot(jiboxtimes_arr, deltat_arr,'.')
    ax[1,0].plot([jiboxtimes_arr[0],jiboxtimes_arr[-1]],[1,1],'k--')
    ax[1,0].plot([jiboxtimes_arr[0],jiboxtimes_arr[-1]],[-1,-1],'k--')
    ax[1,0].bar(up_df, len(up_df)*[ht[car]['max']],color='palegreen',width=1)
    ax[1,0].bar(up_df, len(up_df)*[ht[car]['min']],color='palegreen',width=1)
    ax[1,0].set_ylabel('Jibox minus logger time (s)')
    ax[1,0].set_xlabel('Jibox time at sync')
    ax[0,1].boxplot(dayboxplot_list,whis=[1,99])
    ax[0,1].plot([0,7],[1,1],'k--')
    ax[0,1].plot([0,7],[-1,-1],'k--')
    ax[0,1].set_xticklabels(abbrday_list)
    ax[0,1].set_ylabel('Jibox minus logger time (s)')
    ax[0,1].set_xlabel('Day of week')
    ax[1,1].boxplot(hourboxplot_list,whis=[1,99])
    ax[1,1].plot([0,24],[1,1],'k--')
    ax[1,1].plot([0,24],[-1,-1],'k--')
    ax[1,1].set_xticklabels(range(24))
    ax[1,1].set_ylabel('Jibox minus logger time (s)')
    ax[1,1].set_xlabel('Hour of day')
    fig.savefig('TimeSync'+car+dt.date.today().strftime('_%y%b%d')+'.png',dpi=300)

    #print(shiftcount_df)
    ax2[i].bar(shiftcount_df.index,shiftcount_df)
    ax2[i].set_title('Vehicle '+car)
    ax2[i].set_ylabel('Number of syncs per day')
fig2.savefig('SyncCount'+dt.date.today().strftime('_%y%b%d')+'.png',dpi=300)
#plt.show()
