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
import matplotlib.lines as mlines

species = 'NO2' #units are ppb
#load uncertainty data for indiv road segments
destinationtable_str = 'drivebias2_NO2_sampleuncertainty_byfunction_2020Apr23.csv'
destinationcsv_str = os.path.join(r'..\..\..\..\Data\Intermediate\temporal_bias',destinationtable_str)
df = pd.read_csv(destinationcsv_str)
df['conc_expected_ug'] = df['conc_expected']/0.52 #convert to ug/m3
df['diff_ug'] = df['diff']/0.52 #convert to ug/m3

#load combined uncertainty data in ppb total difference
destinationtable_str = 'central2_ulezeval_NO2_after_sampleuncertainty_2020Apr13.csv'
destinationcsv_str = os.path.join(r'..\..\..\..\Data\Intermediate\central_tendency',destinationtable_str)
df2 = pd.read_csv(destinationcsv_str)
cols = ['p{0:02}t'.format(pct) for pct in range(1,100)]
dft = df2.set_index(keys='n_sub',drop=True)[cols].T

#load combined uncertainty data in % difference
destinationtable_str = 'central2_ulezeval_NO2_after_sampleuncertainty_2020Mar20.csv'
destinationcsv_str = os.path.join(r'..\..\..\..\Data\Intermediate\central_tendency',destinationtable_str)
df3 = pd.read_csv(destinationcsv_str)
cols = ['p{0:02}t'.format(pct) for pct in range(1,100)]
dft_pct = df3.set_index(keys='n_sub',drop=True)[cols].T

#plots by function, total difference
plt.rcParams.update({'font.size':14})
fig,axes = plt.subplots(1,2,figsize=(12,6))
ax = axes[0]
df_a = df[(df['n_sub']==10) & (df['function']=='A Road')]
df_b = df[(df['n_sub']==10) & (df['function']=='Local Road')]
for i,segid in enumerate(df['segid'].unique()):
    ax.plot(df_a[df_a['segid']==segid]['diff'],df_a[df_a['segid']==segid]['cumulfrac'],'b-',label='A Road')
    ax.plot(df_b[df_b['segid']==segid]['diff'],df_b[df_b['segid']==segid]['cumulfrac'],'r-',label='Local Road')
ax.plot(dft[10],np.array(range(1,100))/100,'k-',label='All Roads',linewidth=2)
ax.set_ylabel('Cumulative probability')
ax.set_xlabel('Sample error (ppb)')
ax.set_xlim([-50,50])
ax.title.set_text('{0} uncertainty by function, n = 10'.format(species))

ax = axes[1]
df_a = df[(df['n_sub']==35) & (df['function']=='A Road')]
df_b = df[(df['n_sub']==35) & (df['function']=='Local Road')]
for i,segid in enumerate(df['segid'].unique()):
    ax.plot(df_a[df_a['segid']==segid]['diff'],df_a[df_a['segid']==segid]['cumulfrac'],'b-')
    ax.plot(df_b[df_b['segid']==segid]['diff'],df_b[df_b['segid']==segid]['cumulfrac'],'r-')
ax.plot(dft[35],np.array(range(1,100))/100,'k-',label='All Roads',linewidth=2)
ax.set_xlabel('Sample error (ppb)')
ax.set_xlim([-50,50])
blue_line = mlines.Line2D([],[],color='blue',label='A Road')
red_line = mlines.Line2D([],[],color='red',label='Local Road')
black_line = mlines.Line2D([],[],color='black',label='All Road',linewidth=2)
ax.legend(handles=[blue_line,red_line,black_line],loc='right')
ax.title.set_text('{0} uncertainty by function, n = 35'.format(species))
fig.tight_layout()
fig.savefig(r'..\charts\mobile_sampleuncertainty_byfunction_{0}_ppb_{1}.png'.format(species.replace(' ','_'),dt.date.today().strftime('%Y%b%d')))
    
#plots by function, percent difference
plt.rcParams.update({'font.size':14})
fig,axes = plt.subplots(1,2,figsize=(12,6))
ax = axes[0]
df_a = df[(df['n_sub']==10) & (df['function']=='A Road')]
df_b = df[(df['n_sub']==10) & (df['function']=='Local Road')]
for i,segid in enumerate(df['segid'].unique()):
    ax.plot(df_a[df_a['segid']==segid]['diffpct'],df_a[df_a['segid']==segid]['cumulfrac'],'b-',label='A Road')
    ax.plot(df_b[df_b['segid']==segid]['diffpct'],df_b[df_b['segid']==segid]['cumulfrac'],'r-',label='Local Road')
ax.plot(dft_pct[10],np.array(range(1,100))/100,'k-',label='All Roads',linewidth=2)
ax.set_ylabel('Cumulative probability')
ax.set_xlabel('Sample error (%)')
ax.set_xlim([-100,100])
ax.title.set_text('{0} uncertainty by function, n = 10'.format(species))

ax = axes[1]
df_a = df[(df['n_sub']==35) & (df['function']=='A Road')]
df_b = df[(df['n_sub']==35) & (df['function']=='Local Road')]
for i,segid in enumerate(df['segid'].unique()):
    ax.plot(df_a[df_a['segid']==segid]['diffpct'],df_a[df_a['segid']==segid]['cumulfrac'],'b-')
    ax.plot(df_b[df_b['segid']==segid]['diffpct'],df_b[df_b['segid']==segid]['cumulfrac'],'r-')
ax.plot(dft_pct[35],np.array(range(1,100))/100,'k-',label='All Roads',linewidth=2)
ax.set_xlabel('Sample error (%)')
ax.set_xlim([-100,100])
blue_line = mlines.Line2D([],[],color='blue',label='A Road')
red_line = mlines.Line2D([],[],color='red',label='Local Road')
black_line = mlines.Line2D([],[],color='black',label='All Road',linewidth=2)
ax.legend(handles=[blue_line,red_line,black_line],loc='right')
ax.title.set_text('{0} uncertainty by function, n = 35'.format(species))
fig.tight_layout()
fig.savefig(r'..\charts\mobile_sampleuncertainty_byfunction_{0}_pct_{1}.png'.format(species.replace(' ','_'),dt.date.today().strftime('%Y%b%d')))
    
#plots by concentration, percent difference
plt.rcParams.update({'font.size':14})
fig,axes = plt.subplots(1,2,figsize=(12,6))
co = ['navy','mediumseagreen','gold','orangered','maroon']
ax = axes[0]
df_o = []
conc_list = [0,25,35,45,55,1e6]
for p in range(len(conc_list)-1):
    df_a=df[(df['n_sub']==10) & (df['conc_expected_ug']>=conc_list[p]) & (df['conc_expected_ug']<conc_list[p+1])]
    df_o.append(df_a.sort_values(by='diffpct').reset_index(drop=True))
    for i,segid in enumerate(df['segid'].unique()):
        ax.plot(df_a[df_a['segid']==segid]['diffpct'],df_a[df_a['segid']==segid]['cumulfrac'],'-',color=co[p],alpha=0.1)
for p in range(len(conc_list)-1):
    ax.plot(df_o[p]['diffpct'],np.array(df_o[p].index.to_series())/len(df_o[p]),'--',linewidth=2,color=co[p])
ax.plot(dft_pct[10],np.array(range(1,100))/100,'k-',label='All Roads',linewidth=2)
ax.set_ylabel('Cumulative probability')
ax.set_xlabel('Sample error (%)')
ax.set_xlim([-100,100])
ax.title.set_text('{0} uncertainty by concentration, n = 10'.format(species))

ax = axes[1]
df_o = []
for p in range(len(conc_list)-1):
    df_a=df[(df['n_sub']==35) & (df['conc_expected_ug']>=conc_list[p]) & (df['conc_expected_ug']<conc_list[p+1])]
    df_o.append(df_a.sort_values(by='diffpct').reset_index(drop=True))
    for i,segid in enumerate(df['segid'].unique()):
        ax.plot(df_a[df_a['segid']==segid]['diffpct'],df_a[df_a['segid']==segid]['cumulfrac'],'-',color=co[p],alpha=0.1)
for p in range(len(conc_list)-1):
    ax.plot(df_o[p]['diffpct'],np.array(df_o[p].index.to_series())/len(df_o[p]),'--',linewidth=2,color=co[p])
ax.plot(dft_pct[35],np.array(range(1,100))/100,'k-',label='All Roads',linewidth=2)
ax.set_xlabel('Sample error (%)')
ax.set_xlim([-100,100])
leglines = [mlines.Line2D([],[],color=co[0],label='<25'),\
mlines.Line2D([],[],color=co[1],label='25 to <35'),\
mlines.Line2D([],[],color=co[2],label='35 to <45'),\
mlines.Line2D([],[],color=co[3],label='45 to <55'),\
mlines.Line2D([],[],color=co[4],label='>=55')]
black_line = mlines.Line2D([],[],color='black',label='All Road',linewidth=2)
ax.legend(handles=leglines+[black_line],loc='right')
ax.title.set_text('{0} uncertainty by concentration, n = 35'.format(species))
fig.tight_layout()
fig.savefig(r'..\charts\mobile_sampleuncertainty_byconc_{0}_pct_{1}.png'.format(species.replace(' ','_'),dt.date.today().strftime('%Y%b%d')))
    
#plots by concentration, abs difference ug/m3
plt.rcParams.update({'font.size':14})
fig,axes = plt.subplots(1,2,figsize=(12,6))
co = ['navy','mediumseagreen','gold','orangered','maroon']
ax = axes[0]
df_o = []
for p in range(len(conc_list)-1):
    df_a=df[(df['n_sub']==10) & (df['conc_expected_ug']>=conc_list[p]) & (df['conc_expected_ug']<conc_list[p+1])]
    #df_o.append(df_a.groupby(['cumulfrac']).agg({'diff_ug':'median'}).reset_index().sort_values(by='cumulfrac'))
    df_o.append(df_a.sort_values(by='diff_ug').reset_index(drop=True))
    for i,segid in enumerate(df['segid'].unique()):
        ax.plot(df_a[df_a['segid']==segid]['diff_ug'],df_a[df_a['segid']==segid]['cumulfrac'],'-',color=co[p],alpha=0.1)
print(df_a.head())
print(df_o[0].index.to_series())
for p in range(len(conc_list)-1):
    #ax.plot(df_o[p]['diff_ug'],df_o[p]['cumulfrac'],linewidth=3,color=co[p])
    ax.plot(df_o[p]['diff_ug'],np.array(df_o[p].index.to_series())/len(df_o[p]),'--',linewidth=2,color=co[p])
ax.plot(dft[10]/0.52,np.array(range(1,100))/100,'k-',label='All Roads',linewidth=2)
ax.set_ylabel('Cumulative probability')
ax.set_xlabel('Sample error (ug/m3)')
ax.set_xlim([-100,100])
ax.title.set_text('{0} uncertainty by concentration, n = 10'.format(species))

ax = axes[1]
df_o = []
for p in range(len(conc_list)-1):
    df_a=df[(df['n_sub']==35) & (df['conc_expected_ug']>=conc_list[p]) & (df['conc_expected_ug']<conc_list[p+1])]
    df_o.append(df_a.sort_values(by='diff_ug').reset_index(drop=True))
    for i,segid in enumerate(df['segid'].unique()):
        ax.plot(df_a[df_a['segid']==segid]['diff_ug'],df_a[df_a['segid']==segid]['cumulfrac'],'-',color=co[p],alpha=0.1)
for p in range(len(conc_list)-1):
    ax.plot(df_o[p]['diff_ug'],np.array(df_o[p].index.to_series())/len(df_o[p]),'--',linewidth=2,color=co[p])
ax.plot(dft[35]/0.52,np.array(range(1,100))/100,'k-',label='All Roads',linewidth=2)
ax.set_xlabel('Sample error (ug/m3)')
ax.set_xlim([-100,100])
leglines = [mlines.Line2D([],[],color=co[0],label='<25'),\
mlines.Line2D([],[],color=co[1],label='25 to <35'),\
mlines.Line2D([],[],color=co[2],label='35 to <45'),\
mlines.Line2D([],[],color=co[3],label='45 to <55'),\
mlines.Line2D([],[],color=co[4],label='>=55')]
black_line = mlines.Line2D([],[],color='black',label='All Road',linewidth=2)
ax.legend(handles=leglines+[black_line],loc='right')
ax.title.set_text('{0} uncertainty by concentration, n = 35'.format(species))
fig.tight_layout()
fig.savefig(r'..\charts\mobile_sampleuncertainty_byconc_{0}_ugm3_{1}.png'.format(species.replace(' ','_'),dt.date.today().strftime('%Y%b%d')))
plt.rcParams.update({'font.size':14})
fig,ax = plt.subplots(1,1,figsize=(8,6))

#plots by n, all roads, total difference
color_list = ['navy','steelblue','mediumseagreen','gold','orange','orangered','maroon']
for k,n in enumerate(df2['n_sub'].unique()):
    ax.plot(dft[n],np.array(range(1,100))/100,color=color_list[k],label=str(n))

ax.set_ylabel('Cumulative probability')
ax.set_xlabel('Sample error (ppb)')
ax.legend(loc='right',title='n')
ax.title.set_text('{0} uncertainty \n({1} trials for {2:.0f} streets)'.format(species,df2['n_trial'][0],df2['n_seg'][0]))
fig.tight_layout()
fig.savefig(r'..\charts\mobile_sampleuncertainty_all_{0}_ppb_{1}.png'.format(species.replace(' ','_'),dt.date.today().strftime('%Y%b%d')))

plt.rcParams.update({'font.size':14})
fig,ax = plt.subplots(1,1,figsize=(8,6))
#plots by n, all roads, percent difference
color_list = ['navy','steelblue','mediumseagreen','gold','orange','orangered','maroon']
for k,n in enumerate(df2['n_sub'].unique()):
    ax.plot(dft_pct[n],np.array(range(1,100))/100,color=color_list[k],label=str(n))

ax.set_ylabel('Cumulative probability')
ax.set_xlabel('Sample error (%)')
ax.legend(loc='right',title='n')
ax.title.set_text('{0} uncertainty \n({1} trials x {2:.0f} streets)'.format(species,df3['n_trial'][0],df3['n_seg'][0]))
fig.tight_layout()
fig.savefig(r'..\charts\mobile_sampleuncertainty_all_{0}_pct_{1}.png'.format(species.replace(' ','_'),dt.date.today().strftime('%Y%b%d')))
