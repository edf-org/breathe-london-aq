import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from pandas.plotting import register_matplotlib_converters
import datetime as dt
import os
import sys
import glob
from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(r"C:\Users\lpadilla\Documents\London\Scripts\pks\street-view-air-quality-london-dc8f329b26cf.json")
project_id = 'street-view-air-quality-london'
client = bigquery.Client(credentials=credentials,project=project_id)
location = 'EU'
dataset_str = 'UK'

register_matplotlib_converters()
root_path = r"C:\Users\lpadilla\Documents\London\Data\Intermediate\grid_snap_road\testmeddrivepassmeans"
src_path = os.path.join(root_path,"snap4_drivesummarystats_19Jun27.csv")

df = pd.read_csv(src_path,index_col=False)
colora = '#838996'
colorb = '#1F262A'
colorc = '#A8A9AD'

def abbrv(name):
    if len(name)<=7:
        return name
    else:
        abbr_list = []
        n_list = name.split()
        wordlen = int(7./len(n_list))
        for n in n_list:
            abbr_list.append(n[:wordlen])
        return ' '.join(abbr_list)
    
dict_list = [{'pm2_5_med':{'title':'PM2.5 median of drive hr means (ug/m3)','color':colorb,'unit':'ug/m3','ax':0},\
        'pm2_5_drvct':{'title':'PM2.5 drive hr count','color':colorc,'unit':'count','ax':1}},\
        {'co2_med':{'title':'CO2 median of drive hr means (umol/mol)','color':colorb,'unit':'umol/mol','ax':0},\
        'co2_drvct':{'title':'CO2 drive hr count','color':colorc,'unit':'count','ax':1}},\
        {'no2_med':{'title':'NO2 median of drive hr means (ppbv)','color':colorb,'unit':'ppbv','ax':0},\
        'no2_drvct':{'title':'NO2 drive hr count','color':colorc,'unit':'count','ax':1}},\
        {'bc_med':{'title':'BC median of drive hr means (ng/m3)','color':colorb,'unit':'ng/m3','ax':0},\
        'bc_drvct':{'title':'BC drive hr count','color':colorc,'unit':'count','ax':1}}]
sql_str = "select fid as polyid, name from UK.london_drive_polygons where fid <> - 1 and name <> 'Teddington' order by polyid"
poly_df = client.query(sql_str, location=location).to_dataframe()
#allpoly_df = client.query(sql_str, location=location).to_dataframe()
#need to add -1 (no poly)
#nopoly_df = pd.DataFrame([[-1,'no polygon']],columns=['polyid','name'])
#poly_df=pd.concat([nopoly_df,allpoly_df],ignore_index=True)

for sd in dict_list:
    out_path = os.path.join(root_path,"plot0_polycompare_"+list(sd.keys())[0]+dt.date.today().strftime('_%y%b%d')+".png")
    #figure 1, boxplots by species
    fig1 = plt.figure()
    fig1.set_size_inches(11,8)
    gs=fig1.add_gridspec(1,3)
    ax1 = []
    ax1.append(fig1.add_subplot(gs[0,0:2]))
    ax1.append(fig1.add_subplot(gs[0,2]))
    #fig1,ax1 = plt.subplots(1,3)
    fig1.suptitle('')
    chartdata_list = []

    #loop to collect all the stats 
    for row in poly_df.itertuples():
        df_row = []
        #get data for species at each site
        for i,(sp,val) in enumerate(sd.items()):
            #print(row.polyid, row.name)
            tmp_df = df[(df['polyid']==row.polyid) & (df[sp].notnull())][sp]
            #if len(tmp_df) > 0:
            df_row.extend([row.polyid,tmp_df.quantile(.1),tmp_df.quantile(.9),tmp_df.median(),len(tmp_df),'{0},{1} ({2} rds)'.format(abbrv(row.name),row.polyid,len(tmp_df))])
        chartdata_list.append(df_row)
    chartdata_df = pd.DataFrame.from_records(chartdata_list,columns=['polyid0','min0','max0','med0','len0','label0','polyid1','min1','max1','med1','len1','label1'])
    #write to csv
    chartdata_df.to_csv(os.path.join(root_path,sp+dt.date.today().strftime('_%y%b%d')+".csv"),index=False)

    #sort and plot them
    sortdata_df = chartdata_df[chartdata_df['len0']>0].sort_values(by='med1',ascending=True).reset_index()
    #print(sortdata_df)
    for i,(sp,val) in enumerate(sd.items()):
        for row in sortdata_df.itertuples():
            an = val['ax']
            at = str(an)
            ax1[an].semilogx([getattr(row,'min'+at), getattr(row,'max'+at)],[row.Index]*2,'-',color=val['color'],linewidth=2)
            ax1[an].semilogx(getattr(row,'med'+at),row.Index,'o',color=val['color'])

        ax1[an].set_title(val['title'])
        ax1[an].set_yticks(list(sortdata_df.index))
        ax1[an].set_ylim(-1,len(sortdata_df)+1)
        #ax1[idx_x].set_xticklabels(abbrday_list)
        #ax1[i].set_xlabel('Concentration ({0})'.format(species_dict[sp]['unit']))
    ax1[0].set_yticklabels(sortdata_df['label0'],fontsize=9,wrap=False)
    ax1[1].set_yticklabels([],fontsize=9,wrap=False)
        #ax1[i].set_ylim(bottom=-9)

    fig1.tight_layout()
    fig1.savefig(out_path,dpi=300)
