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
sys.path.append(r'..\helpers')
import drivebias_helpers as dh
import ggplot as gg

credentials = service_account.Credentials.from_service_account_file(r"..\..\..\pks\street-view-air-quality-london-dc8f329b26cf.json")
project_id = 'street-view-air-quality-london'
client = bigquery.Client(credentials=credentials,project=project_id)

#true for all queries in this process
dataset_ref = client.dataset('UK')
job_config = bigquery.QueryJobConfig()
job_config.write_disposition = 'WRITE_TRUNCATE' #overwrite table if exists
location = 'EU'

species = 'pm25_pdr'
#species = 'pm25'
#species = 'no2'
if species =='pm25':
    title = 'PM2.5'
elif species == 'no2':
    title = 'NO2'
elif species == 'pm25_pdr':
    title = 'PM2.5 PDR'
###############################################################
#drivebias0 Calculate background from stationary data
destinationtable_str = 'drivebias0_background_'+species
#destinationtable_str = 'drivebias0_background_aqmesh_no2'
job_config.destination = dataset_ref.table(destinationtable_str)
qry_str = dh.sql_str_0background_laqn(species)
#qry_str = dh.sql_str_0background_aqmesh('no2')
#qry_job = client.query(qry_str,location=location,job_config=job_config)
#rows = qry_job.result()
print("Query results loaded into table {0}".format(destinationtable_str))

###############################################################
#drivebias1 Select segments, passes and calculate expected values
sourcetable_str0 = destinationtable_str
destinationtable_str = 'drivebias1_selectpasses_'+species
job_config.destination = dataset_ref.table(destinationtable_str)
qry_str = dh.sql_str_1selectpasses(species,sourcetable_str0)
#qry_job = client.query(qry_str,location=location,job_config=job_config)
#rows = qry_job.result()
print("Query results loaded into table {0}".format(destinationtable_str))

###############################################################
#drivebias2 Create random subsample realizations
sourcetable_str1 = destinationtable_str
destinationtable_str = 'drivebias2_subsamples'+species
job_config.destination = dataset_ref.table(destinationtable_str)
qry_str = dh.sql_str_2subsamples_a(sourcetable_str1)
#qry_job = client.query(qry_str,location=location,job_config=job_config)
#rows = qry_job.result()
print("Query results loaded into table {0}".format(destinationtable_str))

###############################################################
#drivebias3 Summarize bias by n passes
sourcetable_str2 = destinationtable_str
destinationtable_str = 'drivebias3_statistics_{0}.csv'.format(species)
qry_str = dh.sql_str_3summarize(sourcetable_str1,sourcetable_str2)
#qry_job = client.query(qry_str,location=location,job_config=bigquery.QueryJobConfig())
#df = qry_job.to_dataframe()
#df.to_csv(r'..\charts\laqn_{0}'.format(destinationtable_str))
df=pd.read_csv(r'..\charts\laqn_{0}'.format(destinationtable_str))
#colors=['#add8e6','#ffbf47','#90ee90','#8ea1cd','#d48f35','#70ca6d','#6c6cb3','#aa6224','#50a74a','#45399a','#813514','#2f8528','#000080','#590000','#006500']
colors=['#000080','#add8e6', '#6c6cb3', '#add8e6', '#000080']
brks = ['p05t','p25t','p50t','p75t','p95t','p05e','p25e','p50e','p75e','p95e','p05te','p25te','p50te','p75te','p95te']
#restack the data for ggplot, adding method and percentile columns
#total-based
dftmp = df[['n_sub']+brks[:5]].melt(id_vars=['n_sub'],value_vars=brks[:5], var_name = 'stat',value_name = 'value')
dftmp['method']=['(Total-Expected Total)/Expected Total']*dftmp['n_sub'].size
df_stacked = dftmp
#enhancement-based
dftmp = df[['n_sub']+brks[5:10]].melt(id_vars=['n_sub'],value_vars=brks[5:10], var_name = 'stat',value_name = 'value')
dftmp['method']=['(Enhanc-Expected Enhanc)/Expected Enhanc']*dftmp['n_sub'].size
df_stacked = df_stacked.append(dftmp)
#enhancements + full sample background
dftmp = df[['n_sub']+brks[10:]].melt(id_vars=['n_sub'],value_vars=brks[10:], var_name = 'stat',value_name = 'value')
dftmp['method']=['(Enhanc+Expected Backgr-Expected Total)/Expected Total']*dftmp['n_sub'].size
df_stacked = df_stacked.append(dftmp)
df_stacked['percentile']=['{0}th%'.format(a[1:3]) for a in df_stacked['stat']]
#plots
#compare all 3
plt1 = gg.ggplot(df_stacked, gg.aes(x='n_sub',y='value',color='percentile'))+gg.geom_line()+gg.xlab('N drives')+gg.ylab('Bias (%)')+gg.theme_bw()+gg.scale_color_manual(values=colors)+gg.geom_hline(y=[-25,25],linetype="dashed",color="gray")+gg.geom_vline(x=[10,15],linetype="dashed",color="gray")+gg.facet_wrap('method')+gg.ggtitle('Bias comparison {0}'.format(title))
plt1.save(filename = r'..\charts\drivebias_laqn_{0}.png'.format(species), width=None, height=None, dpi=300)

#plot total alone for presenation
plt2 = gg.ggplot(df_stacked[df_stacked['method']=='(Total-Expected Total)/Expected Total'], gg.aes(x='n_sub',y='value',color='percentile'))+gg.geom_line()+gg.xlab('N drives')+gg.ylab('Bias (%)')+gg.ylim(-100,100)+gg.scale_color_manual(values=colors)+gg.geom_hline(y=[-25,25],linetype="dashed",color="gray")+gg.geom_vline(x=[10,15],linetype="dashed",color="gray")+gg.ggtitle('Bias comparison {0}'.format(title))
t = gg.theme_bw()
t._rcParams['font.size']=16
plt2 = plt2+t
plt2.save(filename = r'..\charts\drivebias_laqn_{0}_total.png'.format(species), width=None, height=None, dpi=300)

#plot enhancement alone for presenation
plt3 = gg.ggplot(df_stacked[df_stacked['method']=='(Enhanc+Expected Backgr-Expected Total)/Expected Total'], gg.aes(x='n_sub',y='value',color='percentile'))+gg.geom_line()+gg.xlab('N drives')+gg.ylab('Bias (%)')+gg.ylim(-100,100)+gg.scale_color_manual(values=colors)+gg.geom_hline(y=[-25,25],linetype="dashed",color="gray")+gg.geom_vline(x=[10,15],linetype="dashed",color="gray")+gg.ggtitle('Bias comparison {0}'.format(title))
t = gg.theme_bw()
t._rcParams['font.size']=16
plt3 = plt3+t
plt3.save(filename = r'..\charts\drivebias_laqn_{0}_enhanc.png'.format(species), width=None, height=None, dpi=300)
print("Query results loaded into table {0}".format(destinationtable_str))
