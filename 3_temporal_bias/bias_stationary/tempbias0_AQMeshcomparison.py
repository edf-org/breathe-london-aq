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
import ggplot as gg
import pandas as pd
import sys
sys.path.append(r"..\helpers")
import bias_helpers as bh
import datetime as dt

credentials = service_account.Credentials.from_service_account_file(
    r"..\..\..\pks\street-view-air-quality-london-dc8f329b26cf.json")
project_id = 'street-view-air-quality-london'
bqclient = bigquery.Client(credentials=credentials,project=project_id)

#round mobile timestamps to match nearest stationary timestamp
#format string to timestamp and round nearest 1 min
mobiletime_1min = "TIMESTAMP_TRUNC(TIMESTAMP_ADD(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',passtime), INTERVAL 30 SECOND),MINUTE)" 
#format string to timestamp and round to nearest 15 min
mobiletime_15min = "timestamp_add(timestamp('1900-01-01 00:00:00'), INTERVAL cast(timestamp_diff(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',passtime),timestamp('1900-01-01 00:00:00'),MINUTE)/15 as int64)*15 MINUTE)"
#format string to timestamp and round to nearest 60 min
mobiletime_60min = "TIMESTAMP_TRUNC(TIMESTAMP_ADD(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',passtime), INTERVAL 30 MINUTE),HOUR)" 

charts = [\
#        {'name':'no2_aqmesh1min','cte0':bh.cte0_str_no2_aqmesh1min(),'mobiletime':mobiletime_1min,\
#        'pid':133,'template':bh.qry_str_template_a()},\
#        {'name':'no2_aqmesh1hr','cte0':bh.cte0_str_no2_aqmesh1hr(),'mobiletime':mobiletime_60min,\
#        'pid':133,'template':bh.qry_str_template_a()},\
#        {'name':'no2_laqn15min','cte0':bh.cte0_str_no2_laqn15min(),'mobiletime':mobiletime_15min,\
#        'pid':133,'template':bh.qry_str_template_a()},\
#        {'name':'pm25_laqn15min','cte0':bh.cte0_str_pm25_laqn15min(),'mobiletime':mobiletime_15min,\
#        'pid':3,'template':bh.qry_str_template_a()},\
#        {'name':'no2_aqmesh1min_restricted','cte0':bh.cte0_str_no2_aqmesh1min(),'mobiletime':mobiletime_1min,\
#        'pid':133,'template':bh.qry_str_template_b()},\
#        {'name':'no2_aqmesh1hr_restricted','cte0':bh.cte0_str_no2_aqmesh1hr(),'mobiletime':mobiletime_60min,\
#        'pid':133,'template':bh.qry_str_template_b()},\
#        {'name':'no2_laqn15min_restricted','cte0':bh.cte0_str_no2_laqn15min(),'mobiletime':mobiletime_15min,\
#        'pid':133,'template':bh.qry_str_template_b()},\
#        {'name':'pm25_laqn15min_restricted','cte0':bh.cte0_str_pm25_laqn15min(),'mobiletime':mobiletime_15min,\
#        'pid':3,'template':bh.qry_str_template_b()}\
        {'name':'no2_laqn60min_restricted','cte0':bh.cte0_str_no2_laqn60min(),'mobiletime':mobiletime_60min,\
        'pid':133,'template':bh.qry_str_template_b()},\
        {'name':'no2_laqn60min','cte0':bh.cte0_str_no2_laqn60min(),'mobiletime':mobiletime_60min,\
        'pid':133,'template':bh.qry_str_template_a()}\
#        {'name':'pm25_laqn60min_restricted','cte0':bh.cte0_str_pm25_laqn60min(),'mobiletime':mobiletime_60min,\
#        'pid':3,'template':bh.qry_str_template_b()}\
        ]

site_list = ['IM1', 'CT2','WM6','CT4','CT6','CT8','WMA','WMB','WMC','NB1','BL0','CT3','SK6','WM5','WM0']
colors=['#000080','#add8e6', '#6c6cb3', '#add8e6', '#000080']
dtstamp = dt.datetime.today().strftime('%Y%b%d')
for c in charts:
    qry_str=c['template'].format(c['cte0'],c['mobiletime'],c['pid'])
    qry_job = bqclient.query(qry_str,location='EU',job_config=bigquery.QueryJobConfig())

    #save result as dataframe
    df_a = qry_job.to_dataframe()
    df_a.to_csv(r'..\charts\{0}_distribs_{1}.csv'.format(c['name'],dtstamp))
    df_a = pd.read_csv(r'..\charts\{0}_distribs_{1}.csv'.format(c['name'],dtstamp))
    df_a = df_a[df_a['site_str'].isin(site_list)] #filter to only ULEZ sites if applicable
    df_along = df_a.melt(id_vars=['site_str','n_passes'],value_vars=['p05','p25','p50','p75','p95'], var_name = 'yparam',value_name = 'value')
    print(c['name'])
    #print(df_a)

    #plots
    #split percentiles into different charts, all sites
    #plt1 = gg.ggplot(df_along, gg.aes(x='n_passes',y='value',color='site_str'))+gg.geom_point()+gg.xlab('N drives')+gg.ylab('Bias (%)')+gg.theme_bw()+gg.xlim(0,100)+gg.facet_wrap('yparam',scales='free_y')
    #plt1.save(filename = r'..\charts\bias_{0}.png'.format(c['name']), width=None, height=None, dpi=200)
    #n_segments
    plt2 = gg.ggplot(df_a, gg.aes(x='n_passes',y='n_segments',color='site_str'))+gg.geom_line()+gg.xlab('n, number drive periods')+gg.ylab('Sample size (number of drive patterns)')+gg.theme_bw()+gg.xlim(0,35)+gg.ylim(0,2000)
    plt2.save(filename = r'..\charts\n_segments_{0}_{1}.png'.format(c['name'],dtstamp), width=None, height=None, dpi=200)
    #combine percentiles, split sites 
    plt3 = gg.ggplot(df_along, gg.aes(x='n_passes',y='value',color='yparam'))+gg.geom_line()+gg.xlab('n, number of drive periods')+gg.ylab('Sample error (%)')+gg.theme_bw()+gg.xlim(0,35)+gg.ylim(-100,100)+gg.geom_hline(y=25,linetype="dashed",color="gray")+gg.geom_hline(y=-25,linetype="dashed",color="gray")+gg.geom_vline(x=[10,15],linetype="dashed",color="gray")+gg.scale_color_manual(values=colors)+gg.facet_wrap('site_str')
    plt3.save(filename = r'..\charts\percentiles_{0}_{1}.png'.format(c['name'],dtstamp), width=None, height=None, dpi=200)

