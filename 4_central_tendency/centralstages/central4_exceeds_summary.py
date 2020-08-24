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

import pandas as pd
import numpy as np

#summarize by uncertainty range, road function, ULEZ location

#segments WHO confidence
file_list = [{'label':'mean','path':r'..\..\..\..\Data\Intermediate\central_tendency\central_mean_exceeds_WHO_confidence_post_ulez_40ugm3_2020Jun12.csv'},\
        {'label':'median','path':r'..\..\..\..\Data\Intermediate\central_tendency\central_median_exceeds_WHO_confidence_post_ulez_40ugm3_2020Jun12.csv'}]

for f in file_list:
    #all monitored roads with >= 5 passes
    df_orig=pd.read_csv(f['path'])
    #also repeat for ULEZ only
    df_ulez = df_orig[df_orig['inULEZ']].copy(deep=True)

    for d in [{'label':'all','df':df_orig},{'label':'ulez','df': df_ulez}]:
        df = d['df']
        #create confidence ranges
        df['confrange'] = np.floor(df['confidence']/0.333)*0.333

        #group by confidence ranges, counting segments and lengths in each range
        df_group = df.groupby(by=['confrange']).agg({'segid':'count','length_m':'sum'}).reset_index()
        #divide by total count/sum
        df_group['total_length_m'] = df['length_m'].sum()
        df_group['total_count'] = df['length_m'].count()
        df_group['frac_length_m'] = df_group['length_m']/df_group['total_length_m']
        df_group['frac_count'] = df_group['segid']/df_group['total_count']

        print(f['label'],d['label'])
        print(df_group.head())
        print('50%',len(df),len(df[df['confidence']>.5])/len(df))
        df_group.to_csv(r'..\..\..\..\Data\Intermediate\central_tendency\central4_summary_{0}_{1}.csv'.format(f['label'],d['label']))
        #print('length (km)',total_length_m/1000)
        #print(frac_length)
        #print('count (# seg)',total_count)
        #print(frac_count)

        #count/sum by function
        df_group1 = df.groupby(by=['function']).agg({'segid':'count','length_m':'sum'}).reset_index()
        #group by confidence range and function
        df_group2 = df.groupby(by=['confrange','function']).agg({'segid':'count','length_m':'sum'}).reset_index()
        #divide by function count/sum
        df_join = df_group1.merge(df_group2,on='function')
        df_join['frac_length_m'] = df_join['length_m_y']/df_join['length_m_x']
        df_join['frac_count'] = df_join['segid_y']/df_join['segid_x']
        
        print(df_join.head())
        df_join.to_csv(r'..\..\..\..\Data\Intermediate\central_tendency\central4_summary_by_function_{0}_{1}.csv'.format(f['label'],d['label']))


#save summary table
#df_joined.to_csv(r'..\..\..\..\Data\Intermediate\central_tendency\central_'+central_stat+'_exceeds_WHO_'+str(threshold)+option+'_test.csv',index=False)
