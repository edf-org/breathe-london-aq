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

#check for consistency with pod data

#pods with nearest segment, must be < 30 m 
df_match = pd.read_csv(r'..\..\..\..\Data\Intermediate\central_tendency\pod_segments_within30m.csv')

#pods long-term means/medians
df_pod = pd.read_csv(r'..\..\..\..\Data\Intermediate\central_tendency\Dan\5-4-2020_AQMesh_LAQN_pod_median_update.csv')

#segments WHO confidence
df_WHO = pd.read_csv(r'..\..\..\..\Data\Intermediate\central_tendency\central_mean_exceeds_WHO_confidence_post_ulez_40ugm3_2020Aug17.csv')
threshold = 40
#df_WHO = pd.read_csv(r'..\..\..\..\Data\Intermediate\central_tendency\central_exceeds_WHO_confidence_post_ulez_30ugm3_2020Apr23.csv')
#threshold = 30
#df_WHO = pd.read_csv(r'..\..\..\..\Data\Intermediate\central_tendency\central_exceeds_WHO_confidence_post_ulez_50ugm3_2020Apr23.csv')
#threshold = 50

option = 'B' #choose A or B
central_stat = 'mean' #choose mean or median
#join the dataframes
df_joined = df_match.merge(df_pod, on=['pod_id_location'])
df_joined = df_joined.merge(df_WHO, on=['segid'])

if central_stat == 'mean':
    central_calc = 'mean_no2_ugm3_Apr8fullyear'
elif central_stat == 'median':
    central_calc = 'median_no2_ugm3_Apr8fullyear'
if option == 'A':
    #OPTION A
    #average confidence across segments for each pod
    df_joined = df_joined.groupby(['Type_x','ULEZ', 'Latitude_x', 'Longitude_x',central_calc,'pod_id_location']).agg({'confidence': 'mean','median':'mean','pass_count':'mean'}).reset_index()
elif option == 'B':
    #OPTION B
    #choose confidence from closest segment
    df_joined = df_joined[(df_joined['pod_seg_dist_m']==df_joined['min_pod_seg_dist_m'])]

#create confidence ranges
df_joined['confrange'] = np.floor(df_joined['confidence']/0.333)*0.333
df_joined['exceedsWHO'] = df_joined[central_calc]>threshold
df_joined['mobile_central_tendency_ugm3'] = df_joined['central_tendency']/0.52
#group by confidence ranges, counting pods in each range

#save summary table
df_joined.to_csv(r'..\..\..\..\Data\Intermediate\central_tendency\central_'+central_stat+'_exceeds_WHO_'+str(threshold)+option+'_test.csv',index=False)
#code run in BQ console to find nearest segment to each pod
"""
--select pods and nearest roads from those that have an exceedance probability
select segid, pod_seg_dist_m, min_pod_seg_dist_m, median as median_ppb
, pod_id_location, Type, ULEZ, Latitude, Longitude, Distance_from_Road
from (
  select a.segid, median
  , min(ST_DISTANCE(geowkt, ST_GEOGPOINT(Longitude, Latitude))) over (partition by pod_id_location) as min_pod_seg_dist_m
  , ST_DISTANCE(geowkt, ST_GEOGPOINT(Longitude, Latitude)) as pod_seg_dist_m
, pod_id_location, Type, ULEZ, Latitude, Longitude, Distance_from_Road
FROM (select Site_Name as pod_id_location, Type, ULEZ, Latitude, Longitude, null as Distance_from_Road from LAQN.LAQN_site_metadata
      union all 
      select cast(pod_id_location as STRING) pod_id_location, Type, ULEZ, Latitude, Longitude, Distance_from_Road from AQMesh.NO2_site_info_20180901_20200112)
, UK.central3_postulez_summaryno2 a
join UK.greaterlondon_roads_wgs84_split30m b on a.segid=b.segid
where pass_count>=5 and median is not null
)
where pod_seg_dist_m < 30
order by pod_id_location, pod_seg_dist_m desc
"""
