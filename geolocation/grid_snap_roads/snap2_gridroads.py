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

import os
import sys
sys.path.append(r'..\helpers')
import grid_road_helpers as grh
from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(r"..\..\..\pks\street-view-air-quality-london-dc8f329b26cf.json")
project_id = 'street-view-air-quality-london'
client = bigquery.Client(credentials=credentials,project=project_id)
location = 'EU'
tb = 'greaterlondon_roads_wgs84'
outfc = r"..\..\..\..\Data\Intermediate\geolocation\gridspacing\grid_30m\grid_30m_greaterlondon_v4.shp"

fc = grh.split_at_gridded_points(client, {'ds':'UK','tb':tb}, outfc, "geowkt", "fid", "function,name1,numbertoid",location)
print(fc)
