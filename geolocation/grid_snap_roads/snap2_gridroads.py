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
