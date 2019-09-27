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
sys.path.insert(0,r"..\helpers")
import grid_road_helpers as grh

credentials = service_account.Credentials.from_service_account_file(r"..\..\..\pks\street-view-air-quality-london-dc8f329b26cf.json")
project_id = 'street-view-air-quality-london'
client = bigquery.Client(credentials=credentials,project=project_id)

dataset_str = 'UK'
schema_list = [('fid','INTEGER'), ('geowkt','GEOGRAPHY'),('name','STRING'),\
        ('folderpath','STRING'),('popupinfo','STRING'),('shape_len','FLOAT'),\
        ('shape_area','FLOAT')]

#load data tables
filename_str=r"..\..\..\..\Data\Intermediate\boundaries\london_drive_polygons.csv"
print(filename_str)

grh.wkt2bq(filename_str,client,dataset_str,schema_list,location='EU')
