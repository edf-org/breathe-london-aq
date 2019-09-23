from google.cloud import bigquery
from google.oauth2 import service_account
import glob
import os
import sys
sys.path.insert(0,r"C:\Users\lpadilla\Documents\Houston\Scripts\grid_road")
import grid_road_helpers as grh

credentials = service_account.Credentials.from_service_account_file(r"C:\Users\lpadilla\Documents\London\Scripts\pks\street-view-air-quality-london-dc8f329b26cf.json")
project_id = 'street-view-air-quality-london'
client = bigquery.Client(credentials=credentials,project=project_id)

dataset_str = 'UK'
schema_list = [('fid','INTEGER'), ('geowkt','GEOGRAPHY'),('name','STRING'),\
        ('folderpath','STRING'),('popupinfo','STRING'),('shape_len','FLOAT'),\
        ('shape_area','FLOAT')]

#load data tables
filename_str=r"C:\Users\lpadilla\Documents\London\Data\Intermediate\boundaries\london_drive_polygons.csv"
print(filename_str)

grh.wkt2bq(filename_str,client,dataset_str,schema_list,location='EU')
