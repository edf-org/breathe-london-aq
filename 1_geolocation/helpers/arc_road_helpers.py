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

#arc_road_helpers.py
#functions using arcpy, replace with open source later
import glob
import os
import arcpy
import csv
import datetime as dt
import codecs

#create shapefile from Well-Known-Text data format
def wkt2shp(incsv_orlist,outfc, fieldout_list,typeout_list, order_list):
    sr = arcpy.SpatialReference(4326) #set to WGS 1984
    arcpy.env.overwriteOutput = True
    #create empty shapefile, Id field will be automatically generated
    arcpy.CreateFeatureclass_management(os.path.dirname(outfc),os.path.basename(outfc),"POLYLINE",spatial_reference=sr)
    #add fields
    for fname, dtype in zip(fieldout_list[1:],typeout_list[1:]):
        arcpy.AddField_management(outfc,fname,dtype)
        print(fname,dtype)
    #delete default fields
    if len(fieldout_list)> 1:
        arcpy.DeleteField_management(outfc,'Id') #minimum of one other field in addition to OID and Shape required
    #check fields added correctly in new feature class
    for fd in fieldout_list:
        print(fd, fd in [field.name for field in arcpy.ListFields(outfc)])
    #search cursor on input text file
    if isinstance(incsv_orlist,list):
        cursor = incsv_orlist
        print('list input')
    else: 
        cursor = arcpy.da.SearchCursor(incsv_orlist,['*'])
        print(cursor.fields)
    #insert cursor on output shape file
    curs_out = arcpy.da.InsertCursor(outfc,fieldout_list) #if only columns are OID and Shape, could write an Id
    print(curs_out.fields)
    #cursor.next() #skip header line #not needed, arc seems to do this automatically
    for m,row in enumerate(cursor):
        if isinstance(incsv_orlist,list):
            temp=row
        else:
            temp=list(row)
        for i,r in enumerate(temp):
            #shapefiles don't support null values, need to adopt our own null convention
            if not r:
                if typeout_list[i].lower()=="text":
                    temp[i]=''
                else: 
                    temp[i]=-999
        out_list = [arcpy.FromWKT(temp[order_list[0]],sr)]+[temp[order_list[k]] for k in range(1,len(order_list))]
        curs_out.insertRow(out_list)
    return outfc

#create Well-Known-Text csv dataset from shapefile
def shp2wkt(fc,outcsv,fieldin_list):
    #make a copy in a temporary folder removing any Z, M values
    temp_path = os.path.join(os.path.dirname(fc),'temp'+dt.datetime.today().strftime('%y%b%d_%H%M%S'))
    os.mkdir(temp_path)
    arcpy.env.outputZFlag = 'Disabled'
    arcpy.env.outputMFlag = 'Disabled'
    arcpy.FeatureClassToShapefile_conversion(fc,temp_path)
    #convert to WKT and print
    #source fc should be WGS84 for BQ
    fc_temp = os.path.join(temp_path,os.path.basename(fc))
    cursor = arcpy.da.SearchCursor(fc_temp, fieldin_list)
    with open(outcsv,'w') as f:
        f_writer = csv.writer(f,delimiter=',',quotechar='"',lineterminator='\n')
        for row in cursor:
            f_writer.writerow([s.encode('utf8') if isinstance(s,unicode) else s for s in row])
    #delete the temporary shapefile and directory
    arcpy.Delete_management(fc_temp)
    os.rmdir(temp_path)
    return outcsv
