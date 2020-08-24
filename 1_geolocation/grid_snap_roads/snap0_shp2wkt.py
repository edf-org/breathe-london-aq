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

import sys
sys.path.insert(0,r'..\helpers')
import arc_road_helpers as arh
import arcpy

infc =r"..\..\..\..\Data\Intermediate\greaterLondon_roads_wgs84\greaterLondon_roads_wgs84.shp" 
outcsv=r"..\..\..\..\Data\Intermediate\greaterLondon_roads_wgs84\greaterLondon_roads_wgs84.csv"

fieldin_list = ["FID","SHAPE@WKT","fictitious","identifier","class",\
        "roadNumber","name1","name1_lang","name2","name2_lang","formOfWay",\
        "primary","trunkRoad","loop","startNode","endNode","structure",\
        "nameTOID","numberTOID","function"]
#convert to WKT and print
#source fc should be WGS84 for BQ
#txt=arh.shp2wkt(infc,outcsv,fieldin_list)
#print(txt)

infc =r"..\..\..\..\Data\Intermediate\boundaries\london_drive_polygons.shp"
outcsv=r"..\..\..\..\Data\Intermediate\boundaries\london_drive_polygons.csv"

fieldin_list = ["FID","SHAPE@WKT","Name","FolderPath","PopupInfo",\
        "Shape_Leng","Shape_Area"]
#convert to WKT and print
#source fc should be WGS84 for BQ
txt=arh.shp2wkt(infc,outcsv,fieldin_list)
print(txt)
