import sys
sys.path.insert(0,r'C:\Users\lpadilla\Documents\Houston\Scripts\grid_road')
import arc_road_helpers as arh
import arcpy

infc =r"C:\Users\lpadilla\Documents\London\Data\Intermediate\greaterLondon_roads_wgs84\greaterLondon_roads_wgs84.shp" 
outcsv=r"C:\Users\lpadilla\Documents\London\Data\Intermediate\greaterLondon_roads_wgs84\greaterLondon_roads_wgs84.csv"

fieldin_list = ["FID","SHAPE@WKT","fictitious","identifier","class",\
        "roadNumber","name1","name1_lang","name2","name2_lang","formOfWay",\
        "primary","trunkRoad","loop","startNode","endNode","structure",\
        "nameTOID","numberTOID","function"]
#convert to WKT and print
#source fc should be WGS84 for BQ
#txt=arh.shp2wkt(infc,outcsv,fieldin_list)
#print(txt)

infc =r"C:\Users\lpadilla\Documents\London\Data\Intermediate\boundaries\london_drive_polygons.shp"
outcsv=r"C:\Users\lpadilla\Documents\London\Data\Intermediate\boundaries\london_drive_polygons.csv"

fieldin_list = ["FID","SHAPE@WKT","Name","FolderPath","PopupInfo",\
        "Shape_Leng","Shape_Area"]
#convert to WKT and print
#source fc should be WGS84 for BQ
txt=arh.shp2wkt(infc,outcsv,fieldin_list)
print(txt)
