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
import os
sys.path.append(r'..\..\1_geolocation\helpers')
import arc_road_helpers as arh
import datetime as dt

dirname = r'..\..\..\..\Data\Intermediate\central_tendency'
fname = 'central_mean_exceeds_WHO_confidence_post_ulez_40ugm3_2020Aug17'
csvtoconvert = os.path.join(dirname,fname+'.csv')
outfc = os.path.join(dirname,fname+".shp")

fieldin_list = ["the_geom","segid","pass_count","central_tendency","function","length_m","inULEZ","confidence"]
#first item must be geometry, field names <= 10 characters
fieldout_list = ["SHAPE@","segid","pass_count","centr_tend","function","inULEZ","confidence"]
typeout_list = ["geometry","text","short","float","text","text","float"]
order_list = list(range(0,5))+[6,7] #index of input fields associated with output

fc = arh.wkt2shp(csvtoconvert,outfc,fieldout_list,typeout_list,order_list)
print(fc)
