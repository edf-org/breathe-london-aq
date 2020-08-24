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

#grid_road_helpers.py
#functions to generate pts along roads ~x m apart
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import BadRequest
import glob
import os
import csv
import arc_road_helpers as arh
import collections as coll
import numpy as np
import uuid

#wkt2bq is a python approach to loading geographic data into Google's BigQuery
#from a csv file with well-known-text format for the geography
#The BQ command line interface load function is another option and uses less code
#schema_list is a list of (name,datatype) tuples describing each table column
#where name and datatype are strings
def wkt2bq(csvpath_str, client, dataset, schema_list, rowskip=0, location='US'):
    dataset_ref = client.dataset(dataset)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
#    job_config.fieldDelimiter = ','
#    job_config.quote = '"'
    job_config.skip_leading_rows = rowskip
#    job_config.autodetect = True
    job_config.write_disposition = 'WRITE_TRUNCATE' #overwrite table if exists
    temp_list = []
    for tup in schema_list:
        #name, datatype pairs
        temp_list.append(bigquery.SchemaField(tup[0],tup[1]))
    job_config.schema = temp_list

    #load data tables
    print(csvpath_str)
    table_str = ('').join(os.path.basename(csvpath_str).split('.')[0]).lower()
    print(table_str)
    table_ref = dataset_ref.table(table_str)

    with open(csvpath_str, 'rb') as source_file:
        job = client.load_table_from_file(
            source_file,
            table_ref,
            location=location,  # Must match the destination dataset location.
            job_config=job_config)  # API request

    try: 
        job.result()  # Waits for table load to complete.
    except BadRequest as err:
        print(job.errors)
        raise
    print('Loaded {} rows into {}:{}.'.format(
        job.output_rows, dataset, table_str))


#split_one_line takes as input the WKT representation of the line to split and a comma-separated string of points
#at which to split it. The function returns a list of linestrings that are subsegments of the original line 
def split_one_line(pts_str, line_str):
    pts_list = [s.replace('POINT(','').replace(')','') for s in pts_str.split(',')]
    b = line_str.replace('LINESTRING(','').replace(')','')
    lines_list = []
    for pt in pts_list:
        try:
            a,b=b.split(pt)
            if len(a) > 0:
                lines_list.append('LINESTRING('+a+pt+')')
            if len(b) > 0:
                b = pt+b
            else:
                break
        except ValueError:
            print(pt, b)
            break
    if len(b) > 0:
        lines_list.append('LINESTRING('+b+')')
    #print(lines_list)
    return lines_list

#approximate conversion of latitude and longitude in decimal degrees to x,y coordinates in m
#x,y coordinates are also known as easting and northing in some datasets
def lonlat2xy(lon,lat):
    return lon*111111*np.cos(lat),lat*111111

#determine whether a point is on the line defined by coordinate pairs a and b and between a and b
def is_on_segment(pt,a,b):
    x1,y1 = lonlat2xy(a.lon,a.lat)
    x2,y2 = lonlat2xy(b.lon,b.lat)
    xhat,yhat = lonlat2xy(pt.lon,pt.lat)
    theta = np.arctan2((y2-y1),(x2-x1))
    thetahat = np.arctan2((yhat-y1),(xhat-x1))
    r=np.sqrt((x2-x1)**2+(y2-y1)**2)
    rhat=np.sqrt((xhat-x1)**2+(yhat-y1)**2)
    if rhat <= r and abs(thetahat-theta) < 0.01:
        ison = True
    else: 
        ison = False
        #print(rhat,r,thetahat,theta)
    return ison

#returns true for linestrings that have non-zero length
def is_nonzero_linestring(vlist):
    if len(vlist)>2 or (len(vlist)==2 and vlist[0]!=vlist[1]):
        isnonzero = True
    else:
        isnonzero = False
    return isnonzero

#split_one_line2 is an improved version that takes as input the WKT representation of the line to split,
#a comma-separated string of points at which to split it, and any other fields that should be inherited 
#by the new split lines. The function returns a list of linestrings that are subsegments of the original line 
#This version better handles whitespace and unusual WKT formatting than its predecessor 
def split_one_line2(pts_str, line_str,other_list):
    #other_list is list of additional fields that should be included with each new line segment
    lines_list = []
    Point = coll.namedtuple('Point', ['lon', 'lat'])
    if not pts_str:
        #pass the linestring straight through, add unique id based on time, add other parameters
        lines_list.append([line_str,uuid.uuid4()]+other_list) 
    else:
        #desired line endpoints, split string and strip any extra whitespace
        pts_list = [s.replace('POINT(','').replace(')','').strip() for s in pts_str.split(',')]
        #existing vertices, split string and strip any extra whitespace
        vertices_list = [t.strip() for t in line_str.replace('LINESTRING(','').replace(')','').split(',')]
        for p in pts_list:
            #print(p)
            #change string representation of p to point structure representation
            pt = Point(*[float(v) for v in p.strip().split(' ')])
            for i in range(len(vertices_list)-1):
                #print(vertices_list)
                v1 = Point(*[float(dd) for dd in vertices_list[i].strip().split(' ')]) 
                v2 = Point(*[float(dd) for dd in vertices_list[i+1].strip().split(' ')]) 
                if is_on_segment(pt,v1,v2):
                    #make linestring between v1 and pt
                    tmp_list = vertices_list[:i+1]+[p]
                    if is_nonzero_linestring(tmp_list):
                        lines_list.append(['LINESTRING('+','.join(tmp_list)+')',uuid.uuid4()]+other_list)
                    #update vertices_list
                    vertices_list = [p]+vertices_list[i+1:]
                    break
        #when no more points, write the last linestring with whatever vertices remain
        #if there are more than 2 vertices or the first and last are not equal
        if is_nonzero_linestring(vertices_list):
            lines_list.append(['LINESTRING('+','.join(vertices_list)+')',uuid.uuid4()]+other_list)
    return lines_list

#split_at_gridded_points is a top-level call, makes use of generate_30m_points, split_one_line2
#Returns a shape file with the new split lines, a table in BQ, and csvs with the split points and lines
def split_at_gridded_points(client, data, outfc, geom_field, uniqueid, otherfield_str,location, job_config=bigquery.QueryJobConfig()):
    #return list of new split linestrings
    #wkt_list will have one column of linestrings, a unique id, and optional additional attributes carried along 
    wkt_list = []
    #save the generated points and split segments to csv for debugging
    csvgenpts_str = os.path.join(os.path.dirname(outfc),'genpts.csv')
    dstbin_str = data['ds']+'.'+data['tb']
    tbout_str = data['tb']+"_split30m"
    csvsplit_str = os.path.join(os.path.dirname(outfc),tbout_str+'.csv')
    with open(csvgenpts_str,'wb') as fgen:
        fgen_writer=csv.writer(fgen,delimiter=',',quotechar='"')
        #generate the points
        for row in generate_30m_points(client,dstbin_str,geom_field,uniqueid,otherfield_str,location,job_config):
            fgen_writer.writerow(row)
            #split the segments
            wkt_list.extend(split_one_line2(row.pts_str,row.geom_str,list(row[2:])))
    with open(csvsplit_str,'wb') as fsplit:
        fsplit_writer=csv.writer(fsplit,delimiter=',',quotechar='"')
        for row in wkt_list:
            fsplit_writer.writerow(row)
    print("Split segments at points.")
    
    #make shapefile with new segments, giving each a unique id
    other_list = otherfield_str.split(',')
    fieldin_list = ["geowkt_text","segid"]+other_list
    fieldout_list = ["SHAPE@","segid"]+other_list
    typeout_list = ["geometry","TEXT"]+["TEXT"]*len(other_list)
    order_list = [0,1]+list(range(2,len(other_list)+2))
    fc = arh.wkt2shp(wkt_list, outfc,fieldin_list, fieldout_list,typeout_list,order_list)
    print("Saved to shapefile")
    #make BQ table
    schema_list = [('geowkt','GEOGRAPHY'),('segid','STRING')]+[(a,'STRING') for a in other_list]
    wkt2bq(csvsplit_str, client, data['ds'], schema_list, rowskip=0, location=location)
    print("Saved table {0} to BQ".format(tbout_str))
    return fc

#generate_30m_points determines lat,lon pairs at which to split lines with roughly 30 m spacing (targeting 20 to 40 m).
#Expected inputs are source table in BQ with linestring geometry in geom_field and unique segment id in uniqueid
#Source geometry should be intersection to intersection segments
def generate_30m_points(client, tb, geom_field, uniqueid, other_fields, location='US',job_config=bigquery.QueryJobConfig()):
    sql_str=("""
    with cte0 as (
    --figure out how many points we need to generate given the segment length and desired grid size (20-40 m)
    select {1}, {2}, {3}
    , ST_ASTEXT({1}) as geom_str
    , CAST(SPLIT(REGEXP_EXTRACT_ALL(ST_ASTEXT({1}),r'[^,A-Z\(\)]+')[OFFSET(0)],' ')[OFFSET(0)] as NUMERIC) as x1_dd
    , CAST(SPLIT(REGEXP_EXTRACT_ALL(ST_ASTEXT({1}),r'[^,A-Z\(\)]+')[OFFSET(0)],' ')[OFFSET(1)] as NUMERIC) as y1_dd
    , CAST(SPLIT(ARRAY_REVERSE(REGEXP_EXTRACT_ALL(ST_ASTEXT({1}),r'[^,A-Z\(\)]+'))[OFFSET(0)],' ')[OFFSET(1)] as NUMERIC) as x2_dd
    , CAST(SPLIT(ARRAY_REVERSE(REGEXP_EXTRACT_ALL(ST_ASTEXT({1}),r'[^,A-Z\(\)]+'))[OFFSET(0)],' ')[OFFSET(2)] as NUMERIC) as y2_dd
    , ST_LENGTH({1}) as geom_length
    , IF(ST_LENGTH({1})/30.0 < 1, 1, IF(MOD(CAST(ST_LENGTH({1}) as NUMERIC),30.0) < 0.666, TRUNC(ST_LENGTH({1})/30.0), ROUND(ST_LENGTH({1})/30.0))) as npts
    from {0}
    --where LINEARID = 1104485206984
    --where fid = 132580
    )
    , cte1 as (
    --find the angle of the tangent line between the start and end points of the segment in a cartesian coordinate system
    --this works well as long as the segments are not nearly closed or closed circles
    select ATAN2(((y2_dd-y1_dd)*111111),((x2_dd-x1_dd)*111111*COS(y1_dd*ACOS(-1)/180.0))) as theta
    , x1_dd, y1_dd
    , geom_length/npts as increment_m --radial increment
    , GENERATE_ARRAY(geom_length/npts,geom_length-geom_length/npts,geom_length/npts) as r_meters_arr --radius of each generated point along tangent line
    , {1}, {2}
    from cte0
    )
    , cte2 as (
    select --theta, x1_dd, y1_dd, 
     r_m,
     ST_ASTEXT(ST_CLOSESTPOINT({1}, ST_GEOGPOINT(--x,y 
        (1/(111111*COS(y1_dd*ACOS(-1)/180)))*r_m*COS(theta)+x1_dd  --convert back to dd and add x1
       ,(1/111111)*r_m*SIN(theta)+y1_dd) --convert back to dd and add y1
      )) as closestpt --find the closest point on the line to the generate point
    , {2}
    from cte1,
    UNNEST(r_meters_arr) r_m
    group by closestpt, {2}, r_m
    )
    select geom_str
    , ARRAY_TO_STRING(ARRAY_AGG(closestpt order by r_m),',') as pts_str --these are the points to split on, they already exist as vertices of the segment
    , {3}
    from cte0 LEFT JOIN cte2 on cte0.{2} = cte2.{2}
    group by geom_str, {3}""".format(tb,geom_field,uniqueid,other_fields))
    job = client.query(sql_str,location=location,job_config=job_config)
    print("Generated points.")
    return job.result()
