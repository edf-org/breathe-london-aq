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

#create hex to status lookup table
import numpy as np
import pandas as pd
import itertools as it
import sys
import os
sys.path.append('..\helpers')
from qaqchelpers_2019Feb5 import hex2bin, bin2bits

data_path_str = r"..\..\..\..\Data\Intermediate\QAQC"
dev_path_str = os.path.join(data_path_str,"devices.csv")
status_path_str = os.path.join(data_path_str,"statuses.csv")
out_path_str = os.path.join(data_path_str, "hexstatus_lookup.csv")
f = open(out_path_str,'w')
f.write('dev_id,status,bin_str,status_flag,qc_flag,status_desc\n')

devices_df = pd.read_csv(dev_path_str)
status_df = pd.read_csv(status_path_str)

concatlist = lambda s:(';').join(s)

for row in devices_df.itertuples(index=False):
    dev_id = int(row[0])
    status_type = row[4]
    inv_bool = False
    print("Processing {} status codes".format(row[1]))
    if status_type == 'hex0' or status_type == 'hex1':
        hexstart = int(row[2])
        hexlen = int(row[3])
        #format string: {leading zero bits and non-zero bits}{trailing zero bits}
        if hexstart > 0:
            fmt_str = '{0:0'+str(8-hexstart)+'x}'+'{1:0'+str(hexstart)+'x}'
        else:
            fmt_str = '{0:08x}'
        if status_type == 'hex0':
            inv_bool = True

        for i in range(1,16**hexlen):
            hex_str = fmt_str.format(i,0)
            bin_str = hex2bin(hex_str)
            bit_list = bin2bits(bin_str,inverse=inv_bool)
            #print(hex_str,bin_str,bit_list)
            #get corresponding status
            selected_df = ((status_df['bit'].isin(bit_list)) & (status_df['dev_id']==dev_id))
            temp_df = status_df[selected_df]
            if len(temp_df) > 0:
                #aggregate descriptions and flags
                out_df = temp_df.groupby('dev_id').agg({'status_flag':np.max,'qc_flag':np.max, 'bit_desc':concatlist})
                #print(out_df)
                f.write('{0},{1},{2},{3},{4},{5}\n'.format(dev_id,hex_str,bin_str,out_df.status_flag.item(),out_df.qc_flag.item(),out_df.bit_desc.item()))

    elif status_type == 'bit':
        a = range(4)
        b = range(3)
        c = range(2)
        d = range(5)
        e = range(6)
        lilstatus_df = status_df[status_df['dev_id']==dev_id]
        for combi in it.product(e,d,c,b,a):
            selected_df = ( ((lilstatus_df['bit']==4) & (lilstatus_df['bit_value']==combi[4]))\
                    |  ((lilstatus_df['bit']==3) & (lilstatus_df['bit_value']==combi[3]))\
                    |  ((lilstatus_df['bit']==2) & (lilstatus_df['bit_value']==combi[2]))\
                    |  ((lilstatus_df['bit']==1) & (lilstatus_df['bit_value']==combi[1]))\
                    |  ((lilstatus_df['bit']==0) & (lilstatus_df['bit_value']==combi[0]))  )
            temp_df = lilstatus_df[selected_df]
            #aggregate descriptions and flags
            if len(temp_df) > 0:
                out_df = temp_df.groupby('dev_id').agg({'status_flag':np.max,'qc_flag':np.max, 'bit_desc':concatlist})
                f.write('{0},{1},0,{2},{3},{4}\n'.format(dev_id,'{4}{3}{2}{1}{0}'.format(*combi),out_df.status_flag.item(),out_df.qc_flag.item(),out_df.bit_desc.item()))
f.close()
