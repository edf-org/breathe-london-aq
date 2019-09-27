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
from qaqchelpers_2019Feb5 import hex2bin, bin2bits

dev_path_str = r"..\..\..\..\Data\Intermediate\results-20190214-172304.csv"
status_path_str = r"..\..\..\..\Data\Intermediate\statuses.csv"
out_path_str = r"..\..\..\..\Data\Intermediate\status_summary_2019Feb12.csv"
f = open(out_path_str,'w')
f.write('vid,dev_id,duration(h),status,bin_str,status_flag,qc_flag,status_desc\n')

devices_df = pd.read_csv(dev_path_str)
status_df = pd.read_csv(status_path_str)

concatlist = lambda s:(';').join(s)

for row in devices_df.itertuples(index=False):
    dev_id = int(row[2])
    duration_h = int(row[3])/3600.
    inv_bool = False
    print("Processing {} status codes".format(row[1]))
    if dev_id in range(1,7):
        if dev_id == 3:
            inv_bool = True
        hex_str = row[1]
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
            f.write('{0},{1},{2},{3},{4},{5},{6},{7}\n'.format(row[0],dev_id,duration_h,hex_str,bin_str,out_df.status_flag.item(),out_df.qc_flag.item(),out_df.bit_desc.item()))

    elif dev_id in [7]:
        lilstatus_df = status_df[status_df['dev_id']==dev_id]
        combi = row[1][::-1]
        selected_df = ( ((lilstatus_df['bit']==4) & (lilstatus_df['bit_value']==int(combi[4])))\
                |  ((lilstatus_df['bit']==3) & (lilstatus_df['bit_value']==int(combi[3])))\
                |  ((lilstatus_df['bit']==2) & (lilstatus_df['bit_value']==int(combi[2])))\
                |  ((lilstatus_df['bit']==1) & (lilstatus_df['bit_value']==int(combi[1])))\
                |  ((lilstatus_df['bit']==0) & (lilstatus_df['bit_value']==int(combi[0])))  )
        temp_df = lilstatus_df[selected_df]
        #aggregate descriptions and flags
        if len(temp_df) > 0:
            out_df = temp_df.groupby('dev_id').agg({'status_flag':np.max,'qc_flag':np.max, 'bit_desc':concatlist})
            f.write('{0},{1},{2},{3},,{4},{5},{6}\n'.format(row[0],dev_id,duration_h,row[1],out_df.status_flag.item(),out_df.qc_flag.item(),out_df.bit_desc.item()))
f.close()
