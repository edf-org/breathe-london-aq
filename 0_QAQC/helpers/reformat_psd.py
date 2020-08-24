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

import pandas as pd

csvin = r'C:\Users\lpadilla\Downloads\PSD_20190420_pcm3.txt'
csvout = 'out.csv'

col_list = list(range(0,84)) #first 84 cols only, ignoring cumulative data columns
df = pd.read_csv(csvin,header=None,names=col_list,usecols=col_list,skiprows=8,sep='\t')[col_list]
print(df.head())
df.to_csv(csvout,index=False)
