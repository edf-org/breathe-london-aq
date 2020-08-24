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

import ggplot as gg
import pandas as pd
import datetime as dt
import sys
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()

species = 'no2'
df = pd.read_csv(r'.\charts\background_data_melted.csv',index_col='idx',dtype={'timestamp':'str','vidperiod':'str','type':'str','param':'str','value':'float64'})
print(df[:10])
df['timestamp'] = pd.to_datetime(df['timestamp'],format="%Y-%m-%d %H:%M:%S")
#plots
plt1 = gg.ggplot(df, gg.aes(x='timestamp',y='value',color='type'))+gg.geom_line()+gg.xlab('Time')+gg.ylab('Concentration')+gg.theme_bw()+gg.ylim(0,100)+gg.facet_wrap('vidperiod',scales='free')+gg.ggtitle('Regional background comparison {0}'.format(species))
#+gg.theme(axis_text_x=gg.element_text(angle=20))
plt1.save(filename = r'.\charts\background_{0}_ggtest_{1}.png'.format(species,dt.datetime.today().strftime('%Y%b%d')), width=None, height=None, dpi=300)

