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

from google.cloud import bigquery
from google.oauth2 import service_account
import ggplot as gg
import matplotlib.pyplot as plt
import pandas as pd
import datetime as dt
import numpy as np
import sys
sys.path.append(r".\helpers")
import background_helpers as bh
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()

credentials = service_account.Credentials.from_service_account_file(r"..\..\pks\street-view-air-quality-london-dc8f329b26cf.json")
project_id = 'street-view-air-quality-london'
bqclient = bigquery.Client(credentials=credentials,project=project_id)

#species = 'no2'
species = 'pm_pdr'
if species == 'no2':
    unit = 'ppb'
    meas = 'NO2'
elif species == 'pm_pdr':
    unit = 'ug/m3'
    meas = 'PDR PM 2.5'

#run query
qry_str = bh.background_testdays_str(species)
qry_job = bqclient.query(qry_str,location='EU',job_config=bigquery.QueryJobConfig())
#save result as dataframe
df = qry_job.to_dataframe()
#write dataframe to csv for later use
df.to_csv(r'.\charts\background_{0}_data.csv'.format(species),index=False)
#df = pd.read_csv(r'.\charts\background_{0}_data.csv'.format(species),parse_dates=['timestamp'])
print(df.dtypes)

#plots
#get unique periods
period_list = df['vidperiod'].unique()
#matplotlib approach
for m in range(4):
	plt.rcParams.update({'font.size':14})
	fig,axes = plt.subplots(3,1,figsize=(18,12))
	for i,p in enumerate(period_list[3*m:min(3*(m+1),len(period_list))]):
		#ax = axes[int(i/4),np.mod(i,4)]
		ax = axes[i]
		df_tmp = df[(df['vidperiod']==p) & (~df['type'].isin(['AQMesh','LAQN']))]
		ax.plot(df_tmp.timestamp,df_tmp['value'],'k')
		ax.plot(df_tmp.timestamp,df_tmp['background_value'],'b')
		df_tmp = df[(df['vidperiod']==p) & (df['type']=='AQMesh')]
		ax.plot(df_tmp.timestamp,df_tmp['background_value'],'r')
		df_tmp = df[(df['vidperiod']==p) & (df['type']=='LAQN')]
		ax.plot(df_tmp.timestamp,df_tmp['background_value'],'g')
		ax.set_ylim(0,100)
	axes[-1].legend(['mobile 1 hz','mobile bkgd','AQMesh bkgd','LAQN bkgd'],bbox_to_anchor=(0.,-0.2,1,0.2),loc='lower left',ncol=4,mode='expand',borderaxespad=0.)
	axes[0].set_title('{0} ({1}) background comparison\n'.format(meas,unit))

	fig.tight_layout()
	#plt1.show()
	#print(plt1)
	fig.savefig(r'.\charts\background_{0}_{1}_{2}.png'.format(species,m,dt.datetime.today().strftime('%Y%b%d')),dpi=300)
