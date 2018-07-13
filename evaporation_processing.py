import fnmatch,os
import tarfile


def untar(fname):
    tar = tarfile.open(fname)
    tar.extractall()
    tar.close()


rootPath = "Evap"
pattern = '*.tar'
for root, dirs, files in os.walk(rootPath):
    for filename in fnmatch.filter(files, pattern):
        print(os.path.join(root, filename))
        untar(os.path.join(root, filename))


import pandas as pd

df = pd.DataFrame( columns = ['LAT', 'LON', 'AMT', 'ID', 'ST', 'CITY', 'ELEV', 'TIME', 'YEAR', 'MONTH', 'DAY'] ) 

for file in os.listdir():
	if ".txt" in file:
		print (file)
		t = pd.read_csv(file)
		year = file[9:13]
		month = file[13:15]
		day = file[15:17]
		if t.columns.shape[0] == 7:
			t.columns = ['LAT', 'LON', 'AMT', 'ID', 'ST', 'CITY', 'ELEV']
			t['TIME'] = 'N/A'
		else:
			t.columns = ['LAT', 'LON', 'AMT', 'ID', 'ST', 'CITY', 'ELEV', 'TIME']  
		t['YEAR'] = year
		t['MONTH'] = month
		t['DAY'] = day
		df = df.append(t)

gb_sum = df[ ['LAT', 'LON', 'AMT', 'YEAR'] ].groupby( ['LAT', 'LON', 'YEAR'] ).sum().add_suffix('_Sum').reset_index()
gb_avg = gb_sum[ ['LAT', 'LON', 'AMT_Sum', 'YEAR'] ].groupby( ['LAT', 'LON'] ).mean().add_suffix('_Ave').reset_index()

gb_avg.AMT_Sum_Ave = gb_avg.AMT_Sum_Ave * 0.01
gb_avg.to_csv('usa_annual_evaporation_compiled.csv', sep=',')

