import numpy as np
import os
import pandas as pd

Evaporation_Data = pd.DataFrame(columns=['lat', 'lon', 'ann_evap_ft'])

def parser(row):
	data_series = row[8:-1]
	data_array = []
	for i in np.linspace(0, 66, 12).astype(int):
		data_array.append(data_series[i:i+6])
	return np.array(data_array).astype(float)

rootdir ='/Volumes/LaCie/FPV_GIS/Evaporation/stations'
for subdir, dirs, files in os.walk(rootdir):
	for file in files:
		if file[-4:] == '.par':
			filename = os.path.join(subdir, file)
			print (filename)

			with open (filename, "r") as myfile:
			    data=myfile.readlines()

			lat, lon = float(data[1][7:14]), float(data[1][19:27])

			Tmax = parser(data[8])
			Tmin = parser(data[9])
			T = (Tmax + Tmin) / 2 # Average Temperature [F]
			T = (T - 32) * (5.0/9.0) # Average Temperature [C]

			Tdew = parser(data[15]) # Dew point temperature [F]
			Tdew = (Tdew - 32) * (5.0/9.0) # Dew point temperature [C]


			delta = ( 4098 * (0.6108 * np.exp( (17.27 * T) / (T + 237.3) ) ) ) / ( (T + 237.3)**2 ) # slope of vapor pressure-temperature curve [kPa/C]
			G = 0 # Soil heat flux ( assume zero for bodies of water ) [MJ/m2/day]
			gamma = 0.067 # psychometric constant [kPa/C]
			Rn = parser(data[12]) # Solar Radiation [langleys/day]
			Rn = Rn * 0.04184 # Solar Radiation [MJ/m2/day]


			time_pk_ind = np.linspace(17,77,16).astype(int) # indices of wind direction percent times
			mean_wind_ind = time_pk_ind + 1 # indices of mean wind speeds

			wind_dir_percents = np.empty(shape=(17,12)) # initialize array (16 wind directions + 1 calm X 12 months )
			wind_speeds = np.empty(shape=(17,12)) # initialize array (16 wind directions + 1 calm X 12 months )

			for i in range(0,16): # populate arrays from data
			    wind_dir_percents[i] = parser(data[time_pk_ind[i]])
			    wind_speeds[i] = parser(data[mean_wind_ind[i]])
			wind_dir_percents[16] = parser(data[81])
			wind_speeds[16] = np.zeros(shape=(1,12)) # account for calm conditions

			percent_time_wind = wind_dir_percents.sum(axis=0) # Check: Must add to 100 % in each month

			u2 = (wind_speeds * (wind_dir_percents/100)).sum(axis=0) # weighted average of all 16 wind directions [mph] (typo in cligen metadata)
			u2 = u2 * 0.44704 # average wind speeds [m/s]

			es = 0.611 * ( 10 ** ( (7.5*T) / (237.3 + T) ) ) # saturated vapor pressure [kPa]
			ea = 0.611 * ( 10 ** ( (7.5*Tdew) / (237.3 + Tdew) ) ) # actual vapor pressure [kPa]

			ETo = ( 0.408 * delta * (Rn - G) + gamma * ( 900 / (T + 273) ) * u2 * (es - ea) ) / (delta + gamma * (1 + 0.34 * u2)) # water volume evapotranspiration [mm/day]
			ETo = (ETo * 0.00328084) # [ft/day]
			num_days_in_month = np.array([31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])
			Annual_Evap = (ETo * num_days_in_month).sum() # should be about 7 feet per year

			Evaporation_Data.loc[len(Evaporation_Data)] = [lat, lon, Annual_Evap]


Evaporation_Data.to_csv("Penman_Montieth_Results.csv")



(4.024998-3.238978)/4.024998, (4.498008-3.448900)/4.498008, (5.465522-4.318932)/5.465522, (6.075280-4.666703)/6.075280, (7.267623-5.886043)/7.267623, (7.683970-6.032210)/7.683970