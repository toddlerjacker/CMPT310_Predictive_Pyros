import pandas as pd

file = 'nasa_dataset.csv'

# Read the CSV file
df = pd.read_csv(file)

# Desired column order
new_order = ['DATE_TIME', 'STATION_CODE', 'STATION_NAME', 'lat', 'lon', 'DANGER_RATING', 'ELEVATION_M', 'EVI', 'NDVI', 'VI_Quality', 'evapotranspiration_mean', 'evapotranspiration_max', 'rootzone_wetness_mean', 'rootzone_wetness_max', 'surface_temp_mean', 'surface_temp_max', 'surface_wetness_mean', 'surface_wetness_max']

# Reorder the DataFrame
df = df[new_order]

df.to_csv(file, index=False)