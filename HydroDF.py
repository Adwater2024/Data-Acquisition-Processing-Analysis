from pynhd import NLDI, WaterData, NHDPlusHR, GeoConnex
import geopandas as gpd
import pandas as pd
from supporting_scripts import getData, SNOTEL_Analyzer, dataprocessing, mapping
from shapely.geometry import box, Polygon
import os
import datetime
import matplotlib.pyplot as plt
import numpy as np
import warnings
warnings.filterwarnings("ignore")

nldi = NLDI()
usgs_gage_id = "10020100" # NWIS id for Bear river at the inlet of Woodruff Narrows Reservoir
WY = 2024 # Water Year to analyze. A water year is defined as the 12 month period from October 1st to September 30th.

basinname ='BearRiverBasin'

print('Collecting basins...', end='')
basin = nldi.get_basins(usgs_gage_id)
if not os.path.exists('files'):
    os.makedirs('files')
basin.to_file(f"files/{basinname}.shp")
print('done')

site_feature = nldi.getfeature_byid("nwissite", f"USGS-{usgs_gage_id}")
upstream_network = nldi.navigate_byid(
    "nwissite", f"USGS-{usgs_gage_id}", "upstreamMain", "flowlines", distance=9999
)

# Create geodataframe of all stations
all_stations_gdf = gpd.read_file('https://raw.githubusercontent.com/egagli/snotel_ccss_stations/main/all_stations.geojson').set_index('code')
all_stations_gdf = all_stations_gdf[all_stations_gdf['csvData']==True]

# Use the polygon geometry to select snotel sites that are within the domain
gdf_in_bbox = all_stations_gdf[all_stations_gdf.geometry.within(basin.geometry.iloc[0])]

#reset index to have siteid as a column
gdf_in_bbox.reset_index(drop=False, inplace=True)

#make begin and end date a str
gdf_in_bbox['beginDate'] = [datetime.datetime.strftime(gdf_in_bbox['beginDate'][i], "%Y-%m-%d") for i in np.arange(0,len(gdf_in_bbox),1)]
gdf_in_bbox['endDate'] = [datetime.datetime.strftime(gdf_in_bbox['endDate'][i], "%Y-%m-%d") for i in np.arange(0,len(gdf_in_bbox),1)]
gdf_in_bbox

# Use the getData module to retrieve data 
OutputFolder = 'files/SNOTEL'
if not os.path.exists(OutputFolder):
    os.makedirs(OutputFolder)

for i in gdf_in_bbox.index:
    getData.getSNOTELData(gdf_in_bbox.name[i], gdf_in_bbox.code[i], 'UT', gdf_in_bbox.beginDate[i], gdf_in_bbox.endDate[i], OutputFolder)
    
watershed = "Bear River Basin"
AOI = 'Above Woodruff Narrows Reservoir'
DOI = '04-01' #must be in MM-DD form
SNOTEL_Analyzer.SNOTELPlots(sitedict, gdf_in_bbox, WY, watershed, AOI,DOI)

SNOTEL_Analyzer.catchmentSNOTELAnalysis(sitedict, WY, watershed, AOI, DOI)

cleaned = dataprocessing.clean_nwis_dataframe(streamflow)
#set the index name to Date
cleaned.index.name = "Date"

cleaned['flow_cfs'] = cleaned['flow_cfs'] * 0.0283168
cleaned.rename(columns={'flow_cfs': 'flow_cms'}, inplace=True)

fig, ax1 = plt.subplots(figsize=(6, 6))

# --- Primary Y-axis: Streamflow ---
ax1.set_xlabel('Date', fontsize=12)
ax1.set_ylabel('Streamflow (cms)', color='tab:blue', fontsize=12, fontweight='bold')
ax1.plot(cleaned.index, cleaned['flow_cms'], color='tab:blue', label='Streamflow', linewidth=2)
ax1.tick_params(axis='y', labelcolor='tab:blue')
ax1.grid(True, alpha=0.3)

# Title and Layout
plt.title('Streamflow at USGS gage: ' + usgs_gage_id, fontsize=14, pad=15)
fig.tight_layout()
plt.show()

#plot the relationship between SWE_cm and flow_cms
#For year 2019, plot all SWE_cm columns
df = Hydro_df.loc['2019-10-01':'2020-09-30'].copy()

#select all columns with 'SWE_cm' and 'flow_cms' in the name
df = df.loc[:, df.columns.str.contains('SWE_cm') | df.columns.str.contains('flow_cms')]

#get colum names that contain SWE_cm
swe_cols = df.columns[df.columns.str.contains('SWE_cm')]

#make the plot
fig, ax1 = plt.subplots(figsize=(6, 6))

# --- Primary Y-axis: SWE_cm ---
ax1.set_xlabel('Date', fontsize=12)
ax1.set_ylabel('SWE (cm)', color='darkorange', fontsize=12, fontweight='bold')
for swe in swe_cols:    
    ax1.plot(df.index, df[swe], linewidth=2, label = swe)

ax1.plot(df.index, df.flow_cms, color='blue', linewidth=2, label='Streamflow') 
ax1.tick_params(axis='y', labelcolor='darkorange')
ax1.grid(True, alpha=0.3)
#make second axis for streamflow
ax2 = ax1.twinx()
ax2.set_ylabel('Streamflow (cms)', color='blue', fontsize=12, fontweight='bold')
ax2.tick_params(axis='y', labelcolor='blue')

#show a legend
ax1.legend()

# Title and Layout
plt.title('SSWE and Streamflow for water year 2019 at USGS gage: ' + usgs_gage_id, fontsize=14, pad=15)
fig.tight_layout()
plt.show()