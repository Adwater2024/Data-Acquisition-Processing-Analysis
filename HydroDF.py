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
WY = 2020 # Water Year to analyze. A water year is defined as the 12 month period from October 1st to September 30th.

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

mapping.basin_mapping(basin, site_feature)

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

mapping.snotel_mapping(gdf_in_bbox, basin, site_feature)

# Use the getData module to retrieve data 
OutputFolder = 'files/SNOTEL'
if not os.path.exists(OutputFolder):
    os.makedirs(OutputFolder)

for i in gdf_in_bbox.index:
    getData.getSNOTELData(gdf_in_bbox.name[i], gdf_in_bbox.code[i], 'UT', gdf_in_bbox.beginDate[i], gdf_in_bbox.endDate[i], OutputFolder)


begin = gdf_in_bbox.beginDate.max()
end = gdf_in_bbox.endDate.min()
streamflow = getData.get_usgs_streamflow(usgs_gage_id, begin, end)

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

#clip the dataframe to show one single year
start = '2020-10-01'
end = '2021-09-30'
WY_df = cleaned.loc[start:end]

#make the plot
fig, ax1 = plt.subplots(figsize=(6, 6))

# --- Primary Y-axis: Streamflow ---
ax1.set_xlabel('Date', fontsize=12)
ax1.set_ylabel('Streamflow (cms)', color='tab:blue', fontsize=12, fontweight='bold')
ax1.plot(WY_df.index, WY_df['flow_cms'], color='tab:blue', label='Streamflow', linewidth=2)
ax1.tick_params(axis='y', labelcolor='tab:blue')
ax1.grid(True, alpha=0.3)

# Title and Layout
plt.title('Streamflow for water year 2020 at USGS gage: ' + usgs_gage_id, fontsize=14, pad=15)
fig.tight_layout()
plt.show()

import earthaccess
from pynhd import NLDI
import pydaymet as daymet

# Authenticate with NASA
earthaccess.login(persist=True)

#Get geometry and ensure CRS is correct
basin = NLDI().get_basins(usgs_gage_id)
geometry_centroid = basin.to_crs("EPSG:4326").geometry[0].centroid
centroid = (geometry_centroid.x, geometry_centroid.y)

var = ["prcp", "tmin", "tmax"]
dates = (cleaned.index[0].strftime('%Y-%m-%d') , cleaned.index[-1].strftime('%Y-%m-%d') ) # Use the streamflow to determine dates

#Fetch data - authentication now happens automatically via earthaccess/.netrc
# Try this simplified call first
met_df = daymet.get_bycoords(centroid, dates, variables=["prcp", "tmin", "tmax"])

#load snotel data
unprocessed_SNOTEL = {}
#read all files in the following path into the dictionary
path = 'files/SNOTEL'
for filename in os.listdir(path):
    if filename.endswith('.csv'):
        #select the name of the file between the _ and _
        name = filename.split('_')[1] 
        unprocessed_SNOTEL[name] = pd.read_csv(os.path.join(path, filename))
        #make the date a datetime object and set to the index
        unprocessed_SNOTEL[name]['Date'] = pd.to_datetime(unprocessed_SNOTEL[name]['Date'])
        unprocessed_SNOTEL[name].set_index('Date', inplace=True)
        #rename the Snow Water Equivalent (m) Start of Day Values to SWE_cm
        unprocessed_SNOTEL[name].rename(columns={'Snow Water Equivalent (m) Start of Day Values': f"{name}_SWE_cm"}, inplace=True)
        #convert SWE_m to cm
        unprocessed_SNOTEL[name][f"{name}_SWE_cm"] = unprocessed_SNOTEL[name][f"{name}_SWE_cm"] * 100
        #remove the Water_Year column
        unprocessed_SNOTEL[name].drop(columns=['Water_Year'], inplace=True)
        #we need to know how many obs for each DF, print the df name, its length, and the start/end dates
        print(f"{name}: {len(unprocessed_SNOTEL[name])} start date: {unprocessed_SNOTEL[name].index.min()} end date: {unprocessed_SNOTEL[name].index.max()}")

        #The TES site is missing many values and will not be useful for our analysis, remove it
unprocessed_SNOTEL.pop('TES', None)

#The site with the latest start date will guide the rest
latest_start_date = max([df.index.min() for df in unprocessed_SNOTEL.values()])

#The site with the earliest end date will guide the rest
soonest_end_date = min([df.index.max() for df in unprocessed_SNOTEL.values()])
for key in unprocessed_SNOTEL.keys():
    unprocessed_SNOTEL[key] = unprocessed_SNOTEL[key][unprocessed_SNOTEL[key].index >= latest_start_date]
    unprocessed_SNOTEL[key] = unprocessed_SNOTEL[key][unprocessed_SNOTEL[key].index <= soonest_end_date]

#merge all dictionary dataframes into one larger dataframe
SNOTEL_df = pd.concat(unprocessed_SNOTEL.values(), axis=1)

SNOTEL_df.head()

#find the latest start date and the earliest end date for SNOTEL_df, met_df, cleaned
begin_date = max([df.index.min() for df in [SNOTEL_df, met_df, cleaned]])
end_date = min([df.index.max() for df in [SNOTEL_df, met_df, cleaned]])

#clip each dataframe to have the same begin and end dates
SNOTEL_df = SNOTEL_df[(SNOTEL_df.index >= begin_date) & (SNOTEL_df.index <= end_date)]
met_df = met_df[(met_df.index >= begin_date) & (met_df.index <= end_date)]
cleaned = cleaned[(cleaned.index >= begin_date) & (cleaned.index <= end_date)]

#merge the SNOTEL_df, met_df, and streamflow dataframes
Hydro_df = pd.concat([SNOTEL_df, met_df, cleaned], axis=1)
Hydro_df.head(50)

#all of the NaN values here should be 0, fill them
Hydro_df = Hydro_df.fillna(0)
Hydro_df.head(50)

#take a look around peak SWE to make sure we have snotel values, early season can be tricky to assess
Hydro_df.loc['2020-03-01':'2021-04-01']

#For year 2019, plot all SWE_cm columns
SWE_df = Hydro_df.loc['2020-10-01':'2021-09-30'].copy()

#select all columns with 'SWE_cm' in the name
SWE_df = SWE_df.loc[:, SWE_df.columns.str.contains('SWE_cm')]

#plot
SWE_df.plot(figsize=(10, 6))


# RESERVOIR MANAGEMENT ANALYSIS
# Bear River Basin – Woodruff Narrows Reservoir
# USGS Gage: 10020100 | Water Year: 2025 (Oct 2020 – Sep 2021)
# Analysis Date of Interest: April 1, 2020

 
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.lines import Line2D
import warnings
warnings.filterwarnings("ignore")
 

USGS_GAGE_ID  = "10020100"
WY_TARGET     = 2020          # the water year you are reporting on
DOI_MMDD      = "04-01"       # date of interest (April 1)
WATERSHED     = "Bear River Basin"
AOI           = "Above Woodruff Narrows Reservoir"
 
# Months to analyse for streamflow (Sections 3 & 4)
MONTHS        = [4, 5, 6, 7, 8, 9]
MONTH_LABELS  = ["April", "May", "June", "July", "August", "September"]
 
 

# HELPER FUNCTIONS

 
def get_swe_cols(df):
    """Return list of SWE column names present in df."""
    return [c for c in df.columns if "_SWE_cm" in c]
 
 
def label_water_year(df):
    """Add a WaterYear column (Oct–Sep convention) to a copy of df."""
    df = df.copy()
    df["WaterYear"] = df.index.year.where(df.index.month < 10, df.index.year + 1)
    return df
 
 
def april1_swe(df, swe_col):
    """
    Return a Series indexed by water year with the SWE value on April 1
    (or nearest available date within ±3 days).
    """
    df = label_water_year(df)
    results = {}
    for wy, grp in df.groupby("WaterYear"):
        target = f"{wy}-{DOI_MMDD}"
        try:
            target_dt = pd.Timestamp(target)
            # look within a ±3-day window in case of missing data
            window = grp.loc[
                (grp.index >= target_dt - pd.Timedelta(days=3)) &
                (grp.index <= target_dt + pd.Timedelta(days=3))
            ]
            if not window.empty:
                results[wy] = window[swe_col].iloc[
                    abs((window.index - target_dt)).argmin()
                ]
        except Exception:
            pass
    return pd.Series(results, name=swe_col)
 
 
def peak_swe_by_year(df, swe_col):
    """Return a Series indexed by water year with the peak (max) SWE value."""
    df = label_water_year(df)
    return df.groupby("WaterYear")[swe_col].max()
 
 
def monthly_volume_cms_days(df, month):
    """
    For every water year, compute total volumetric streamflow for a given
    calendar month (cms × days-in-month).  Returns a Series indexed by WY.
    """
    df = label_water_year(df)
    monthly = df[df.index.month == month].copy()
    # sum daily flow_cms → cms·days (proxy for volume)
    return monthly.groupby("WaterYear")["flow_cms"].sum()
 
 
# SECTION 2 – SWE ANALYSIS
 
def plot_swe_historical_range(Hydro_df):
    """
    For each SNOTEL site, produce a figure showing:
      • Median daily SWE across all historical years (dark line)
      • 25th–75th percentile band
      • Min–max band
      • The target water year highlighted
      • A vertical dashed line on April 1
    Prints a summary table of April 1 SWE vs historical stats.
    """
    swe_cols = get_swe_cols(Hydro_df)
    if not swe_cols:
        print("No SWE columns found in Hydro_df.")
        return
 
    df = label_water_year(Hydro_df)
    # Build a "day of water year" axis (Oct 1 = day 1)
    df["DOWY"] = df.index.map(
        lambda d: (d - pd.Timestamp(
            year=(d.year if d.month >= 10 else d.year - 1),
            month=10, day=1
        )).days + 1
    )
 
    target_wy = WY_TARGET
    n_sites = len(swe_cols)
    fig, axes = plt.subplots(1, n_sites, figsize=(7 * n_sites, 5), sharey=False)
    if n_sites == 1:
        axes = [axes]
 
    summary_rows = []
 
    for ax, swe_col in zip(axes, swe_cols):
        site_name = swe_col.replace("_SWE_cm", "")
 
        # pivot: rows = DOWY, columns = water year
        pivot = df.pivot_table(index="DOWY", columns="WaterYear", values=swe_col)
        hist_pivot = pivot.drop(columns=[target_wy], errors="ignore")
 
        med   = hist_pivot.median(axis=1)
        p25   = hist_pivot.quantile(0.25, axis=1)
        p75   = hist_pivot.quantile(0.75, axis=1)
        vmin  = hist_pivot.min(axis=1)
        vmax  = hist_pivot.max(axis=1)
 
        # Day-of-water-year for April 1 (non-leap: day 183, leap: 184)
        doi_dowy = (pd.Timestamp(f"{target_wy}-04-01") -
                    pd.Timestamp(f"{target_wy - 1}-10-01")).days + 1
 
        ax.fill_between(pivot.index, vmin, vmax,
                        color="lightsteelblue", alpha=0.4, label="Min–Max range")
        ax.fill_between(pivot.index, p25, p75,
                        color="steelblue", alpha=0.5, label="25th–75th pct")
        ax.plot(pivot.index, med, color="navy", lw=2, label="Historical median")
 
        if target_wy in pivot.columns:
            ax.plot(pivot.index, pivot[target_wy],
                    color="darkorange", lw=2.5, label=f"WY{target_wy}")
 
        ax.axvline(doi_dowy, color="red", ls="--", lw=1.5,
                   label="April 1 DOI")
 
        ax.set_xlabel("Day of Water Year (Oct 1 = Day 1)", fontsize=11)
        ax.set_ylabel("SWE (cm)", fontsize=11)
        ax.set_title(f"{site_name} – Daily SWE\n{WATERSHED}", fontsize=12)
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)
 
        # --- Summary stats ---
        apr1_hist = april1_swe(
            Hydro_df[Hydro_df.index.year.map(
                lambda y: (y if Hydro_df.loc[Hydro_df.index.year == y].index[0].month >= 10
                           else y) != target_wy
            )], swe_col
        )  # all years except target
        apr1_all  = april1_swe(Hydro_df, swe_col)
        target_val = apr1_all.get(target_wy, np.nan)
        hist_vals  = apr1_all.drop(index=target_wy, errors="ignore")
 
        summary_rows.append({
            "Site": site_name,
            f"April 1 WY{target_wy} SWE (cm)": round(target_val, 1),
            "Historical Median (cm)":           round(hist_vals.median(), 1),
            "Historical Mean (cm)":             round(hist_vals.mean(), 1),
            "Pct of Median (%)":                round(100 * target_val / hist_vals.median(), 1)
                                                if hist_vals.median() != 0 else np.nan,
        })
 
    fig.suptitle(f"Section 2 – Historical SWE Range by SNOTEL Site\n"
                 f"{AOI} | April 1, {target_wy} highlighted",
                 fontsize=13, fontweight="bold", y=1.02)
    plt.tight_layout()
    plt.savefig("Section2_SWE_historical_range.png", dpi=150, bbox_inches="tight")
    plt.show()
    print("Figure saved → Section2_SWE_historical_range.png\n")
 
    # Print summary table
    summary_df = pd.DataFrame(summary_rows)
    print("=" * 60)
    print(f"Table 1 – April 1, {target_wy} SWE vs Historical Statistics")
    print("=" * 60)
    print(summary_df.to_string(index=False))
    print()
    return summary_df
 
 

# SECTION 3 – MONTHLY STREAMFLOW VOLUME SUBPLOTS (2×3)

 
def plot_monthly_streamflow_boxplots(Hydro_df):
    """
    2×3 grid of box-and-whisker plots showing the historical distribution of
    monthly streamflow volume (cms·days) for April–September.
    The WY_TARGET value is overlaid as a red star.
    """
    fig, axes = plt.subplots(2, 3, figsize=(14, 8))
    axes_flat = axes.flatten()
 
    summary_rows = []
 
    for ax, month, label in zip(axes_flat, MONTHS, MONTH_LABELS):
        vol = monthly_volume_cms_days(Hydro_df, month)
 
        target_vol = vol.get(WY_TARGET, np.nan)
        hist_vol   = vol.drop(index=WY_TARGET, errors="ignore")
 
        # Box plot of historical values
        bp = ax.boxplot(
            hist_vol.dropna().values,
            vert=True,
            patch_artist=True,
            widths=0.5,
            boxprops=dict(facecolor="steelblue", alpha=0.6),
            medianprops=dict(color="navy", linewidth=2),
            whiskerprops=dict(color="steelblue"),
            capprops=dict(color="steelblue"),
            flierprops=dict(marker="o", color="gray", alpha=0.5, markersize=4),
        )
 
        # Overlay target year
        if not np.isnan(target_vol):
            ax.scatter([1], [target_vol], color="red", zorder=5,
                       s=120, marker="*", label=f"WY{WY_TARGET}")
 
        ax.set_title(label, fontsize=12, fontweight="bold")
        ax.set_ylabel("Volume (cms·days)", fontsize=10)
        ax.set_xticks([])
        ax.grid(True, axis="y", alpha=0.3)
        ax.legend(fontsize=9)
 
        summary_rows.append({
            "Month":                          label,
            f"WY{WY_TARGET} Volume (cms·d)":  round(target_vol, 1),
            "Historical Median (cms·d)":       round(hist_vol.median(), 1),
            "Historical Mean (cms·d)":         round(hist_vol.mean(), 1),
            "Pct of Median (%)":               round(100 * target_vol / hist_vol.median(), 1)
                                               if hist_vol.median() != 0 else np.nan,
        })
 
    fig.suptitle(
        f"Section 3 – Historical Monthly Streamflow Volume Distribution\n"
        f"USGS Gage {USGS_GAGE_ID} | {WATERSHED} | WY{WY_TARGET} shown as ★",
        fontsize=13, fontweight="bold"
    )
    plt.tight_layout()
    plt.savefig("Section3_monthly_streamflow_boxplots.png", dpi=150, bbox_inches="tight")
    plt.show()
    print("Figure saved → Section3_monthly_streamflow_boxplots.png\n")
 
    summary_df = pd.DataFrame(summary_rows)
    print("=" * 60)
    print(f"Table 2 – WY{WY_TARGET} Monthly Streamflow vs Historical")
    print("=" * 60)
    print(summary_df.to_string(index=False))
    print()
    return summary_df
 
 

# SECTION 4 – PEAK SWE vs MONTHLY STREAMFLOW PARITY PLOTS (2×3)

 
def plot_peakSWE_vs_streamflow(Hydro_df):
    """
    2×3 grid of scatter (parity) plots.
    X-axis: basin-average peak SWE (cm) for each water year
    Y-axis: total streamflow volume (cms·days) for the given month
    WY_TARGET is highlighted in red; all other years in blue.
    A linear regression line is drawn through historical points.
    """
    swe_cols = get_swe_cols(Hydro_df)
    if not swe_cols:
        print("No SWE columns found.")
        return
 
    # Compute basin-average peak SWE per water year
    peak_swe_by_site = {col: peak_swe_by_year(Hydro_df, col) for col in swe_cols}
    peak_swe_df = pd.DataFrame(peak_swe_by_site)
    basin_avg_peak = peak_swe_df.mean(axis=1)  # Series indexed by WY
 
    fig, axes = plt.subplots(2, 3, figsize=(14, 8))
    axes_flat = axes.flatten()
 
    for ax, month, label in zip(axes_flat, MONTHS, MONTH_LABELS):
        vol = monthly_volume_cms_days(Hydro_df, month)
 
        # Align on common water years
        common_wys = basin_avg_peak.index.intersection(vol.index)
        x_all = basin_avg_peak.loc[common_wys]
        y_all = vol.loc[common_wys]
 
        # Split into historical and target
        hist_mask   = common_wys != WY_TARGET
        x_hist = x_all[hist_mask]
        y_hist = y_all[hist_mask]
        x_tgt  = x_all[~hist_mask] if (~hist_mask).any() else pd.Series(dtype=float)
        y_tgt  = y_all[~hist_mask] if (~hist_mask).any() else pd.Series(dtype=float)
 
        # Scatter historical
        ax.scatter(x_hist, y_hist, color="steelblue", alpha=0.7,
                   edgecolors="navy", s=60, label="Historical years")
 
        # Annotate each historical year lightly
        for wy in x_hist.index:
            ax.annotate(str(wy), (x_hist[wy], y_hist[wy]),
                        fontsize=6, color="gray", ha="center", va="bottom")
 
        # Highlight target year
        if not x_tgt.empty:
            ax.scatter(x_tgt, y_tgt, color="red", s=150, zorder=5,
                       marker="*", label=f"WY{WY_TARGET}")
 
        # Linear regression on historical data
        if len(x_hist) > 2:
            m, b = np.polyfit(x_hist.values, y_hist.values, 1)
            x_line = np.linspace(x_hist.min(), x_hist.max(), 100)
            ax.plot(x_line, m * x_line + b, color="darkorange",
                    lw=1.8, ls="--", label=f"y={m:.1f}x+{b:.0f}")
            # Pearson r
            r = np.corrcoef(x_hist.values, y_hist.values)[0, 1]
            ax.text(0.05, 0.92, f"r = {r:.2f}", transform=ax.transAxes,
                    fontsize=9, color="darkorange")
 
        ax.set_xlabel("Basin-Avg Peak SWE (cm)", fontsize=10)
        ax.set_ylabel(f"{label} Volume (cms·d)", fontsize=10)
        ax.set_title(f"Peak SWE vs {label} Runoff", fontsize=11, fontweight="bold")
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)
 
    fig.suptitle(
        f"Section 4 – Peak SWE vs Monthly Streamflow Volume\n"
        f"{WATERSHED} | {AOI} | WY{WY_TARGET} highlighted in red ★",
        fontsize=13, fontweight="bold"
    )
    plt.tight_layout()
    plt.savefig("Section4_PeakSWE_vs_streamflow_parity.png", dpi=150, bbox_inches="tight")
    plt.show()
    print("Figure saved → Section4_PeakSWE_vs_streamflow_parity.png\n")

 
if __name__ == "__main__" or True:   # set True so it runs in a notebook too
 
    
    print("SECTION 2 – SWE Historical Range")
    
    sec2_summary = plot_swe_historical_range(Hydro_df)
 
    
    print("SECTION 3 – Monthly Streamflow Volume Distribution")
    
    sec3_summary = plot_monthly_streamflow_boxplots(Hydro_df)
 
   
    print("SECTION 4 – Peak SWE vs Monthly Streamflow Parity Plots")
    
    plot_peakSWE_vs_streamflow(Hydro_df)