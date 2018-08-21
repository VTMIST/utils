import sys
sys.path.append('/usr/local/lib/python3.5/dist-packages/')
sys.path.append('/usr/local/lib/python3.5/dist-packages/tsyganenko/')
# sys.path.append('/home/shanec1/.local/lib/python3.5/site-packages/')
sys.path.append('/code/utils/')
import tsyganenko
import shared
import numpy as np
import pandas as pd
import aacgmv2
from datetime import datetime
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

def sun_pos(dt=None):
    if dt is None:
        dt = datetime.utcnow()

    axial_tilt = 23.4
    ref_solstice = datetime(2016, 6, 21, 22, 22)
    days_per_year = 365.2425
    seconds_per_day = 24 * 60 * 60.0

    days_since_ref = (dt - ref_solstice).total_seconds() / seconds_per_day
    lat = axial_tilt * np.cos(2 * np.pi * days_since_ref / days_per_year)
    sec_since_midnight = (dt - datetime(dt.year, dt.month, dt.day)).seconds
    lng = -(sec_since_midnight / seconds_per_day - 0.5) * 360
    return lat, lng

def tsyg_plot(sampledate):
	# sampledate = datetime(2016,1,25,11)

	noonlat, noonlong = sun_pos(sampledate)
	
	southpole = aacgmv2.convert(-90,0,0,date=sampledate, a2g=True)
	northpole = aacgmv2.convert(90,0,0,date=sampledate, a2g=True)
	southpole, northpole

	fig = plt.figure(figsize=[15,7])
	ax1 = fig.add_subplot(1,2,1, projection=ccrs.NearsidePerspective(central_longitude=180+noonlong, central_latitude=90))
	ax2 = fig.add_subplot(1,2,2, projection=ccrs.NearsidePerspective(central_longitude=noonlong, central_latitude=-90))

	mag_gridN = ccrs.RotatedGeodetic(northpole[1][0], northpole[0][0])
	mag_gridS = ccrs.RotatedGeodetic(southpole[1][0], southpole[0][0])

	for ax in [ax1, ax2]:
	    ax.coastlines(zorder=1)
	    ax.stock_img()
	#     gl = ax.gridlines(crs=mag_gridN)
	#     gl.ylocator = mticker.FixedLocator(list(range(-90,-35,5))+list(range(35,90,5)))
	    ax.text(0.5, -0.04, "00", size=20, ha="center", transform=ax.transAxes)
	    ax.text(1.03, 0.50, "06", size=20, ha="center", transform=ax.transAxes)
	    ax.text(0.5, 1.01, "12", size=20, ha="center", transform=ax.transAxes)
	    ax.text(-0.03, 0.50, "18", size=20, ha="center", transform=ax.transAxes)

	# Mag North Pole
	ax1.plot(northpole[1], northpole[0], 'ro', markersize=7, zorder=4, transform=ccrs.PlateCarree())

	# Northern Ovation Nowcast
	# lons, lats, C, img_proj = form_image(northcast)
	# ax1.pcolormesh(lons, lats, C, shading='gouraud', transform=ccrs.PlateCarree())
	gl = ax1.gridlines(crs=mag_gridN, zorder=2)
	gl.ylocator = mticker.FixedLocator(list(range(-90,-35,5))+list(range(35,90,5)))
	gl.xlocator = mticker.FixedLocator(list(range(0,361,60)))

	# Solar Wind Paramaters
	kwargs = shared.get_omni_sw_params(sampledate, offline=True)
	if abs(kwargs['byimf']) > 9999:
	    kwargs = shared.get_wind_sw_params(sampledate, offline=True)

	# AAL-PIP
	lats, lons = [-83.58, -84.50, -84.42, -84.81, -83.32, -81.95], [89.26, 77.20, 57.96, 37.63, 12.97, 5.67]
	ax2.plot(lons, lats, '2C3', markersize=7, transform=ccrs.PlateCarree())

	# Conjugates
	conjugates = tsyganenko.tsygTrace(lat=lats, lon=lons, rho=[6371]*len(lats), datetime=sampledate, **kwargs)
	#                                   vswgse=[-411,-15,1], pdyn=1.04, dst=-2, byimf=-0.5, bzimf=0.9)
	ax1.plot(conjugates.lonNH, conjugates.latNH, '2C3', markersize=7, transform=ccrs.PlateCarree())

	# DTU
	lats, lons = [72.78, 70.68, 69.25, 67.93, 65.42, 64.17], [303.85, 307.87, 306.47, 306.43, 307.10, 308.27]
	ax1.plot(lons, lats, '2C1', markersize=7, transform=ccrs.PlateCarree())

	# Conjugates
	conjugates = tsyganenko.tsygTrace(lat=lats, lon=lons, rho=[6371]*len(lats), datetime=sampledate, **kwargs)
	#                                   vswgse=[-411,-15,1], pdyn=1.04, dst=-2, byimf=-0.5, bzimf=0.9)
	ax2.plot(conjugates.lonSH, conjugates.latSH, '2C1', markersize=7, transform=ccrs.PlateCarree())

	# Mag South Pole
	ax2.plot(southpole[1], southpole[0], 'ro', markersize=7, zorder=4, transform=ccrs.PlateCarree())

	# Southern Ovation Nowcast
	# lons, lats, C, img_proj = form_image(southcast, north=False)
	# ax2.pcolormesh(lons, lats, C, shading='gouraud', transform=ccrs.PlateCarree())
	gl = ax2.gridlines(crs=mag_gridS, zorder=2)
	gl.ylocator = mticker.FixedLocator(list(range(-90,-35,5))+list(range(35,90,5)))
	gl.xlocator = mticker.FixedLocator(list(range(0,361,60)))
	ax2.texts[1].set_text('18')
	ax2.texts[3].set_text('06')

	# 40th Meridian
	mlats = np.array(list(range(-60,-95,-5)))
	mlons = [40]*len(mlats)
	lats, lons = aacgmv2.convert(mlats, mlons, 0, date=sampledate, a2g=True)
	ax2.plot(lons, lats, 'b', zorder=1, transform=ccrs.PlateCarree(), alpha=0.5)
	mlats = np.array(list(range(60,95,5)))
	lats, lons = aacgmv2.convert(mlats, mlons, 0, date=sampledate, a2g=True)
	ax1.plot(lons, lats, 'b', zorder=1, transform=ccrs.PlateCarree(), alpha=0.5)

	plt.show()

if __name__ == '__main__':
	tsyg_plot(dt.datetime(2016,1,25,12))
	exit()
