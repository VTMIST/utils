# import os
# import sys
import numpy as np
# import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt
import matplotlib.colors as mcolor

# sys.path.append('/code/utils/')
import aalpip


daycheck = dt.datetime(2014, 2, 23)
normal_file_count = {'fg':1,'sc':4,'cases':1,'hf':1,'hskp':1}


def _sys_to_PG(nparray):
	"""Reorder rows from system to PG based on current positions
	
	Args:
	    nparray (np.arryay): nparray should be have systems in rows and data in columns
	
	Returns:
	    np.array(): PG sorted numpy array
	"""
	PG_array = nparray[[2, 1, 4, 5, 6, 3, 0]]
	return PG_array


def _build_inventory(datetime):
    """Summary

    Args:
        datetime (TYPE): Description
    """
    syslist = [1, 2, 3, 4, 5, 6]
    subsyslist = ['fg','sc','cases','hf','hskp']
    count = {sub:np.array() for sub in subsyslist}
    
    
    for sub in subsyslist:
	    # hskp files
	    filelist = {i: aalpip.generate_filelist(start=daycheck, system=i, subsystem=sub) for i in syslist}
	    totals = [len(filelist[sys]) for sys in filelist]
	    counter = np.zeros((7, 24))
	    # set the filecount map for sys2+
	    for system in syslist:
	        for hour in range(24):
	            counter[system, hour] = [daycheck.strftime('%Y_%m_%d_{:02}'.format(hour)) in file for file in filelist[system]].count(True)
	    counter[1, :] = len(filelist[1])
	    # rearrange by PG
	    counter = _sys_to_PG(counter).T

	    count[sub] = counter.copy()

def _build_figure():
	fig = plt.figure(figsize=[21, 9])
	ax_fg = fig.add_subplot(1, 5, 1)
	ax_sc = fig.add_subplot(1, 5, 2)
	ax_cases = fig.add_subplot(1, 5, 3)
	ax_hf = fig.add_subplot(1, 5, 4)
	ax_hskp = fig.add_subplot(1, 5, 5)

	# -- AXES SETUP ---
	# titles
	fig.suptitle('Inventory for {}'.format(daycheck.strftime('%Y-%m-%d')), fontsize=19)
	ax_fg.set_title('Number of FGM Files')
	ax_sc.set_title('Number of SCM Files')
	ax_cases.set_title('Number of CASES Files')
	ax_hf.set_title('Number of HF Files')
	ax_hskp.set_title('Number of HSKP Files')

	# axes
	for ax in [ax_fg, ax_sc, ax_cases, ax_hf, ax_hskp]:
	    # y-axis
	    ax_fg.set_ylabel('Hour')
	    ax.yaxis.set_ticks(np.linspace(0.5, 23.5, 24))
	    ax.yaxis.set_ticklabels(range(24))
	    # x-axis
	    ax.set_xlabel('System')
	    ax.xaxis.set_ticks(np.linspace(0.5, 6.5, 7))
	    ax.xaxis.set_ticklabels(['PG0', 'PG1', 'PG2', 'PG3', 'PG4', 'PG5', 'TST'])

	# --- PLOTTING ---
	# colormap
	cmap = mcolor.LinearSegmentedColormap.from_list('aal-pip', [mcolor.hex2color('#f73548'), mcolor.hex2color('#35f7a3')], N=2)
	cmap.set_over(mcolor.hex2color('#3680f7'), alpha=1)

	# input should be in 2d array, systems in row, hour bin in column, as dataframes are organized by system
	# -- FLUXGATE --
	ax_fg.pcolormesh(count['fg'], alpha=0.7, linestyle='-', linewidth=0.05, color='black', cmap=cmap, norm=mcolor.Normalize(0, normal_file_count['fg']))
	# -- SEARCHCOIL --
	ax_sc.pcolormesh(count['sc'], alpha=0.7, linestyle='-', linewidth=0.05, color='black', cmap=cmap, norm=mcolor.Normalize(0, normal_file_count['sc']))
	# -- CASES --
	ax_cases.pcolormesh(count['cases'], alpha=0.7, linestyle='-', linewidth=0.05, color='black', cmap=cmap, norm=mcolor.Normalize(0, normal_file_count['cases']))
	# -- HF --
	ax_hf.pcolormesh(count['hf'], alpha=0.7, linestyle='-', linewidth=0.05, color='black', cmap=cmap, norm=mcolor.Normalize(0, normal_file_count['hf']))
	# -- HOUSEKEEPING --
	ax_hskp.pcolormesh(count['hskp'], alpha=0.7, linestyle='-', linewidth=0.05, color='black', cmap=cmap, norm=mcolor.Normalize(0, normal_file_count['hskp']))


	# annotation
	# gridmap is currently indexed by hour (0->23), then PG (0-5, test box)
	xoffset = 0.15
	yoffset = 0.30
	for hour in range(len(count['fg'])):
	    for PG in range(len(count['fg'][hour])):
	        if count['fg'][hour, PG] > 1:
	            ax_fg.annotate(str(count['fg'][hour, PG]), (PG + xoffset, hour + yoffset))
	for hour in range(len(count['sc'])):
	    for PG in range(len(count['sc'][hour])):
	        if count['sc'][hour, PG] > 4:
	            ax_sc.annotate(str(count['sc'][hour, PG]), (PG + xoffset, hour + yoffset))
	for hour in range(len(count['cases'])):
	    for PG in range(len(count['cases'][hour])):
	        if count['cases'][hour, PG] > 1:
	            ax_cases.annotate(str(count['cases'][hour, PG]), (PG + xoffset, hour + yoffset))
	for hour in range(len(count['hf'])):
	    for PG in range(len(count['hf'][hour])):
	        if count['hf'][hour, PG] > 1:
	            ax_hf.annotate(str(count['fg'][hour, PG]), (PG + xoffset, hour + yoffset))
	for hour in range(len(count['hskp'])):
	    for PG in range(len(count['hskp'][hour])):
	        if count['hskp'][hour, PG] > 1:
	            ax_hskp.annotate(str(count['hskp'][hour, PG]), (PG + xoffset, hour + yoffset))

	plt.savefig('/home/shanec1/Downloads/inventory.png')
	plt.show()


if __name__ == '__main__':
	main()