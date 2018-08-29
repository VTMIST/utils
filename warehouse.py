# import os
import sys
import numpy as np
import datetime as dt
import matplotlib.pyplot as plt
import matplotlib.colors as mcolor
import matplotlib.dates as mdates

sys.path.append('/code/utils/')
import aalpip


daycheck = dt.datetime(2018, 2, 23)
normal_file_count = {'fg': 1, 'sc': 4, 'cases': 1, 'hf': 1, 'hskp': 1}
syslist = [1, 2, 3, 4, 5, 6]


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
    global daycheck
    daycheck = datetime
    subsyslist = ['fg', 'sc', 'cases', 'hf', 'hskp']
    count = {sub: np.array([]) for sub in subsyslist}

    for sub in subsyslist:
        # hskp files
        filelist = {i: aalpip.generate_filelist(start=daycheck, system=i, subsystem=sub) for i in syslist}
        # totals = [len(filelist[sys]) for sys in filelist]
        counter = np.zeros((7, 24))
        # set the filecount map for sys2+
        for system in syslist:
            for hour in range(24):
                counter[system, hour] = [daycheck.strftime('%Y_%m_%d_{:02}'.format(hour)) in file for file in filelist[system]].count(True)
        counter[1, :] = len(filelist[1])

        # rearrange by PG --- NEEDS TO BE UPDATED ---
        counter = _sys_to_PG(counter).T

        count[sub] = counter.copy()

    hskp = {i: aalpip.import_subsys(start=daycheck, system=i, subsys='hskp') for i in syslist}
    count['reboots'] = {i: aalpip.find_reboots(hskp[i]) for i in syslist}
    # count['reboots'] = {i: 0 if count['reboots'][i][0].empty else count['reboots'][i] for i in syslist}

    return count


def _build_figure(count):
    # create a matplotlib figure and the relevant axes
    fig = plt.figure(figsize=[14, 16])
    ax_fg = fig.add_subplot(2, 5, 1)
    ax_sc = fig.add_subplot(2, 5, 2)
    ax_cases = fig.add_subplot(2, 5, 3)
    ax_hf = fig.add_subplot(2, 5, 4)
    ax_hskp = fig.add_subplot(2, 5, 5)
    ax_tmps = fig.add_subplot(4, 1, 3)
    ax_vlts = fig.add_subplot(4, 1, 4)

    # -- AXES SETUP ---
    # Main title
    fig.text(.5, .96, 'File Inventory for {}'.format(daycheck.strftime('%Y-%m-%d')), fontsize=19, ha='center')
    # Reboot counter
    fig.text(.5, .92, 'Reboot Counter: PG0 - {rb0:02} | PG1 - {rb1:02} | PG2 - {rb2:02} | PG3 - {rb3:02} | PG4 - {rb4:02} | PG5 - {rb5:02} | TST - {rb7:02}'.format(
        rb0=len(count['reboots'][2][0]),
        rb1=len(count['reboots'][1][0]),
        rb2=len(count['reboots'][4][0]),
        rb3=len(count['reboots'][5][0]),
        rb4=len(count['reboots'][6][0]),
        rb5=len(count['reboots'][3][0]),
        rb7=0), ha='center', fontsize=14)
    # Inventory Titles
    ax_fg.set_title('FGM')
    ax_sc.set_title('SCM')
    ax_cases.set_title('CASES')
    ax_hf.set_title('HF')
    ax_hskp.set_title('HSKP')

    # axes
    for ax in [ax_fg, ax_sc, ax_cases, ax_hf, ax_hskp]:
        # y-axis
        ax_fg.set_ylabel('Hour')
        # place ticks and labels in the middle of the boxes
        ax.yaxis.set_ticks(np.linspace(0.5, 23.5, 24))
        ax.yaxis.set_ticklabels(range(24))
        # x-axis
        ax.xaxis.set_ticks(np.linspace(0.5, 6.5, 7))
        ax.xaxis.set_ticklabels(['PG0', 'PG1', 'PG2', 'PG3', 'PG4', 'PG5', 'TST'], fontsize=9)

    # --- PLOTTING ---
    # colormap used for inventory counter status (empty, expected, exceeded)
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
    # offsets to get text more or less in the center of the box
    xoffset = 0.15
    yoffset = 0.30
    # loop through the grid in each axes and display a file count if beyond the normal expected values set above
    for hour in range(len(count['fg'])):
        for PG in range(len(count['fg'][hour])):
            if count['fg'][hour, PG] > 1:
                ax_fg.annotate(str(int(count['fg'][hour, PG])), (PG + xoffset, hour + yoffset), fontsize=8)
    for hour in range(len(count['sc'])):
        for PG in range(len(count['sc'][hour])):
            if count['sc'][hour, PG] > 4:
                ax_sc.annotate(str(int(count['sc'][hour, PG])), (PG + xoffset, hour + yoffset), fontsize=8)
    for hour in range(len(count['cases'])):
        for PG in range(len(count['cases'][hour])):
            if count['cases'][hour, PG] > 1:
                ax_cases.annotate(str(int(count['cases'][hour, PG])), (PG + xoffset, hour + yoffset), fontsize=8)
    for hour in range(len(count['hf'])):
        for PG in range(len(count['hf'][hour])):
            if count['hf'][hour, PG] > 1:
                ax_hf.annotate(str(int(count['fg'][hour, PG])), (PG + xoffset, hour + yoffset), fontsize=8)
    for hour in range(len(count['hskp'])):
        for PG in range(len(count['hskp'][hour])):
            if count['hskp'][hour, PG] > 1:
                ax_hskp.annotate(str(int(count['hskp'][hour, PG])), (PG + xoffset, hour + yoffset), fontsize=8)

    # HSKP plots
    # First, import the data
    hskp = {i: aalpip.import_subsys(start=daycheck, system=i, subsys='hskp') for i in syslist}
    # list tuples of PG an sys for populated hskp dataframes
    sys_conv = {i: hskp[i].PG for i in hskp if not hskp[i].empty}
    # create a sorted list (pglist) of PG stations and their corresponding system
    pglist = sorted([(value, key) for key, value in sys_conv.items()])

    # Plot each temperature we have available, keeping colors consistent
    [ax_tmps.plot(hskp[peng[1]]['datetime'], hskp[peng[1]]['T_router'], 'C{}'.format(peng[1] - 1), alpha=0.9) for peng in pglist]
    # Plot any reboots on the temperature graph (cleaner here than on voltage)
    [ax_tmps.plot(count['reboots'][peng[1]][0], count['reboots'][peng[1]][1], 'xC{}'.format(peng[1] - 1)) for peng in pglist]
    # write the legend, sorted by PG
    lgnd = ax_tmps.legend([peng[0] for peng in pglist], loc='upper left', bbox_to_anchor=(1, 1), fontsize=7)
    # Change the text to red if a reboot occured
    for system in syslist:
        if len(count['reboots'][system][0]) > 0:
            for txt in lgnd.texts:
                if '{}'.format(hskp[system].PG) in txt.get_text():
                    txt.set_color('r')
    # legend guide
    ax_tmps.set_title('Red = Reboot', loc='right', color='r', x=1.1)
    # axes fitting
    ax_tmps.autoscale(axis='x', tight=True)
    # axes title
    ax_tmps.set_title('Electronics Box Temperature (deg C)')
    # cap the ylimit to +40 C
    ax_tmps.set_ylim(ax_tmps.get_ylim()[0], 40 if ax_tmps.get_ylim()[1] > 40 else ax_tmps.get_ylim()[1])

    # Same as above, but for battery temperature
    [ax_vlts.plot(hskp[peng[1]]['datetime'], hskp[peng[1]]['V_batt_1'], 'C{}'.format(peng[1] - 1), alpha=0.9) for peng in pglist]
    lgnd = ax_vlts.legend([peng[0] for peng in pglist], loc='upper left', bbox_to_anchor=(1, 1), fontsize=7)
    for system in syslist:
        if len(count['reboots'][system][0]) > 0:
            for txt in lgnd.texts:
                if '{}'.format(hskp[system].PG) in txt.get_text():
                    txt.set_color('r')
    ax_vlts.autoscale(axis='x', tight=True)
    ax_vlts.set_title('Battery Voltage (volts)')
    # cap the ylimit to +15 V
    ax_vlts.set_ylim(ax_vlts.get_ylim()[0], 15 if ax_vlts.get_ylim()[1] > 15 else ax_vlts.get_ylim()[1])

    # add gridlines on the hour
    ml = mdates.HourLocator()
    for ax in [ax_tmps, ax_vlts]:
        ax.xaxis.set_minor_locator(ml)
        ax.grid(which='both')

    plt.savefig('/home/shanec1/Downloads/inventory.png')
    # plt.show()
    pass


if __name__ == '__main__':
    counter = _build_inventory(dt.datetime(2018, 2, 28))
    _build_figure(counter)
    exit()
