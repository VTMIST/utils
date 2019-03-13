import pandas as pd
import numpy as np
import datetime as dt
import matplotlib
# matplotlib.use('agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as tkr
import matplotlib.colors as colors
from matplotlib import gridspec
import scipy.signal as sig
import sys
import scipy.io as io
from astropy.time import Time

sys.path.append('/code/utils/')
import aalpip
import dtu


def generate_fgm_axes(daycheck=dt.datetime(2017, 6, 1), endcheck=dt.datetime(2017, 6, 1), shour=10, ehour=20, ylim=45):
    print(type(daycheck), type(endcheck))
    # data import
    try:
        upn = dtu.import_subsys(daycheck, endcheck, station='upn')
    except ValueError as err:
        upn = pd.DataFrame(columns=['datetime', 'Bx', 'By', 'Bz'])
        print(err)
        print('failed to import upn')
    try:
        umq = dtu.import_subsys(daycheck, endcheck, station='umq')
    except ValueError:
        umq = pd.DataFrame(columns=['datetime', 'Bx', 'By', 'Bz'])
        print('failed to import umq')
    try:
        ghb = dtu.import_subsys(daycheck, endcheck, station='ghb')
    except ValueError:
        ghb = pd.DataFrame(columns=['datetime', 'Bx', 'By', 'Bz'])
        print('failed to import ghb')
    try:
        gdh = dtu.import_subsys(daycheck, endcheck, station='gdh')
    except ValueError:
        gdh = pd.DataFrame(columns=['datetime', 'Bx', 'By', 'Bz'])
        print('failed to import gdh')
    try:
        atu = dtu.import_subsys(daycheck, endcheck, station='atu')
    except ValueError:
        atu = pd.DataFrame(columns=['datetime', 'Bx', 'By', 'Bz'])
        print('failed to import atu')
    try:
        skt = dtu.import_subsys(daycheck, endcheck, station='skt')
    except ValueError:
        skt = pd.DataFrame(columns=['datetime', 'Bx', 'By', 'Bz'])
        print('failed to import skt')
    dtumag = [upn, umq, gdh, atu, skt, ghb]

    syslist = [1, 2, 3, 4, 5, 6]
    fg = {}
    for i in syslist:
        try:
            fg[i] = aalpip.import_subsys(start=daycheck, end=endcheck, system=i, subsys='fg')
        except Exception as e:
            fg[i] = pd.DataFrame([])
    # fg = {i: aalpip.import_subsys(start=daycheck, end=endcheck, system=i, subsys='fg') for i in syslist}

    # build horizontal components
    for i in syslist:
        try:
            fg[i]['Bhrz'] = np.sqrt((sig.detrend(fg[i].Bx) ** 2) + (sig.detrend(fg[i].By) ** 2))
        except Exception as e:
            fg[i]['Bhrz'] = pd.Series([])
    for station in dtumag:
        try:
            station['Bhrz'] = np.sqrt((sig.detrend(station.Bx) ** 2) + (sig.detrend(station.By) ** 2))
        except Exception as e:
            station['Bhrz'] = pd.Series([])

    # plot
    fig = plt.figure(figsize=[13, 8])
    gs = gridspec.GridSpec(6, 1)
    ax0 = plt.subplot(gs[0, 0])
    ax1 = plt.subplot(gs[1, 0], sharex=ax0, sharey=ax0)
    ax2 = plt.subplot(gs[2, 0], sharex=ax0, sharey=ax0)
    ax3 = plt.subplot(gs[3, 0], sharex=ax0, sharey=ax0)
    ax4 = plt.subplot(gs[4, 0], sharex=ax0, sharey=ax0)
    ax5 = plt.subplot(gs[5, 0], sharex=ax0, sharey=ax0)

    magaxis = 'Bhrz'

    # Filtering
    sos_filt = sig.butter(5, [2e-3, 10e-3], 'bandpass', output='sos')
    slow_sos_filt = sig.butter(5, np.array([2e-3, 10e-3]) * 10, 'bandpass', output='sos')

    try:
        ax0.plot(upn['datetime'], sig.sosfilt(slow_sos_filt, sig.detrend(upn[magaxis])), 'C0', label='UPN')
    except Exception as e:
        pass
    try:
        ax0.plot(fg[2]['datetime'], sig.sosfilt(sos_filt, sig.detrend(fg[2][magaxis])), 'C1', label='PG0')
    except Exception as e:
        pass

    try:
        ax1.plot(umq['datetime'], sig.sosfilt(slow_sos_filt, sig.detrend(umq[magaxis])), 'C0', label='UMQ')
    except Exception as e:
        pass
    try:
        ax1.plot(fg[1]['datetime'], sig.sosfilt(sos_filt, sig.detrend(fg[1][magaxis])), 'C1', label='PG1')
    except Exception as e:
        pass

    try:
        ax2.plot(gdh['datetime'], sig.sosfilt(slow_sos_filt, sig.detrend(gdh[magaxis])), 'C0', label='GDH')
    except Exception as e:
        pass
    try:
        ax2.plot(fg[4]['datetime'], sig.sosfilt(sos_filt, sig.detrend(fg[4][magaxis])), 'C1', label='PG2')
    except Exception as e:
        pass

    try:
        ax3.plot(atu['datetime'], sig.sosfilt(slow_sos_filt, sig.detrend(atu[magaxis])), 'C0', label='ATU')
    except Exception as e:
        pass
    try:
        ax3.plot(fg[5]['datetime'], sig.sosfilt(sos_filt, sig.detrend(fg[5][magaxis])), 'C1', label='PG3')
    except Exception as e:
        pass

    try:
        ax4.plot(skt['datetime'], sig.sosfilt(slow_sos_filt, sig.detrend(skt[magaxis])), 'C0', label='SKT')
    except Exception as e:
        pass
    try:
        ax4.plot(fg[6]['datetime'], sig.sosfilt(sos_filt, sig.detrend(fg[6][magaxis])), 'C1', label='PG4')
    except Exception as e:
        pass

    try:
        ax5.plot(ghb['datetime'], sig.sosfilt(slow_sos_filt, sig.detrend(ghb[magaxis])), 'C0', label='GHB')
    except Exception as e:
        pass
    try:
        ax5.plot(fg[3]['datetime'], sig.sosfilt(sos_filt, sig.detrend(fg[3][magaxis])), 'C1', label='PG5')
    except Exception as e:
        pass

    ax0.set_title('{} (nt)'.format(magaxis), loc='center', fontsize=8)

    ax0.set_ylabel('UPN - PG0', fontsize=8)
    ax1.set_ylabel('UMQ - PG1', fontsize=8)
    ax2.set_ylabel('GDH - PG2', fontsize=8)
    ax3.set_ylabel('ATU - PG3', fontsize=8)
    ax4.set_ylabel('SKT - PG4', fontsize=8)
    ax5.set_ylabel('GHB - PG5', fontsize=8)

    [ax.legend(bbox_to_anchor=(1, 1), loc=2, fontsize=8) for ax in fig.axes]

    plt.setp([ax0.get_xticklabels(), ax1.get_xticklabels(), ax2.get_xticklabels(), ax3.get_xticklabels(), ax4.get_xticklabels()], visible=False)
    ax0.autoscale(axis='x', enable=True, tight=True)

    # ax0.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax5.tick_params(axis='x', labelsize=8)
    # ax0.xaxis.set_minor_locator(mdates.HourLocator())
    ax0.yaxis.set_major_locator(tkr.MultipleLocator(ylim / 2))
    ax0.set_xlim([daycheck + dt.timedelta(hours=shour), endcheck + dt.timedelta(hours=ehour)])
    ax0.set_ylim(-ylim, ylim)

    [ax.grid(which='both') for ax in fig.axes]

    # plt.savefig('out.png')

    return fig


if __name__ == '__main__':
    generate_fgm_axes()
    exit()
