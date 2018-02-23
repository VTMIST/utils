import os
import numpy as np
import pandas as pd
import datetime as dt
from spacepy import pycdf


datapath_local = 'S:/Space/Datasets/dtu'
datapath_remote = 'S:/Space/Datasets/dtu'
# Magnetic Conjugates Station ID
conjugates = {'upn': 'PG0',
              'umq': 'PG1',
              'gdh': 'PG2',
              'atu': 'PG3',
              'skt': 'PG4',
              'ghb': 'PG5'}


def generate_yearly_masterlist(year=2017, station='ghb', local=True):
    """Generate a python list of available files in a year

    Args:
        year (int, optional): Year to discover
        station (int, optional): Station ID
        local (bool, optional): Are we checking the local or remote directory

    Returns:
        list: long filenames
    """
    datapath = datapath_local if local else datapath_remote
    yearly_masterlist = []
    # scan the year's data folder for all available files
    for root, dirs, files in os.walk(
            '{0}/{1}/{2}/'.format(datapath, station, year)):
        if files != []:
            for file in files:
                yearly_masterlist = yearly_masterlist + [root + '/' + file] if file != 'SHA1SUM' else yearly_masterlist
    # return full filenames with paths for each file
    return yearly_masterlist


def read_fluxgate_list(fg_zip_list='', station='ghb'):
    """Read in a fluxgate filelist and return a dataframe

    Args:
        hskp_zip_list (str, optional): Python list of full file names to read

    Returns:
        DataFrame: A pandas dataframe with the following columns:

        'datetime', 'Bx', 'By', 'Bz', 'Calibrating'
    """
    df_fg = pd.DataFrame()
    for file in fg_zip_list:
        with pycdf.CDF(file) as cdf:
            df_in = pd.DataFrame(cdf['thg_mag_{0}'.format(station)][:], columns=['Bx', 'By', 'Bz'])
            df_in['datetime'] = [dt.datetime.fromtimestamp(x) for x in cdf['thg_mag_{0}_time'.format(station)]]
            df_fg = df_fg.append(df_in, ignore_index=True)
    df_fg = df_fg.reindex(
        columns=['datetime', 'Bx', 'By', 'Bz'])
    return df_fg[['datetime', 'Bx', 'By', 'Bz']].astype({'datetime': np.dtype('<M8[ns]'), 'Bx': np.float32, 'By': np.float32, 'Bz': np.float32}, copy=True)


def import_subsys(start='2017_01_01', end='2017_01_01', station='ghb', subsys='fg'):
    """Reads a subset of the year's data and return a dataframe

    Args:
        start (str, optional): First date of subset
        end (str, optional): Last date of subset
        station (int, optional): Which station to grab from
        subsystem (str, optional): Which instrument subsystem

    Returns:
        DataFrame: A pandas dataframe with subsystem specific columns.
    """
    # subsystem function dictionary
    subsfunc = {
        'fg': read_fluxgate_list,
    }
    # fix the start and end strings
    start = start.replace('_', '')
    end = end.replace('_', '')

    # generate a list of all files in a range
    yearly_masterlist = generate_yearly_masterlist(int(start[:4]), station)
    if int(start[:4]) < int(end[:4]):
        for year in range(int(start[:4]) + 1, int(end[:4]) + 1):
            yearly_masterlist += generate_yearly_masterlist(year, station)
    # Find the starting index in the master file list for the year
    for x in yearly_masterlist:
        if start in x:
            start_ind = yearly_masterlist.index(x) + 1
            break
    # Find the ending index in the master file list for the year
    for x in reversed(yearly_masterlist):
        if end in x:
            end_ind = yearly_masterlist.index(x) + 1
            break
    return subsfunc[subsys](yearly_masterlist[start_ind:end_ind], station)
