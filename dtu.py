import os
import numpy as np
import pandas as pd
import datetime as dt
import scipy.io as io
from astropy.time import Time
from spacepy import pycdf


datapath_local = '/data/dtu/'
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
                yearly_masterlist = yearly_masterlist + [root + file] if file != 'SHA1SUM' else yearly_masterlist
    # return full filenames with paths for each file
    return yearly_masterlist


def generate_filelist(start, end=None, station='ghb', coord_format='XYZ'):
    """Search the local and remote datapaths for files in the given date range

    Args:
        start (datetime): First day of timespan
        end (datetime, optional): last day of timespan. If None (default) then end = start
        subsystem (str, optional): Instrument data to search for ('sc' or 'fg')

    Returns:
        filelist (list): List of string paths to files representing data for the given dates
    """
    end = start if end is None else end
    if (type(start) is dt.datetime) and (type(end) is dt.datetime) and (start <= end):
        searchlist = pd.date_range(start=start, end=end).to_pydatetime().tolist()
        filelist = []
        for date in searchlist:
            year = date.year
            month = date.month
            day = date.day
            for root, dirs, files in os.walk('{0}/{1}/{2:02}/'.format(datapath_local, year, month)):
                filelist.extend([root + file for file in files if ((os.stat(root + file).st_size > 0) and
                                                                   (station.upper() in file) and
                                                                   ('{}{:02}{:02}'.format(year,month,day) in file) and
                                                                   (coord_format.upper() in file))])

    return sorted(filelist)


def read_fluxgate_list(fg_zip_list='', station='ghb'):
    """Read in a fluxgate filelist and return a dataframe

    Args:
        hskp_zip_list (str, optional): Python list of full file names to read

    Returns:
        DataFrame: A pandas dataframe with the following columns:

        'datetime', 'Bx', 'By', 'Bz', 'Calibrating'
    """
    def df_fg_gen(fg_zip_list):
        for file in fg_zip_list:
            try:
                idlsav = io.readsav(file)
                df_in = pd.DataFrame(idlsav['mdata'].byteswap().newbyteorder().T)
                df_in['datetime'] = Time(idlsav['mjdtime'], format='mjd').datetime
                df_in.rename(index=str, columns={0: 'Bx', 1: 'By', 2: 'Bz'}, inplace=True)
                yield df_in[['datetime', 'Bx', 'By', 'Bz']]
            except Exception as e:
                print(file, ' CAUSED AN ERROR: ' ,e)

    df_out = pd.concat(df_fg_gen(fg_zip_list), ignore_index=True)
    df_clean = _clean_df(df_out)
    return df_clean


def _read_fluxgate_list(fg_zip_list='', station='ghb'):
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
            df_in['datetime'] = [dt.datetime.utcfromtimestamp(x) for x in cdf['thg_mag_{0}_time'.format(station)]]
            df_fg = df_fg.append(df_in, ignore_index=True)
    df_fg = df_fg.reindex(
        columns=['datetime', 'Bx', 'By', 'Bz'])
    return df_fg[['datetime', 'Bx', 'By', 'Bz']].astype({'datetime': np.dtype('<M8[ns]'), 'Bx': np.float32, 'By': np.float32, 'Bz': np.float32}, copy=True)


def import_subsys(start, end=None, station='ghb', subsys='fg'):
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
    # fix an empty end
    end = start if end is None else end

    # generate a list of all files in a range
    filelist = generate_filelist(start, end, station=station)

    return subsfunc[subsys](filelist)


def _clean_df(df_in):
    """Scrubs errors from input dataframe
    
    Args:
        df_in (dataframe): Input dataframe
    
    Returns:
        dataframe: Clean ("error free") dataframe

    Notes: The specific errors handled by this cleaning function include the removal of -1e32 values
    """

    df_clean = df_in.where(df_in.Bx>-1e31).dropna().sort_values(by=['datetime']).reset_index(drop=True)

    return df_clean


def import_subsys_old(start='2017_01_01', end='2017_01_01', station='ghb', subsys='fg'):
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
            start_ind = yearly_masterlist.index(x)
            break
    # Find the ending index in the master file list for the year
    for x in reversed(yearly_masterlist):
        if end in x:
            end_ind = yearly_masterlist.index(x) + 1
            break
    return subsfunc[subsys](yearly_masterlist[start_ind:end_ind], station)

