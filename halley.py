import os
import requests
import numpy as np
import pandas as pd
import datetime as dt

datapath_local = 'S:/Space/Datasets/halley'
datapath_remote = 'http://psddb.nerc-bas.ac.uk/data/psddata/atmos/space/'


def fetch_remote(datetime, subsystem='sc'):
    """Grabs mag data from UK Polar Data Centre, NERC and saves a local copy to datapath_local/{instrument}/{year}/{file}

    Args:
        datetime (datetime): python datetime object
        subsystem (str, optional): Instrument string ('scm' or 'fgm')

    Returns:
        available (bool): True if file is downloaded from remote server
    """
    year = datetime.year
    doy = '{:03}'.format(datetime.timetuple().tm_yday)
    month = '{:02}'.format(datetime.month)
    date = '{:02}'.format(datetime.day)
    URL = {'sc': '{base_url}/scm/halley//{year}/data/ascii/{doy}{year}.TXT'.format(base_url=datapath_remote, year=year, doy=doy),
           'fg': '{base_url}/fluxgate/halley//{year}/data/00001/ZFM{year}{month}{date}.dat'.format(base_url=datapath_remote, year=year, doy=doy, month=month, date=date)}
    r = requests.get(URL[subsystem])

    available = r.status_code == requests.codes.ok
    if available:
        try:
            with open('{0}/{3}/{1}/{2}{1}.txt'.format(datapath_local, year, doy, subsystem), 'w+') as local_file:
                local_file.write(r.text)
        except FileNotFoundError:
            try:
                os.makedirs('{0}/{1}/{2}'.format(datapath_local, subsystem, year))
                with open('{0}/{3}/{1}/{2}{1}.txt'.format(datapath_local, year, doy, subsystem), 'x+') as local_file:
                    local_file.write(r.text)
            except OSError:
                pass
            pass
    return available


def generate_filelist(start, end=None, subsystem='sc'):
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
        remotelist = []
        locallist = []
        for date in searchlist:
            year = date.year
            doy = '{:03}'.format(date.timetuple().tm_yday)
            try:
                with open('{0}/{3}/{1}/{2}{1}.txt'.format(datapath_local, year, doy, subsystem), 'x'):
                    pass
                remotelist.append(date)
            except FileExistsError:
                locallist.append(date)
            except FileNotFoundError:
                remotelist.append(date)
        locallist = locallist + [date for date in remotelist if fetch_remote(date, subsystem=subsystem)]

        filelist = []
        for date in locallist:
            year = date.year
            doy = '{:03}'.format(date.timetuple().tm_yday)
            filelist.append('{0}/{3}/{1}/{2}{1}.txt'.format(datapath_local, year, doy, subsystem))

    return filelist


def generate_yearly_masterlist(year=2017, subsystem='sc', local=True):
    """Generate a python list of available files in a year

    Args:
        year (int, optional): Year to discover
        subsystem (str, optional): Subsystem ('sc', 'fg')
        local (bool, optional): Are we checking the local or remote directory

    Returns:
        list: long filenames
    """
    datapath = datapath_local if local else datapath_local
    yearly_masterlist = []
    # scan the year's data folder for all available files
    for root, dirs, files in os.walk(
            '{0}/{1}/{2}'.format(datapath, subsystem, year)):
        if files != []:
            for file in files:
                yearly_masterlist = yearly_masterlist + [root + '/' + file]
    # return full filenames with paths for each file
    return yearly_masterlist


def read_fluxgate_list(filelist=''):
    """Read in a fluxgate filelist and return a dataframe

    Args:
        filelist (str, optional): Python list of full file names to read

    Returns:
        DataFrame: A pandas dataframe with the following columns:

        'datetime', 'Bx', 'By', 'Bz'
    """
    df_fg = pd.DataFrame()
    for zip_file in filelist:
        with open(zip_file) as file:
            df_in = pd.read_csv(file, sep=' ', header=None, usecols=[0, 2, 3, 4], names=['datetime', 'Bx', 'By', 'Bz'], dtype={'datetime': np.str, 'Bx': np.float32, 'By': np.float32, 'Bz': np.float32})
            df_fg = df_fg.append(df_in, ignore_index=True)
    df_fg['datetime'] = pd.to_datetime(df_fg['datetime'])
    return df_fg


def read_searchcoil_list(filelist=''):
    """Read in a fluxgate filelist and return a dataframe

    Args:
        filelist (str, optional): Python list of full file names to read

    Returns:
        DataFrame: A pandas dataframe with the following columns:

        'datetime', 'Bx', 'By', 'Bz'
    """
    df_sc = pd.DataFrame()
    sc_datetimes = []
    sc_sample_rate = dt.timedelta(seconds=.1)
    for txt_file in filelist:
        sc_file_start = dt.datetime.strptime(txt_file[-11:-4], '%j%Y')
        with open(txt_file) as file:
            df_in = pd.read_table(file, delim_whitespace=True, skiprows=2, names=['datetime', 'dBx', 'dBy', 'dBz'])
            df_sc = df_sc.append(df_in, ignore_index=True)
            sc_in_dates = [sc_file_start + sc_sample_rate * x for x in range(0, df_in.shape[0])]
            sc_datetimes = sc_datetimes + sc_in_dates
    df_sc['datetime'] = sc_datetimes
    return df_sc.astype({'datetime': np.dtype('<M8[ns]'), 'dBx': np.float32, 'dBy': np.float32, 'dBz': np.float32}, copy=True)
