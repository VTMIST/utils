import os
import gzip
import numpy as np
import pandas as pd
import datetime as dt
import pysftp
import netrc

datapath_local = 'S:/Space/Datasets/aalpip_raw'
datapath_remote = '/home/aalpip/data/'
# Magnetic Conjugates Station ID
conjugates = {'PG0': 'upn',
              'PG1': 'umq',
              'PG2': 'gdh',
              'PG3': 'atu',
              'PG4': 'skt',
              'PG5': 'ghb'}


def get_filebox_cwd():
    """Get the current working directory (root path) of a filebox FTP session

    Returns:
        string: Current working directory of filebox FTP session ('/home/aalpip/')
    """
    creds = netrc.netrc()
    un, _, pw = creds.authenticators('filebox.ece.vt.edu')
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    with pysftp.Connection('filebox.ece.vt.edu', username=un, password=pw, cnopts=cnopts) as sftp:
        return sftp.pwd


def find_reboots(hskp_dataframe):
    """Scan a housekeeping file for negative trends in uptime, indicating a reboot has occured

    Args:
        hskp_dataframe (TYPE): Housekeeping dataframe

    Returns:
        reboots: An index listing of reboots based on the original dataframe
        datetime: A series of reboot datetimes based on the original dataframe
    """
    reboots = hskp_dataframe.diff()['Uptime_secs'][hskp_dataframe.diff()[
        'Uptime_secs'] < 0]
    reboots.where(reboots == 1, 1, inplace=True)
    datetime = hskp_dataframe['datetime'][reboots.index]
    return datetime, reboots


def generate_available_dates(year=2017, system=4, subsystem='sc', local=True):
    """Generate a python list of available dates in a year

    Args:
        year (int, optional): Year to discover
        system (int, optional): System number
        subsystem (str, optional): Subsystem ('sc', 'fg', 'hf', 'hskp', 'cases')
        local (bool, optional): Are we checking the local or remote directory

    Returns:
        list: Dates in '%Y_%m_%d' format
    """
    datapath = datapath_local if local else datapath_remote
    dates_available = []
    # scan the year's data folder for available dates
    for root, dirs, files in os.walk(
            '{0}/{1}/sys_{2}/{3}/'.format(datapath, year, system, subsystem)):
        if dirs != []:
            dates_available = dates_available + dirs
    return dates_available


def generate_filelist(start, end=None, system=2, subsystem='fg'):
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
            datefolder = date.strftime('%Y_%m_%d')
#             doy = '{:03}'.format(date.timetuple().tm_yday)
            for root, dirs, files in os.walk('{0}/{1}/sys_{2}/{3}/{4}/'.format(datapath_local, year, system, subsystem, datefolder)):
                filelist.extend([root + file for file in files])

    return filelist


def generate_yearly_masterlist(year=2017, system=3, subsystem='sc', local=True):
    """Generate a python list of available files in a year

    Args:
        year (int, optional): Year to discover
        system (int, optional): System number
        subsystem (str, optional): Subsystem ('sc', 'fg', 'hf', 'hskp', 'cases')
        local (bool, optional): Are we checking the local or remote directory

    Returns:
        list: long filenames
    """
    datapath = datapath_local if local else datapath_remote
    yearly_masterlist = []
    # scan the year's data folder for all available files
    for root, dirs, files in os.walk(
            '{0}/{1}/sys_{2}/{3}/'.format(datapath, year, system, subsystem)):
        if files != []:
            for file in files:
                yearly_masterlist = yearly_masterlist + [root + '/' + file]
    # return full filenames with paths for each file
    return yearly_masterlist


def read_housekeeping_list(hskp_zip_list=''):
    """Read in a housekeeping filelist and return a dataframe

    Args:
        hskp_zip_list (str, optional): Python list of full file names to read

    Returns:
        DataFrame: A pandas dataframe with the following columns:

        'datetime', 'Modem_on', 'FG_on', 'SC_on', 'CASES_on', 'HF_On', 'Htr_On',
        'Garmin_GPS_on', 'Overcurrent_status_on', 'T_batt_1', 'T_batt_2',
        'T_batt_3', 'T_FG_electronics', 'T_FG_sensor', 'T_router', 'V_batt_1',
        'V_batt_2', 'V_batt_3', 'I_input', 'P_input', 'lat', 'long',
        'sys_time_error_secs', 'UTC_sync_age_secs', 'Uptime_secs',
        'CPU_load_1_min', 'CPU_load_5_min', 'CPU_load_15_min'
    """
    df_hskp = pd.DataFrame()
    for zip_file in hskp_zip_list:
        with gzip.open(zip_file) as file:
            df_in = pd.read_csv(file, sep=',', header=0)
            date = df_in[df_in.columns[:6]]
            df_in.drop(['Month', 'Day', 'Hour', 'Minute', 'Second'], axis=1, inplace=True)
            df_in.rename({'Year': 'datetime'}, axis=1, inplace=True)
            df_in['datetime'] = pd.to_datetime(date)
            df_hskp = df_hskp.append(df_in, ignore_index=True)
    return df_hskp


def _read_fluxgate_list(fg_zip_list=''):
    """Read in a fluxgate filelist and return a dataframe

    Args:
        hskp_zip_list (str, optional): Python list of full file names to read

    Returns:
        DataFrame: A pandas dataframe with the following columns:

        'datetime', 'Bx', 'By', 'Bz', 'Calibrating'
    """
    df_fg = pd.DataFrame()
    fg_datetimes = []
    fg_sample_rate = dt.timedelta(seconds=1)
    for zip_file in fg_zip_list:
        fg_file_start = dt.datetime.strptime(
            zip_file[-30:-11], '%Y_%m_%d_%H_%M_%S')
        with gzip.open(zip_file) as file:
            df_in = pd.read_csv(file, sep=',', header=0)
            df_fg = df_fg.append(df_in, ignore_index=True)
            fg_in_dates = [fg_file_start + fg_sample_rate *
                           x for x in range(0, df_in.shape[0])]
            # fg_datetimes = fg_datetimes + fg_in_dates
            fg_datetimes.extend(fg_in_dates)
    df_fg['datetime'] = fg_datetimes
    df_fg = df_fg.reindex(
        columns=['datetime', 'Bx', 'By', 'Bz', 'Calibrating'])
    return df_fg[['datetime', 'Bx', 'By', 'Bz']].astype({'datetime': np.dtype('<M8[ns]'), 'Bx': np.float32, 'By': np.float32, 'Bz': np.float32}, copy=True)


def read_fluxgate_list(fg_zip_list=''):
    """Read in a fluxgate filelist and return a dataframe

    Args:
        hskp_zip_list (str, optional): Python list of full file names to read

    Returns:
        DataFrame: A pandas dataframe with the following columns:

        'datetime', 'Bx', 'By', 'Bz', 'Calibrating'
    """
    def df_fg_gen(fg_zip_list):
        fg_sample_rate = dt.timedelta(seconds=1)
        for zip_file in fg_zip_list:
            fg_file_start = dt.datetime.strptime(zip_file[-30:-11], '%Y_%m_%d_%H_%M_%S')
            with gzip.open(zip_file) as file:
                df_in = pd.read_csv(file, sep=',', header=0)
                fg_in_dates = pd.date_range(fg_file_start, periods=df_in.shape[0], freq=fg_sample_rate)
                df_in['datetime'] = pd.Series(fg_in_dates)
            df_in = df_in.reindex(columns=['datetime', 'Bx', 'By', 'Bz', 'Calibrating'])
        yield df_in[['datetime', 'Bx', 'By', 'Bz']].astype({'datetime': np.dtype('<M8[ns]'), 'Bx': np.float16, 'By': np.float16, 'Bz': np.float16})

    return pd.concat(df_fg_gen(fg_zip_list), ignore_index=True)


def read_searchcoil_list(sc_zip_list=''):
    """Read in a searchcoil filelist and return a dataframe

    Args:
        sc_zip_list (str, optional): Python list of full file names to read

    Returns:
        DataFrame: A pandas dataframe with the following columns:

        'datetime', 'dBx', 'dBy'
    """
    def df_sc_gen(sc_zip_list):
        sample_rate = dt.timedelta(microseconds=100000)
        for file in sc_zip_list:
            file_start = dt.datetime.strptime(file[-26:-7], '%Y_%m_%d_%H_%M_%S')
            df_in = pd.DataFrame(columns=['datetime', 'dBx', 'dBy'])
            with gzip.open(file, mode='rb') as bitstream:
                in_bits = bitstream.read().hex()
                samples = [int(in_bits[i:i + 3], 16) for i in range(0, len(in_bits), 3)]
                samples = [x - 4096 if x > 2047 else x for x in samples]
                df_in['dBx'] = [samples[x] * (.0049 / 4.43) for x in range(0, len(samples), 2)]
                df_in['dBy'] = [samples[x] * (.0049 / 4.43) for x in range(1, len(samples), 2)]

                in_dates = pd.date_range(file_start, periods=len(in_bits) // 3, freq=sample_rate)
                df_in['datetime'] = pd.Series(in_dates)
            yield df_in.astype({'datetime': np.dtype('<M8[ns]'), 'dBx': np.float16, 'dBy': np.float16})

    return pd.concat(df_sc_gen(sc_zip_list), ignore_index=True)


def import_subsys_old(start='2017_01_01', end='2017_01_01', system=4, subsys='sc'):
    """Reads a subset of the year's data and return a dataframe

    Args:
        start (str, optional): First date of subset
        end (str, optional): Last date of subset
        system (int, optional): Which system to grab from
        subsystem (str, optional): Which instrument subsystem

    Returns:
        DataFrame: A pandas dataframe with subsystem specific columns.
    """
    # subsystem function dictionary
    subsfunc = {
        'sc': read_searchcoil_list,
        'fg': read_fluxgate_list,
        'hskp': read_housekeeping_list
    }

    # generate a list of all files in a range
    yearly_masterlist = generate_yearly_masterlist(
        int(start[:4]), system, subsys)
    if int(start[:4]) < int(end[:4]):
        for year in range(int(start[:4]) + 1, int(end[:4]) + 1):
            yearly_masterlist += generate_yearly_masterlist(
                year, system, subsys)
    # Find the starting index in the master file list for the year
    for x in yearly_masterlist:
        if start in x:
            start_ind = yearly_masterlist.index(x)
            break
        start_ind = 0
    # Find the ending index in the master file list for the year
    for x in reversed(yearly_masterlist):
        if end in x:
            end_ind = yearly_masterlist.index(x) + 1
            break
        end_ind = len(yearly_masterlist) - 1
    return subsfunc[subsys](yearly_masterlist[start_ind:end_ind])


def import_subsys(start: dt.datetime, end=None, system=4, subsys='sc'):
    """Reads a subset of the year's data and return a dataframe

    Args:
        start (str, optional): First date of subset
        end (str, optional): Last date of subset
        system (int, optional): Which system to grab from
        subsystem (str, optional): Which instrument subsystem

    Returns:
        DataFrame: A pandas dataframe with subsystem specific columns.
    """
    # subsystem function dictionary
    subsfunc = {
        'sc': read_searchcoil_list,
        'fg': read_fluxgate_list,
        'hskp': read_housekeeping_list
    }

    # generate a list of all files in a range
    filelist = generate_filelist(start, end, system=system, subsystem=subsys)

    return subsfunc[subsys](filelist)
