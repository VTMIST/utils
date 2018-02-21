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
conjugates = {'UPN': 'PG0',
              'UMQ': 'PG1',
              'GDH': 'PG2',
              'ATU': 'PG3',
              'SKT': 'PG4',
              'GHB': 'PG5'}


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
            df_in.drop(['Month', 'Day', 'Hour', 'Minute',
                        'Second'], axis=1, inplace=True)
            df_in.rename_axis({'Year': 'datetime'}, axis=1, inplace=True)
            df_in['datetime'] = pd.to_datetime(date)
            df_hskp = df_hskp.append(df_in, ignore_index=True)
    return df_hskp


def read_fluxgate_list(fg_zip_list=''):
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
            fg_datetimes = fg_datetimes + fg_in_dates
    df_fg['datetime'] = fg_datetimes
    df_fg = df_fg.reindex(
        columns=['datetime', 'Bx', 'By', 'Bz', 'Calibrating'])
    return df_fg[['datetime', 'Bx', 'By', 'Bz']].astype({'datetime': np.dtype('<M8[ns]'), 'Bx': np.float32, 'By': np.float32, 'Bz': np.float32}, copy=True)


def read_searchcoil_list(sc_zip_list=''):
    """Read in a searchcoil filelist and return a dataframe

    Args:
        sc_zip_list (str, optional): Python list of full file names to read

    Returns:
        DataFrame: A pandas dataframe with the following columns:

        'datetime', 'dBx', 'dBy'
    """
    df_sc = pd.DataFrame()
    datetimes = []
    hexstr = b''
    sample_rate = dt.timedelta(microseconds=100000)
    for file in sc_zip_list:
        file_start = dt.datetime.strptime(file[-26:-7], '%Y_%m_%d_%H_%M_%S')
        with gzip.open(file, mode='rb') as bitstream:
            in_bits = bitstream.read()
            hexstr = hexstr + in_bits
            in_dates = [file_start + sample_rate *
                        x for x in range(0, len(in_bits) // 3)]
            datetimes = datetimes + in_dates
    df_sc['datetime'] = datetimes
    # merge all binary values into a single stream
    # NOTE: This also fixes the stripped leading zeros from read
    binstr = ''.join(['{:08b}'.format(x) for x in hexstr])
    # separate the stream into 12bit ADC values
    binstr = [binstr[i:i + 12] for i in range(0, len(binstr), 12)]
    # convert the binary values to integers
    intstr = [int(x, 2) for x in binstr]
    intstr = [x - 4096 if x > 2047 else x for x in intstr]
    # group the X/Y by sample
    all_samples = [intstr[i:i + 2] for i in range(0, len(intstr), 2)]
    # Add the values to the dataframe in their respective columns,
    # then scale them to our system (bit conversion / ADC Gain)
    df_sc['dBx'] = [x[0] * (.0049 / 4.43) for x in all_samples]
    df_sc['dBy'] = [x[1] * (.0049 / 4.43) for x in all_samples]
    return df_sc.astype({'datetime': np.dtype('<M8[ns]'), 'dBx': np.float32, 'dBy': np.float32}, copy=True)


def import_subsys(start='2017_01_01', end='2017_01_01', system=4, subsys='sc'):
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
    # Find the ending index in the master file list for the year
    for x in reversed(yearly_masterlist):
        if end in x:
            end_ind = yearly_masterlist.index(x) + 1
            break
    return subsfunc[subsys](yearly_masterlist[start_ind:end_ind])
