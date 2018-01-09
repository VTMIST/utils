import os
import gzip
import pandas as pd
import numpy as np
import datetime as dt
import pysftp
import netrc

datapath = 'S:/Space/Datasets/aalpip_raw'


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


def generate_available_dates(year=2017, system=4, subsystem='sc'):
    """Generate a python list of available dates in a year

    Args:
        year (int, optional): Year to discover
        system (int, optional): System number
        subsystem (str, optional): Subsystem ('sc', 'fg', 'hf', 'hskp', 'cases')

    Returns:
        list: Dates in '%Y_%m_%d' format
    """
    dates_available = []
    # scan the year's data folder for available dates
    for root, dirs, files in os.walk(
            '{0}/{1}/sys_{2}/{3}/'.format(datapath, year, system, subsystem)):
        if dirs != []:
            dates_available = dates_available + dirs
    return dates_available


def generate_yearly_masterlist(year=2017, system=3, subsystem='sc'):
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
    df_fg = df_fg.reindex(columns=['datetime','Bx', 'By', 'Bz', 'Calibrating'])
    return df_fg


def read_searchcoil_list(sc_zip_list=''):
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
    return df_sc


def import_searchcoil(start='2017_01_01', end='2017_01_01'):
    # generate a list of all files in a year
    yearly_masterlist = generate_yearly_masterlist(int(start[:4]), 4, 'sc')
    # Find the starting index in the master file list for the year
    for x in yearly_masterlist:
        if start in x:
            start_ind = yearly_masterlist.index(x)
            break
    # Find the ending index in the master file list for the year
    for x in reversed(yearly_masterlist):
        if end in x:
            end_ind = yearly_masterlist.index(x)
            break
    return read_searchcoil_list(yearly_masterlist[start_ind:end_ind])


def data_import_test():
    print('Dates Available - 2017 - sys_4')
    print(generate_available_dates())
    dt, xs, ys = import_searchcoil()
    print(dt[-3:])
    print(dt[:3])
    return


if __name__ == '__main__':
    data_import_test()
    exit()
