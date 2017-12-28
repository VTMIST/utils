import os
import gzip
import pandas as pd
import numpy as np
import datetime as dt

datapath = 'S:/Space/Datasets/aalpip_raw'


def generate_available_dates(year=2017, system=4, subsystem='sc'):
    dates_available = []
    for root, dirs, files in os.walk(
            '{0}/{1}/sys_{2}/{3}/'.format(datapath, year, system, subsystem)):
        if dirs != []:
            dates_available = dates_available + dirs
    return dates_available


def generate_yearly_masterlist(year=2017, system=3, subsystem='sc'):
    yearly_masterlist = []
    for root, dirs, files in os.walk(
            '{0}/{1}/sys_{2}/{3}/'.format(datapath, year, system, subsystem)):
        if files != []:
            for file in files:
                yearly_masterlist = yearly_masterlist + [root + '/' + file]
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
    return df_fg


def read_searchcoil_list(sc_zip_list=''):
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
    return hexstr, datetimes


def parse_searchcoil(hexstr=b''):
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
    # numpyfy the x values
    # then scale them to our system (bit conversion / ADC Gain)
    x_samples = np.array([x[0] for x in all_samples]) * (.0049 / 4.43)
    # numpyfy the y values
    # then scale them to our system (bit conversion / ADC Gain)
    y_samples = np.array([x[1] for x in all_samples]) * (.0049 / 4.43)
    return x_samples, y_samples


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
    # read in the searchcoil data, create a datetime list
    hexstr, datetimes = read_searchcoil_list(
        yearly_masterlist[start_ind:end_ind])
    # convert the binary data to sampled data
    xsamp, ysamp = parse_searchcoil(hexstr)
    return datetimes, xsamp, ysamp


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
