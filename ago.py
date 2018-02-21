import os
import numpy as np
import pandas as pd
import datetime as dt

datapath_local = 'S:/Space/Datasets/halley'
# datapath_remote = '/home/aalpip/data/'

conjugates = {'AGO4': 'KUV',
              'AGO3': 'STF'}

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
    for zip_file in filelist:
        with open(zip_file) as file:
            df_in = pd.read_table(file, header=0, names=['datetime', 'dBx', 'dBy', 'dBz'], dtype={'datetime': np.str, 'dBx': np.float32, 'dBy': np.float32, 'dBz': np.float32})
            df_sc = df_sc.append(df_in, ignore_index=True)
    df_sc['datetime'] = pd.to_datetime(df_sc['datetime'])
    return df_sc
