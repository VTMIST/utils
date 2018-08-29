import os
import gzip
import numpy as np
import pandas as pd
import datetime as dt
import zipfile as zf
# import pysftp
# import netrc

datapath_local = '/data/aal-pip/data'
datapath_remote = '/home/aalpip/data'

# PG0_sys2 = pd.date_range()


# Magnetic Conjugates Station ID
conjugates = {'PG0': 'upn',
              'PG1': 'umq',
              'PG2': 'gdh',
              'PG3': 'atu',
              'PG4': 'skt',
              'PG5': 'ghb'}

lat_ranges = {'SPA': (-90, -89.9),
              'BBG': (37.21, 37.23),
              'PG0': (-83.68, -83.66),
              'PG1': (-84.51, -84.49),
              'PG2': (-84.42, -84.40),
              'PG3': (-84.82, -84.80),
              'PG4': (-83.34, -83.31),
              'PG5': (-81.97, -81.95)}

lon_ranges = {'SPA': (-180, 180),
              'BBG': (80.40, 80.42),
              'PG0': (88.66, 88.70),
              'PG1': (77.18, 77.22),
              'PG2': (57.93, 57.97),
              'PG3': (37.60, 37.64),
              'PG4': (12.23, 12.27),
              'PG5': (5.68, 5.72)}


class housekeeping_df(pd.DataFrame):
    def __init__(self,df,tail_season=None):
        pd.DataFrame.__init__(self, data=df)
        self._tail_season = 'end of season'
        self.system = 0
        self.PG = 'TST'
        
        self._locate_system()
        
    def tail_season(self):
        # This is a placeholder for seasonal charging demarkation
        return self._tail_season
    
    def _locate_system(self):
        try:
            self.lat.replace(0.0, inplace=True, method='bfill')
            self.lat.replace(0.0, inplace=True, method='ffill')
            # LAT should be enough to locate a system
            # self.long.replace(0.0, inplace=True, method='bfill')
            # self.long.replace(0.0, inplace=True, method='ffill')
            for coords in lat_ranges:
                self.loc[self.lat.between(*lat_ranges[coords]),'site'] = coords
            self.PG = self.site.iloc[-1] if self.site.iloc[-1] is not 'BBG' else 'TST'
        except Exception as err:
            pass
        return self.PG


def find_reboots(hskp_dataframe):
    """Scan a housekeeping file for negative trends in uptime, indicating a reboot has occured

    Args:
        hskp_dataframe (TYPE): Housekeeping dataframe

    Returns:
        reboots: An index listing of reboots based on the original dataframe
        datetime: A series of reboot datetimes based on the original dataframe
    """
    if 'Uptime_secs' in hskp_dataframe.columns:
        reboots = hskp_dataframe['Uptime_secs'].diff()[hskp_dataframe['Uptime_secs'].diff() < 0]
        reboots.where(reboots == 1, 1, inplace=True)
        datetime = hskp_dataframe['datetime'][reboots.index]
    else:
        datetime = pd.Series()
        reboots = pd.Series()
    return datetime, reboots


def _generate_filelist(start, end=None, system=2, subsystem='fg'):
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
            for root, dirs, files in os.walk('{0}/{1}/sys_{2}/{3}/{4}/'.format(datapath_local, year, system, subsystem, datefolder)):
                filelist.extend([root + file for file in files if os.stat(root + file).st_size > 0])

    return filelist


def generate_filelist(start, end=None, system=4, subsystem='fg'):
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
            if system == 1:
                file_path_string = '{0}/{1}/sys_{2}/'.format(datapath_local, year, system)
                subsys_string = 'HSKP' if subsystem == 'hskp' else 'MAG'
                if subsystem != 'hskp' and subsystem != 'fg':
                    return filelist
            else:
                file_path_string = '{0}/{1}/sys_{2}/{3}/{4}_{5:02}_{6:02}/'.format(datapath_local, year, system, subsystem, year, month, day)
                if subsystem == 'hf':
                    file_path_string = '{0}/{1}/sys_{2}/{3}/'.format(datapath_local, year, system, subsystem)
                subsys_string = subsystem
            for root, dirs, files in os.walk(file_path_string):
                filelist.extend([root + file for file in files if ((os.stat(root + file).st_size > 0) and
                                                                   ('{}_{:02}_{:02}'.format(year,month,day) in file) and
                                                                   ('.csv' in file[-8:] or '.dat' in file[-8:] or '.txt' in file[-8:]) and 
                                                                   (subsys_string in file))])

    return filelist


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
    def df_hskp_gen(hskp_zip_list):
        for zip_file in hskp_zip_list:
            with gzip.open(zip_file) as file:
                df_in = pd.read_csv(file, sep=',', header=0)
                date = df_in[df_in.columns[:6]]
                df_in.drop(['Month', 'Day', 'Hour', 'Minute', 'Second'], axis=1, inplace=True)
                df_in.rename({'Year': 'datetime'}, axis=1, inplace=True)
                df_in['datetime'] = pd.to_datetime(date)
                yield df_in


    def df_hskp_gen_sys1(hskp_zip_list):
        for zip_file in hskp_zip_list:
            try:
                with zf.ZipFile(zip_file) as zipped:
                    with zipped.open(zipped.namelist()[0]) as csv:
                        df_in = pd.read_csv(csv)
                        df_in.rename({'Min':'Minute', 'Sec':'Second'}, axis=1, inplace=True)
                        date = df_in[df_in.columns[1:7]]
                        df_in.drop(['Year', 'Month', 'Day', 'Hour', 'Minute', 'Second'], axis=1, inplace=True)
                        # Dropping things we dont use..
                        df_in.drop(['X Axis Null(V) Min', 'X Axis Null(V) Max', 'X Axis Null(V) Avg',
                                    'Z Axis Null(V) Min', 'Z Axis Null(V) Max', 'Z Axis Null(V) Avg',
                                    'Battery Temp(C) Min', 'Battery Temp(C) Max', 'CPU Board Temp(C) Min',
                                    'CPU Board Temp(C) Max', 'Battery(V) Min', 'Battery(V) Max', '3.3 V Min',
                                    '3.3 V Max', 'Spare 1(V) Min', 'Spare 1(V) Max', 'Spare 1(V) Avg',
                                    '  Spare 2', ' Spare 3'], axis=1, inplace=True)
                        df_in.rename({'Jul92 Date': 'datetime',
                                      'Sync Age(sec)': 'UTC_sync_age_secs',
                                      'Time Error(sec)':'sys_time_error_secs',
                                      'GPS on for sync(%)':'GPS_sync',
                                      'GPS on for heat(%)':'GPS_heat',
                                      'Int modem on for comm(%)':'int_modem_comm',
                                      'Int modem on for heat(%)':'int_modem_heat',
                                      'Int modem is overtemp(%)':'int_modem_overtemp',
                                      'Ext modem is on for comm(%)':'ext_modem_comm',
                                      'Lat (deg)':'lat',
                                      'Long (deg)':'long',
                                      'Battery Temp(C) Avg':'T_batt_1',
                                      'CPU Board Temp(C) Avg':'T_router',
                                      'Battery(V) Avg':'V_batt_1',
                                      '3.3 V Avg':'3v3',
                                      'Int. Modem RF':'int_modem_signal',
                                      'Ext. Modem RF':'ext_modem_signal'}, axis=1, inplace=True)
                        df_in['datetime'] = pd.to_datetime(date)
                    yield df_in
            except Exception as err:
                print(zip_file, ' caused an error: ', err)
                raise err

    try:
        if 'sys_1' in hskp_zip_list[0]:
            df_out = pd.concat(df_hskp_gen_sys1(hskp_zip_list), ignore_index=True).sort_values(by=['datetime']).reset_index(drop=True)
        else:
            df_out = pd.concat(df_hskp_gen(hskp_zip_list), ignore_index=True).sort_values(by=['datetime']).reset_index(drop=True)
        # df_out = pd.concat(df_hskp_gen_sys1(hskp_zip_list), ignore_index=True).sort_values(by=['datetime']).reset_index(drop=True)
    # except zf.BadZipFile as err:
        # print('Not .zip files (SYS1), trying gzip (SYS2+)')
        # df_out = pd.concat(df_hskp_gen(hskp_zip_list), ignore_index=True).sort_values(by=['datetime']).reset_index(drop=True)
    except IndexError as err:
        print('Empty File List (Data does not exist)')
        return housekeeping_df(pd.DataFrame({'datetime':[],'V_batt_1':[],'T_router':[]}))
    except Exception as e:
        raise e

    return housekeeping_df(df_out)


def _read_housekeeping_list(hskp_zip_list=''):
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


def read_fluxgate_list(fg_zip_list='', sys_1=False):
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
            yield df_in[['datetime', 'Bx', 'By', 'Bz']].astype({'datetime': np.dtype('<M8[ns]'), 'Bx': np.float32, 'By': np.float32, 'Bz': np.float32})


    def df_fg_gen_sys1(fg_zip_list):
        for zip_file in fg_zip_list:
            with zf.ZipFile(zip_file) as zipped:
                with zipped.open(zipped.namelist()[0]) as csv:
                    df_in = pd.read_csv(csv)
                    date = df_in[df_in.columns[1:7]]
                    df_in.drop(['Year', 'Month', 'Day', 'Hour', 'Minute', 'Second', 'X Null(V)', 'Z Null(V)'], axis=1, inplace=True)
                    df_in.rename({'Jul92 Date': 'datetime'}, axis=1, inplace=True)
                    df_in['datetime'] = pd.to_datetime(date)
                    df_in.rename(index=str, columns={'MagX(nT)':'Bx','MagY(nT)':'By','MagZ(nT)':'Bz'}, inplace=True)
                yield df_in[['datetime', 'Bx', 'By', 'Bz']].astype({'datetime': np.dtype('<M8[ns]'), 'Bx': np.float32, 'By': np.float32, 'Bz': np.float32})

    try:
        df_out = pd.concat(df_fg_gen_sys1(fg_zip_list), ignore_index=True).sort_values(by=['datetime']).reset_index(drop=True)
    except zf.BadZipFile as err:
        print('Not a .zip file (SYS1), trying gzip (SYS2+)')
        df_out = pd.concat(df_fg_gen(fg_zip_list), ignore_index=True).sort_values(by=['datetime']).reset_index(drop=True)
    except Exception as e:
        raise

    return df_out


def _read_fluxgate_list(fg_zip_list='', sys_1=False):
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
            yield df_in[['datetime', 'Bx', 'By', 'Bz']].astype({'datetime': np.dtype('<M8[ns]'), 'Bx': np.float32, 'By': np.float32, 'Bz': np.float32})

    return pd.concat(df_fg_gen(fg_zip_list), ignore_index=True).sort_values(by=['datetime']).reset_index(drop=True)


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


def _clean_df(df_in, subsystem='fg'):
    """Scrubs errors from input dataframe

    Args:
        df_in (dataframe): Input dataframe
        df_type (str, optional): Description

    Returns:
        Notes: The specific errors handled by this cleaning function include:
                removal of -1e32 sensor error values
                removal of duplicate timestamps
                removal of statistical outliers (>3 standard deviations)
                *removal of South Pole testing periods for individual stations

                *:To be added
    """

    # Remove error values
    df_clean = df_in.where(df_in.Bx>-1e31)
    # Remove values outside 3 stdev
    for column in df_clean.columns[1:]:
        std = df_clean[column].std()
        mean = df_clean[column].mean()
        df_clean[column] = df_clean[column].where(df_clean[column]<(mean+(3*std)))
        df_clean[column] = df_clean[column].where(df_clean[column]>(mean-(3*std)))

    # return cleaned, non-duplicated, and sorted dataframe
    return df_clean.dropna().sort_values(by=['datetime']).reset_index(drop=True)


def import_subsys(start: dt.datetime, end=None, system=4, subsys='sc', clean=False):
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

    df_out = subsfunc[subsys](filelist)
    if clean:
        _clean_df(df_out, subsystem=subsys)

    return df_out
