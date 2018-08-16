import requests
import datetime as dt
import ai.cdas as cdas
import numpy as np

proton_mass = 1.6726219e-27


def get_ccmc_tsyg_conj(datetime, lat, lon, SW_dyn_press=1, SW_vel=450, IMF_By=0, IMF_Bz=0, DST=1, direction='North-South'):
    """Determine the conjugate location points for a point on earth

    Args:
        datetime (datetime): Description
        lat (float): Description
        lon (float): Description
        SW_dyn_press (int, optional): Description
        SW_vel (int, optional): Description
        IMF_By (int, optional): Description
        IMF_Bz (int, optional): Description
        DST (int, optional): Description
        direction (str, optional): Description

    Returns:
        Dict: Description
    """
    class BadDirection(Exception):
        """Tsyganenko Model Directionality Error"""

        def __init__(self, message, errors):
            super(BadDirection, self).__init__(message)
            self.errors = errors

    ccmc_tsyg_url = "https://ccmc.gsfc.nasa.gov/requests/instant/tsyganenko_results.php"
    payload = {'ts_version': '01',
               'Year': '2015',
               'Day': '76',
               'Hour': '12',
               'Minute': '00',
               'Second': '00',
               'SW dynamic pressure': '1.5',
               'SW velocity': '310',
               'IMF By': '2.9',
               'IMF Bz': '-1.5',
               'Dst': '9',
               'DIR': '1',  # 1:N->S, -1:S->N
               'Geographic Geocentric Latitude': '59.25',
               'Longitude': '303.85',
               'Xgsm': '1',
               'Ygsm': '1',
               'Zgsm': '1'}

    if direction == 'North-South':
        payload['DIR'] = '1'
        if (lat < 0):
            raise BadDirection('Direction Wrong', 'Lat < 0')
    if direction == 'South-North':
        payload['DIR'] = '-1'
        if (lat > 0):
            raise BadDirection('Direction Wrong', 'Lat > 0')

    payload['Year'] = str(datetime.year)
    payload['Day'] = str(datetime.timetuple().tm_yday)
    payload['Hour'] = str(datetime.hour)
    payload['Minute'] = str(datetime.minute)
    payload['Second'] = str(datetime.second)

    payload['Geographic Geocentric Latitude'] = lat
    payload['Longitude'] = lon

    print(payload)

    response = requests.post(ccmc_tsyg_url, data=payload)

    try:
        model_results_html = next(line for line in response.iter_lines(
        ) if b'T01 Run Results' in line).decode('utf-8').split('<br />')
        conjugate_point_raw = next(model_results_html[i - 1:i + 5] for i in range(
            len(model_results_html)) if 'Conjugate point:' in model_results_html[i])
    except StopIteration as err:
        conjugate_point = {'geo_lat': 0.0,
                           'geo_lon': 0.0,
                           'dpl_lat': 0.0,
                           'dpl_lon': 0.0}
        conjugate_point['closed'] = False
    else:
        conjugate_point = {'geo_lat': float(conjugate_point_raw[2][conjugate_point_raw[2].find(':') + 1:].lstrip(' ')),
                           'geo_lon': float(conjugate_point_raw[3][conjugate_point_raw[3].find(':') + 1:].lstrip(' ')),
                           'dpl_lat': float(conjugate_point_raw[4][conjugate_point_raw[4].find(':') + 1:].lstrip(' ')),
                           'dpl_lon': float(conjugate_point_raw[5][conjugate_point_raw[5].find(':') + 1:].lstrip(' '))}
        conjugate_point['closed'] = True
    finally:
        return conjugate_point


def get_wind_sw_params(datetime, offline=False):
    # Round (floor) to the minute
    sw_date = dt.datetime.strptime(
        datetime.strftime('%Y%m%d%H%M'), '%Y%m%d%H%M')
    # Get data from WIND for the minute period of interest
    WIND_MFI = cdas.get_datasets('istp_public', idPattern='WI_H0_MFI.*')
    datasetID = WIND_MFI['DatasetDescription'][0]['Id']
    MFI_data = cdas.get_data('sp_phys', datasetID, sw_date, sw_date + dt.timedelta(minutes=2), ['BGSM'])
    WIND_3DP = cdas.get_datasets('istp_public', idPattern='WI_PM_3DP.*')
    datasetID = WIND_3DP['DatasetDescription'][0]['Id']
    PLM_data = cdas.get_data('sp_phys', datasetID, sw_date, sw_date + dt.timedelta(minutes=1), ['P_DENS', 'P_VELS'])
    # Get DST from OMNI (hourly)
    datasets = cdas.get_datasets(
        'istp_public', idPattern='.*MRG1HR', labelPattern='.*OMNI.*')
    datasetID = datasets['DatasetDescription'][0]['Id']
    variables = cdas.get_variables('istp_public', datasetID)
    DST_data = cdas.get_data(
        'sp_phys', datasetID, sw_date - dt.timedelta(hours=2), sw_date, ['DST1800'])

    sw_params = {
        'IMF_By': MFI_data['BY_(GSM)'][0],
        'IMF_Bz': MFI_data['BZ_(GSM)'][0],
        'SW_vel': PLM_data['VXGSE_PROTN_S/C'][PLM_data['VXGSE_PROTN_S/C'] < -50].mean(),
        'SW_dyn_press': proton_mass * 1e3 * PLM_data['DENS_PROTN_S/C'][PLM_data['DENS_PROTN_S/C'] < 300].mean() * 1e3 * ((PLM_data['VXGSE_PROTN_S/C'][PLM_data['VXGSE_PROTN_S/C'] < -50].mean() * 1e3) ** 2) * 1e9,
        'DST': DST_data['1-H_DST'][-1]
    }

    if offline:
        sw_params['bzimf'] = sw_params.pop('IMF_Bz')
        sw_params['byimf'] = sw_params.pop('IMF_By')
        sw_params['dst'] = sw_params.pop('DST')
        sw_params['pdyn'] = sw_params.pop('SW_dyn_press')
        sw_params['vswgse'] = [sw_params.pop('SW_vel'),PLM_data['VYGSE_PROTN_S/C'].mean(),PLM_data['VZGSE_PROTN_S/C'].mean()]

    return sw_params


def get_omni_sw_params(datetime, offline=False):
    """Get OMNI merged solar wind parameters from CDAWEB. Accurate up to 1 minute.

    Args:
        datetime (datetime): Datetime to get SW parameters for.

    Returns:
        Dict: SW Parameters:
            IMF_Bz, IMF_By, SW_dyn_press, SW_vel, DST
    """

    # Round (floor) to the minute
    sw_date = dt.datetime.strptime(
        datetime.strftime('%Y%m%d%H%M'), '%Y%m%d%H%M')
    # Get data from OMNI for the minute period of interest
    minutelies = ['BZ_GSM', 'BY_GSM', 'flow_speed', 'Pressure']
    datasets = cdas.get_datasets(
        'istp_public', idPattern='.*1MIN', labelPattern='.*OMNI.*')
    datasetID = datasets['DatasetDescription'][0]['Id']
    variables = cdas.get_variables('istp_public', datasetID)
    MIN_data = cdas.get_data(
        'sp_phys', datasetID, sw_date, sw_date + dt.timedelta(minutes=1), minutelies)
    # Get DST from OMNI (hourly)
    datasets = cdas.get_datasets(
        'istp_public', idPattern='.*MRG1HR', labelPattern='.*OMNI.*')
    datasetID = datasets['DatasetDescription'][0]['Id']
    variables = cdas.get_variables('istp_public', datasetID)
    DST_data = cdas.get_data(
        'sp_phys', datasetID, sw_date - dt.timedelta(hours=2), sw_date, ['DST1800'])

    # Build parameter dictionary
    sw_params = {
        'IMF_By': MIN_data['BY,_GSM'][0],
        'IMF_Bz': MIN_data['BZ,_GSM'][0],
        'SW_dyn_press': MIN_data['FLOW_PRESSURE'][0],
        'SW_vel': MIN_data['FLOW_SPEED,_GSE'][0],
        'DST': DST_data['1-H_DST'][-1]
    }

    if offline:
        sw_params['bzimf'] = sw_params.pop('IMF_Bz')
        sw_params['byimf'] = sw_params.pop('IMF_By')
        sw_params['dst'] = sw_params.pop('DST')
        sw_params['pdyn'] = sw_params.pop('SW_dyn_press')
        sw_params['vswgse'] = [sw_params.pop('SW_vel'),0,0]

    return sw_params


if __name__ == '__main__':
    test_date = dt.datetime(2016, 6, 1, 6, 42)
    aal_lat = [-83.58, -84.50, -84.42, -84.81, -83.32, -81.95]
    aal_lon = [89.26, 77.20, 57.96, 37.63, 12.97, 5.67]
    args = list(zip(aal_lat, aal_lon))[4]
    print(get_ccmc_tsyg_conj(test_date, direction='South-North', *args, **get_omni_sw_params(test_date)))
    exit()
