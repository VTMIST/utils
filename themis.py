import wget
from pathlib import Path
from datetime import datetime
from spacepy import pycdf
import pandas as pd


def _get_themis_cdf(dt=datetime(2016, 5, 6), vehicle='the', dataset='sst'):

    date_string = dt.strftime('%Y%m%d')
    file_path = '/data/themis/{vehicle}/l2/{dataset}/{vehicle}_l2_{dataset}_{date}_v01.cdf'.format(vehicle=vehicle, dataset=dataset, date=date_string)
    print('Checking for file:', file_path)
    if not Path(file_path).is_file():
        # does not exist
        if not Path(file_path).parent.is_dir():
            # directory missing, create it
            try:
                Path(file_path).parent.mkdir(parents=True)
            except Exception:
                pass
        try:
            url = 'http://themis.ssl.berkeley.edu/data/themis/{vehicle}/l2/{dataset}/{year}/{vehicle}_l2_{dataset}_{date}_v01.cdf'.format(vehicle=vehicle,
                                                                                                                                          dataset=dataset,
                                                                                                                                          date=date_string,
                                                                                                                                          year=dt.year)
            print('No local copy, downloading ', url)
            wget.download(url, file_path)
            return pycdf.CDF(file_path)
        except Exception:
            print('Could not get cdf for', '{}_{}_{},'.format(vehicle, dataset, date_string), 'skipping...')
            return None
    else:
        # exists
        return pycdf.CDF(file_path)


def get_themis_dataframes(dt, vehicle='the', coord='gse'):

    assert (coord.lower() == 'gse') or (coord.lower() == 'gsm')

    # Collect the moments data first (ions)
    mom_cdf = _get_themis_cdf(dt, vehicle, 'mom')
    dti = pd.to_datetime(mom_cdf['{}_peim_time'.format(vehicle)][:], unit='s')
    df_ion_density = pd.DataFrame(data=mom_cdf['{}_peim_density'.format(vehicle)][:], index=dti, columns=['density'])
    df_ion_pressure = pd.DataFrame(data=mom_cdf['{}_peim_ptot'.format(vehicle)][:], index=dti, columns=['pressure'])
    df_ion_velocity = pd.DataFrame(data=mom_cdf['{}_peim_velocity_{}'.format(vehicle, coord)][:], index=dti, columns=['Vx', 'Vy', 'Vz'])

    fit_cdf = _get_themis_cdf(dt, vehicle, 'fit')
    dti = pd.to_datetime(fit_cdf['{}_fgs_time'.format(vehicle)][:], unit='s')
    df_fgs = pd.DataFrame(data=fit_cdf['{}_fgs_{}'.format(vehicle, coord)][:], index=dti, columns=['Bx', 'By', 'Bz'])

    return df_ion_density, df_ion_pressure, df_ion_velocity, df_fgs


if __name__ == '__main__':
    # print(_get_themis_cdf(datetime(2016, 5, 5), 'the', 'fit'))
    print(get_themis_dataframes(datetime(2016, 5, 15), 'tha'))
    exit()
