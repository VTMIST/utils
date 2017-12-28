# utils
Utility Functions for data acq, parsing, things of that nature

The following functions are made available in this lib:
* generate_available_dates(_year_, _system_, _subsystem_)

  Generates a list of the available dates in a year for a particular station's subsystem
* generate_yearly_masterlist(_year_, _system_, _subsystem_)

  Generates a masterlist of all data files in a given year for a particular station's subsystem
* read_housekeeping_list(list)

  Reads in housekeeping data and returns a pandas dataframe with datetime as the first column
* read_fluxgate_list(list)

  Reads in fluxgate data and returns a pandas dataframe without datetimes
* read_searchcoil_list(list)

  Reads in searchcoil data and returns a complete hex string and generated datetimes for the combined dataset
* parse_searchcoil(hexstring)

  Returns two numpy arrays of X and Y values based on imported hex string
* import_searchcoil(start=YYYY_MM_DD, end=YYYY_MM_DD)

  Returns datetimes, x samples, and y samples for a range of dates
* data_import_test()

  Tests the data gathering process
