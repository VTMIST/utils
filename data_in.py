import aalpip
import halley
import ago

def data_import_test():
    """Test the import capability for 2017, sys_4, searchcoil

    Returns:
        NULL
    """
    daycheck = '2016_12_28'
    endcheck = '2017_01_02'
    print('Dates Available - 2017 - sys_4')
    print(aalpip.import_subsys(start=daycheck, end=endcheck, system=5, subsys='hskp'))
    return


if __name__ == '__main__':
    data_import_test()
    exit()
