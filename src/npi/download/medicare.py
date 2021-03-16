"""
https://www.cms.gov/Research-Statistics-Data-and-Systems/
Statistics-Trends-and-Reports/Medicare-Provider-Charge-Data/
Downloads/Prescriber_Methods.pdf

The Part D Prescriber PUF is based on beneficiaries enrolled in the Medicare
Part D prescription drug program who comprise approximately 70 percent of
all Medicare beneficiaries. Approximately two- thirds of Part D beneficiaries
are enrolled in stand-alone Prescription Drug Plans (PDP) with the remaining
one third enrolled in Medicare Advantage Prescription Drug (MAPD) plans.

The Part D Prescriber PUF is restricted to prescribers who had a valid NPI
and who were included on Medicare Part D PDEs submitted by the Part D plan
sponsors during the calendar year. The dataset contains information
predominantly from individual providers, but also includes a small
proportion of organizational providers, such as nursing homes,
group practices, non-physician practitioners, residential treatment
facilities, ambulatory surgery centers, and other providers.

Providers with fewer than 11 claims are not included in the data file.

https://www.cms.gov/Research-Statistics-Data-and-Systems/
Statistics-Trends-and-Reports/Medicare-Provider-Charge-Data/
Downloads/Medicare-Physician-and-Other-Supplier-PUF-Methodology.pdf

The Physician and Other Supplier PUF includes data for providers
that had a valid NPI and submitted Medicare Part B non-institutional
claims (excluding DMEPOS) during the 2012 through 2017 calendar years.
To protect the privacy of Medicare beneficiaries, any aggregated records
which are derived from 10 or fewer beneficiaries are excluded from the
Physician and Other Supplier PUF.

Two summary type tables have been created to supplement the information
reported in the Physician and Other Supplier PUF: 1) aggregated
information by physician or other supplier (NPI)
"""


import glob
import os
import re

from ..constants import (API_SOURCE_PATH, PART_B_RAW_DIR, PART_B_STUB,
                         PART_B_STUB_SUM, PART_D_OPI_RAW_DIR, PART_D_OPI_STUB,
                         PART_D_RAW_DIR, PART_D_RAW_DIR_NEW,
                         PART_D_SOURCE_PATH)
from ..utils.utils import singleton, unzip_checkfirst, wget_checkfirst


def list_part_d_files(Drug=True, destination_dir=PART_D_RAW_DIR):
    drugfiles = glob.glob(destination_dir + '/*_DRUG_*/*.txt')
    if destination_dir == PART_D_RAW_DIR:
        if Drug:
            files = drugfiles
        else:
            allfiles = glob.glob(destination_dir + '/*/*.txt')
            files = [x for x in allfiles if x not in drugfiles]
        return [(x, 2000 + int(singleton(set(re.findall(r'[0-9][0-9]', x)))))
                for x in files]
    else:
        files = glob.glob(destination_dir + '/*')
        files_sum = glob.glob(destination_dir + '/*Summary*')
        files = [x for x in files if x not in files_sum]
        if Drug:
            return [(x, int(singleton(set(re.findall(r'[0-9][0-9][0-9][0-9]', x))))) for x in files]
        else:
            return [(x, int(singleton(set(re.findall(r'[0-9][0-9][0-9][0-9]', x))))) for x in files_sum]


def get_part_d_data(yearrange=range(13, 19),
                    source_path=PART_D_SOURCE_PATH,
                    destination_dir=PART_D_RAW_DIR):
    '''
    Automatically goes ahead and checks for 2018 data, which should be released
    in may. If "Failed" appears, that means it hasn't been posted yet.
    '''
    paths = ([os.path.join(source_path,
                           f'PartD_Prescriber_PUF_NPI_DRUG_{year}.zip')
             for year in yearrange] +
             [os.path.join(source_path, f'PartD_Prescriber_PUF_NPI_{year}.zip')
              for year in yearrange])
    outpaths = [wget_checkfirst(x, to_dir=destination_dir) for x in paths]
    outpaths = [x for x in outpaths if x]
    [unzip_checkfirst(x, to_dir=os.path.splitext(x)[0]) for x in outpaths]


def list_part_b_files(filestub, destination_dir=PART_B_RAW_DIR):
    """Returns part D files and their associated years"""
    desti_name = os.path.join(destination_dir, filestub)
    return [(x, (int(x.replace(desti_name, '')
                      .replace('.csv', '')
                      .replace('PUF_', '')
                      .replace('CY', ''))))
            for x
            in glob.glob(os.path.join(destination_dir, filestub) + '*.csv')]


def get_part_d_data_new(yearrange=range(2013, 2019),
                        source_path=API_SOURCE_PATH,
                        destination_dir=PART_D_RAW_DIR_NEW):
    file_id_dict_puf_npi_drug = {2018: 'mhdd-npjx',
                                 2017: '77gb-8z53',
                                 2016: 'yvpj-pmj2',
                                 2015: '3z4d-vmhm',
                                 2014: '465c-49pb',
                                 2013: '4uvc-gbfz'}
    file_id_dict_puf_npi = {2018: 'icvy-hptt',
                            2017: 'psut-35i4',
                            2016: 'c9xz-7zpd',
                            2015: 'qywy-pajd',
                            2014: 'mxq9-aiiw',
                            2013: 'cks9-s5d9'}

    already_gotten = [x[1] for x in list_part_d_files(Drug=True, destination_dir=destination_dir)]
    [wget_checkfirst(source_path % x, to_dir=destination_dir)
     for y, x in file_id_dict_puf_npi_drug.items() if y not in already_gotten]
    already_gotten = [x[1] for x in list_part_d_files(Drug=False, destination_dir=destination_dir)]
    [wget_checkfirst(source_path % x, to_dir=destination_dir)
     for y, x in file_id_dict_puf_npi.items() if y not in already_gotten]


def get_part_b_data(yearrange=range(2012, 2019),
                    source_path=API_SOURCE_PATH,
                    destination_dir=PART_B_RAW_DIR):
    """
    """
    file_id_dict_puf = {2018: 'hczc-ufy5',
                        2017: 'fs4p-t5eq',
                        2016: 'utc4-f9xp',
                        2015: 'sk9b-znav',
                        2014: 'ee7f-sh97',
                        2013: 'din4-7td8',
                        2012: 'jzd2-pt4g'}
    # have to check what's been downloaded by hand because the auto-detect
    # filename does not work for these files
    already_gotten = [x[1] for x in list_part_b_files(PART_B_STUB)]
    [wget_checkfirst(source_path % x, to_dir=destination_dir)
     for y, x in file_id_dict_puf.items() if y not in already_gotten]

    file_id_prof_sum = {2018: '5fr6-cch3',
                        2017: 'n5qc-ua94',
                        2016: '85jw-maq9',
                        2015: 'p3uv-6dv4',
                        2014: '4a3h-46r6',
                        2013: '3zix-38y3',
                        2012: 'i587-8mbi'}
    # have to check what's been downloaded by hand because the auto-detect
    # filename does not work for these files
    already_gotten = [x[1] for x in list_part_b_files(PART_B_STUB_SUM)]
    [wget_checkfirst(source_path % x, to_dir=destination_dir)
     for y, x in file_id_prof_sum.items() if y not in already_gotten]


def list_part_d_opi_files(destination_dir=PART_D_OPI_RAW_DIR,
                          filestub=PART_D_OPI_STUB):
    """Returns part D files and their associated years"""
    desti_name = os.path.join(destination_dir, filestub)
    return [(x, (int(x.replace(desti_name, '').replace('.csv', ''))))
            for x
            in glob.glob(os.path.join(destination_dir, filestub) + '*.csv')]


def get_part_d_opi_data(yearrange=range(2013, 2018),
                        source_path=API_SOURCE_PATH,
                        destination_dir=PART_D_OPI_RAW_DIR,
                        filestub=PART_D_OPI_STUB):
    """
    """
    file_id_dict_opi = {2017: 'sakz-a2rp',
                        2016: '6wg9-kwip',
                        2015: '6i2k-7h8p',
                        2014: 'e4ka-3ncx',
                        2013: 'yb2j-f3fp'}
    # have to check what's been downloaded by hand because the auto-detect
    # filename does not work for these files
    already_gotten = [x[1] for x in list_part_d_opi_files()]
    [wget_checkfirst(source_path % x, to_dir=destination_dir)
     for y, x in file_id_dict_opi.items() if y not in already_gotten]


def main():
    get_part_d_data()
    get_part_b_data()
    get_part_d_opi_data()
