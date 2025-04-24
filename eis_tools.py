import os,argparse

from ftplib import FTP
from netCDF4 import Dataset
from datetime import datetime
from datetime import timedelta
from calendar import monthrange


parser = argparse.ArgumentParser(description='Check upload')
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-m", "--mode", help="Mode.", type=str, required=True, choices=['CHECK_CNR_FILES'])
parser.add_argument("-c", "--config_file", help="Configuration file")
parser.add_argument("-s", "--section", help="Reference section in the configuration file")
args = parser.parse_args()


def main_test():
    # b = check_var()
    b = checking_sizes()
    usuario = 'lgonzalezvilas'
    password = 'MegaRoma17!'
    ftp_here = FTP('nrt-dev.cmems-du.eu', user=usuario, passwd=password)
    # ftp_here = FTP('my.cmems-du.eu', user=usuario, passwd=password)
    ftp_here.login(user=usuario, passwd=password)

    # product = 'OCEANCOLOUR_MED_CHL_L4_REP_OBSERVATIONS_009_078'
    # product = 'OCEANCOLOUR_MED_OPTICS_L3_REP_OBSERVATIONS_009_095'
    # datasets = ['dataset-oc-med-chl-multi-l4-interp_1km_daily-rep']
    # datasets = ['dataset-oc-med-opt-multi-l3-adg443_1km_daily-rep-v02',
    #             'dataset-oc-med-opt-multi-l3-aph443_1km_daily-rep-v02',
    #             'dataset-oc-med-opt-multi-l3-bbp443_1km_daily-rep-v02',
    #             'dataset-oc-med-opt-multi-l3-kd490_1km_daily-rep-v02',
    #             'dataset-oc-med-opt-multi-l3-rrs412_1km_daily-rep-v02',
    #             'dataset-oc-med-opt-multi-l3-rrs443_1km_daily-rep-v02',
    #             'dataset-oc-med-opt-multi-l3-rrs490_1km_daily-rep-v02',
    #             'dataset-oc-med-opt-multi-l3-rrs510_1km_daily-rep-v02',
    #             'dataset-oc-med-opt-multi-l3-rrs555_1km_daily-rep-v02',
    #             'dataset-oc-med-opt-multi-l3-rrs670_1km_daily-rep-v02']

    # Monthly rep
    # product = 'OCEANCOLOUR_BS_CHL_L4_REP_OBSERVATIONS_009_079'
    # datasets = ['dataset-oc-bs-chl-multi-l4-chl_1km_monthly-rep-v02']
    #
    # product = 'OCEANCOLOUR_MED_CHL_L4_REP_OBSERVATIONS_009_078'
    # datasets = ['dataset-oc-med-chl-multi-l4-chl_1km_monthly-rep-v02']

    # ref = 38
    # product, datasets = get_products_and_datasets_dailynrt(ref)

    # ref = 41
    # sensor = 'olci'
    # product, datasets = get_products_and_datasets_monthlynrt(ref, sensor)
    #

    # product = 'OCEANCOLOUR_BAL_BGC_L4_NRT_009_332'
    # datasets = c'cmems_obs-oc_bal_bgc-chl_nrt_olci-l4-300m_P1M-m']
    # product = 'OCEANCOLOUR_BS_OPTICS_L4_NRT_OBSERVATIONS_009_043'
    # datasets = ['dataset-oc-bs-opt-olci-l4-kd490_300m_monthly-rt']
    #
    product = 'OCEANCOLOUR_EUR_CHL_L3_NRT_OBSERVATIONS_009_050'
    datasets = ['dataset-oc-eur-chl-multi-l3-chl_1km_daily-rt-v02']

    #
    #
    #
    file_out = f'/mnt/d/luis/EUR_product.txt'
    # file_out = f'/mnt/d/luis/MONTHLY_NRT_{ref}_{sensor}.txt'
    with open(file_out, 'w') as f:
        for dataset in datasets:
            svalue = get_info_dataset(ftp_here, product, dataset)
            f.write(svalue)
            f.write('\n')

    # products = [
    #     'OCEANCOLOUR_BS_CHL_L3_NRT_OBSERVATIONS_009_044',
    #     'OCEANCOLOUR_BS_OPTICS_L3_NRT_OBSERVATIONS_009_042',
    #     'OCEANCOLOUR_BS_OPTICS_L3_NRT_OBSERVATIONS_009_042',
    #     'OCEANCOLOUR_BS_OPTICS_L3_NRT_OBSERVATIONS_009_042',
    #     'OCEANCOLOUR_BS_OPTICS_L3_NRT_OBSERVATIONS_009_042',
    #     'OCEANCOLOUR_BS_OPTICS_L3_NRT_OBSERVATIONS_009_042',
    #     'OCEANCOLOUR_BS_OPTICS_L3_NRT_OBSERVATIONS_009_042',
    #     'OCEANCOLOUR_BS_OPTICS_L3_NRT_OBSERVATIONS_009_042',
    #     'OCEANCOLOUR_BS_OPTICS_L3_NRT_OBSERVATIONS_009_042',
    #     'OCEANCOLOUR_BS_OPTICS_L3_NRT_OBSERVATIONS_009_042',
    #     'OCEANCOLOUR_BS_OPTICS_L3_NRT_OBSERVATIONS_009_042',
    #     'OCEANCOLOUR_BS_OPTICS_L3_NRT_OBSERVATIONS_009_042',
    #     'OCEANCOLOUR_BS_OPTICS_L3_NRT_OBSERVATIONS_009_042',
    #     'OCEANCOLOUR_MED_CHL_L3_NRT_OBSERVATIONS_009_040',
    #     'OCEANCOLOUR_MED_OPTICS_L3_NRT_OBSERVATIONS_009_038',
    #     'OCEANCOLOUR_MED_OPTICS_L3_NRT_OBSERVATIONS_009_038',
    #     'OCEANCOLOUR_MED_OPTICS_L3_NRT_OBSERVATIONS_009_038',
    #     'OCEANCOLOUR_MED_OPTICS_L3_NRT_OBSERVATIONS_009_038',
    #     'OCEANCOLOUR_MED_OPTICS_L3_NRT_OBSERVATIONS_009_038',
    #     'OCEANCOLOUR_MED_OPTICS_L3_NRT_OBSERVATIONS_009_038',
    #     'OCEANCOLOUR_MED_OPTICS_L3_NRT_OBSERVATIONS_009_038',
    #     'OCEANCOLOUR_MED_OPTICS_L3_NRT_OBSERVATIONS_009_038',
    #     'OCEANCOLOUR_MED_OPTICS_L3_NRT_OBSERVATIONS_009_038',
    #     'OCEANCOLOUR_MED_OPTICS_L3_NRT_OBSERVATIONS_009_038',
    #     'OCEANCOLOUR_MED_OPTICS_L3_NRT_OBSERVATIONS_009_038',
    #     'OCEANCOLOUR_MED_OPTICS_L3_NRT_OBSERVATIONS_009_038']
    # datasets = [
    #     'dataset-oc-bs-chl-olci_a-l3-chl_1km_daily-rt-v02',
    #     'dataset-oc-bs-opt-olci_a-l3-kd490_1km_daily-rt-v02',
    #     'dataset-oc-bs-opt-olci_a-l3-rrs400_1km_daily-rt-v02',
    #     'dataset-oc-bs-opt-olci_a-l3-rrs412_1km_daily-rt-v02',
    #     'dataset-oc-bs-opt-olci_a-l3-rrs443_1km_daily-rt-v02',
    #     'dataset-oc-bs-opt-olci_a-l3-rrs490_1km_daily-rt-v02',
    #     'dataset-oc-bs-opt-olci_a-l3-rrs510_1km_daily-rt-v02',
    #     'dataset-oc-bs-opt-olci_a-l3-rrs560_1km_daily-rt-v02',
    #     'dataset-oc-bs-opt-olci_a-l3-rrs620_1km_daily-rt-v02',
    #     'dataset-oc-bs-opt-olci_a-l3-rrs665_1km_daily-rt-v02',
    #     'dataset-oc-bs-opt-olci_a-l3-rrs674_1km_daily-rt-v02',
    #     'dataset-oc-bs-opt-olci_a-l3-rrs681_1km_daily-rt-v02',
    #     'dataset-oc-bs-opt-olci_a-l3-rrs709_1km_daily-rt-v02',
    #     'dataset-oc-med-chl-olci_a-l3-chl_1km_daily-rt-v02',
    #     'dataset-oc-med-opt-olci_a-l3-kd490_1km_daily-rt-v02',
    #     'dataset-oc-med-opt-olci_a-l3-rrs400_1km_daily-rt-v02',
    #     'dataset-oc-med-opt-olci_a-l3-rrs412_1km_daily-rt-v02',
    #     'dataset-oc-med-opt-olci_a-l3-rrs443_1km_daily-rt-v02',
    #     'dataset-oc-med-opt-olci_a-l3-rrs490_1km_daily-rt-v02',
    #     'dataset-oc-med-opt-olci_a-l3-rrs510_1km_daily-rt-v02',
    #     'dataset-oc-med-opt-olci_a-l3-rrs560_1km_daily-rt-v02',
    #     'dataset-oc-med-opt-olci_a-l3-rrs620_1km_daily-rt-v02',
    #     'dataset-oc-med-opt-olci_a-l3-rrs665_1km_daily-rt-v02',
    #     'dataset-oc-med-opt-olci_a-l3-rrs674_1km_daily-rt-v02',
    #     'dataset-oc-med-opt-olci_a-l3-rrs681_1km_daily-rt-v02',
    #     'dataset-oc-med-opt-olci_a-l3-rrs709_1km_daily-rt-v02']

    # products = [
    #     'OCEANCOLOUR_MED_OPTICS_L4_NRT_OBSERVATIONS_009_039',
    #     'OCEANCOLOUR_BS_CHL_L4_NRT_OBSERVATIONS_009_045',
    #     'OCEANCOLOUR_BS_OPTICS_L4_NRT_OBSERVATIONS_009_043',
    #     'OCEANCOLOUR_MED_CHL_L4_NRT_OBSERVATIONS_009_041']
    #
    # datasets = [
    #     'dataset-oc-med-opt-olci_a-l4-kd490_1km_monthly-rt-v02',
    #     'dataset-oc-bs-chl-olci_a-l4-chl_1km_monthly-rt-v02',
    #     'dataset-oc-bs-opt-olci_a-l4-kd490_1km_monthly-rt-v02',
    #     'dataset-oc-med-chl-olci_a-l4-chl_1km_monthly-rt-v02']
    # products = [
    #     'OCEANCOLOUR_BAL_CHL_L3_NRT_OBSERVATIONS_009_049',
    #     'OCEANCOLOUR_BAL_OPTICS_L3_NRT_OBSERVATIONS_009_048',
    #     'OCEANCOLOUR_BAL_OPTICS_L3_NRT_OBSERVATIONS_009_048',
    #     'OCEANCOLOUR_BAL_OPTICS_L3_NRT_OBSERVATIONS_009_048',
    #     'OCEANCOLOUR_BAL_OPTICS_L3_NRT_OBSERVATIONS_009_048',
    #     'OCEANCOLOUR_BAL_OPTICS_L3_NRT_OBSERVATIONS_009_048',
    #     'OCEANCOLOUR_BAL_OPTICS_L3_NRT_OBSERVATIONS_009_048',
    #     'OCEANCOLOUR_BAL_OPTICS_L3_NRT_OBSERVATIONS_009_048',
    #     'OCEANCOLOUR_BAL_OPTICS_L3_NRT_OBSERVATIONS_009_048',
    #     'OCEANCOLOUR_BAL_OPTICS_L3_NRT_OBSERVATIONS_009_048',
    #     'OCEANCOLOUR_BAL_OPTICS_L3_NRT_OBSERVATIONS_009_048',
    #     'OCEANCOLOUR_BAL_OPTICS_L3_NRT_OBSERVATIONS_009_048',
    #     'OCEANCOLOUR_BAL_OPTICS_L3_NRT_OBSERVATIONS_009_048',
    #     'OCEANCOLOUR_BAL_OPTICS_L3_NRT_OBSERVATIONS_009_048',
    #     'OCEANCOLOUR_BAL_OPTICS_L3_NRT_OBSERVATIONS_009_048',
    #     'OCEANCOLOUR_BAL_OPTICS_L3_NRT_OBSERVATIONS_009_048',
    #     'OCEANCOLOUR_BAL_OPTICS_L3_NRT_OBSERVATIONS_009_048'
    # ]
    # datasets = [
    #     'dataset-oc-bal-chl-olci_a-l3-nn_1km_daily-rt-v02',
    #     'dataset-oc-bal-opt-olci_a-l3-adg440_1km_daily-rt-v02',
    #     'dataset-oc-bal-opt-olci_a-l3-adg440_1km_daily-rt-v02',
    #     'dataset-oc-bal-opt-olci_a-l3-aph440_1km_daily-rt-v02',
    #     'dataset-oc-bal-opt-olci_a-l3-bbp510_1km_daily-rt-v02',
    #     'dataset-oc-bal-opt-olci_a-l3-kd490_1km_daily-rt-v02',
    #     'dataset-oc-bal-opt-olci_a-l3-rrs400_1km_daily-rt-v02',
    #     'dataset-oc-bal-opt-olci_a-l3-rrs412_1km_daily-rt-v02',
    #     'dataset-oc-bal-opt-olci_a-l3-rrs443_1km_daily-rt-v02',
    #     'dataset-oc-bal-opt-olci_a-l3-rrs490_1km_daily-rt-v02',
    #     'dataset-oc-bal-opt-olci_a-l3-rrs510_1km_daily-rt-v02',
    #     'dataset-oc-bal-opt-olci_a-l3-rrs560_1km_daily-rt-v02',
    #     'dataset-oc-bal-opt-olci_a-l3-rrs620_1km_daily-rt-v02',
    #     'dataset-oc-bal-opt-olci_a-l3-rrs665_1km_daily-rt-v02',
    #     'dataset-oc-bal-opt-olci_a-l3-rrs674_1km_daily-rt-v02',
    #     'dataset-oc-bal-opt-olci_a-l3-rrs681_1km_daily-rt-v02',
    #     'dataset-oc-bal-opt-olci_a-l3-rrs709_1km_daily-rt-v02'
    # ]
    # file_out = f'/mnt/d/luis/DEPRECATED_DAILY_2.txt'
    # with open(file_out, 'w') as f:
    #     for i in range(len(products)):
    #         product = products[i]
    #         dataset = datasets[i]
    #         svalue = get_info_complete_dataset_daily(ftp_here,product,dataset,2016,4,2021,5)
    #         f.write(svalue)
    #         f.write('\n')

    print('DONE')

    return True


def get_products_and_datasets_dailynrt(ref):
    if ref == 48:
        product = 'OCEANCOLOUR_BAL_OPTICS_L3_NRT_OBSERVATIONS_009_048'
        datasets = ['dataset-oc-bal-opt-olci-l3-adg440_300m_daily-rt',
                    'dataset-oc-bal-opt-olci-l3-aph440_300m_daily-rt',
                    'dataset-oc-bal-opt-olci-l3-bbp510_300m_daily-rt',
                    'dataset-oc-bal-opt-olci-l3-kd490_300m_daily-rt',
                    'dataset-oc-bal-opt-olci-l3-rrs400_300m_daily-rt',
                    'dataset-oc-bal-opt-olci-l3-rrs412_300m_daily-rt',
                    'dataset-oc-bal-opt-olci-l3-rrs443_300m_daily-rt',
                    'dataset-oc-bal-opt-olci-l3-rrs490_300m_daily-rt',
                    'dataset-oc-bal-opt-olci-l3-rrs510_300m_daily-rt',
                    'dataset-oc-bal-opt-olci-l3-rrs560_300m_daily-rt',
                    'dataset-oc-bal-opt-olci-l3-rrs620_300m_daily-rt',
                    'dataset-oc-bal-opt-olci-l3-rrs665_300m_daily-rt',
                    'dataset-oc-bal-opt-olci-l3-rrs674_300m_daily-rt',
                    'dataset-oc-bal-opt-olci-l3-rrs681_300m_daily-rt',
                    'dataset-oc-bal-opt-olci-l3-rrs709_300m_daily-rt']

    if ref == 44:
        product = 'OCEANCOLOUR_BS_CHL_L3_NRT_OBSERVATIONS_009_044'
        datasets = ['dataset-oc-bs-chl-multi-l3-chl_1km_daily-rt-v02',
                    'dataset-oc-bs-chl-olci-l3-chl_300m_daily-rt']

    if ref == 45:
        product = 'OCEANCOLOUR_BS_CHL_L4_NRT_OBSERVATIONS_009_045'
        datasets = ['dataset-oc-bs-chl-multi-l4-interp_1km_daily-rt-v02']

    if ref == 42:
        product = 'OCEANCOLOUR_BS_OPTICS_L3_NRT_OBSERVATIONS_009_042'
        datasets = ['dataset-oc-bs-opt-multi-l3-adg443_1km_daily-rt-v02',
                    'dataset-oc-bs-opt-multi-l3-aph443_1km_daily-rt-v02',
                    'dataset-oc-bs-opt-multi-l3-bbp443_1km_daily-rt-v02',
                    'dataset-oc-bs-opt-multi-l3-kd490_1km_daily-rt-v02',
                    'dataset-oc-bs-opt-multi-l3-rrs412_1km_daily-rt-v02',
                    'dataset-oc-bs-opt-multi-l3-rrs443_1km_daily-rt-v02',
                    'dataset-oc-bs-opt-multi-l3-rrs490_1km_daily-rt-v02',
                    'dataset-oc-bs-opt-multi-l3-rrs510_1km_daily-rt-v02',
                    'dataset-oc-bs-opt-multi-l3-rrs555_1km_daily-rt-v02',
                    'dataset-oc-bs-opt-multi-l3-rrs670_1km_daily-rt-v02',
                    'dataset-oc-bs-opt-olci-l3-kd490_300m_daily-rt',
                    'dataset-oc-bs-opt-olci-l3-rrs400_300m_daily-rt',
                    'dataset-oc-bs-opt-olci-l3-rrs412_300m_daily-rt',
                    'dataset-oc-bs-opt-olci-l3-rrs443_300m_daily-rt',
                    'dataset-oc-bs-opt-olci-l3-rrs490_300m_daily-rt',
                    'dataset-oc-bs-opt-olci-l3-rrs510_300m_daily-rt',
                    'dataset-oc-bs-opt-olci-l3-rrs560_300m_daily-rt',
                    'dataset-oc-bs-opt-olci-l3-rrs620_300m_daily-rt',
                    'dataset-oc-bs-opt-olci-l3-rrs665_300m_daily-rt',
                    'dataset-oc-bs-opt-olci-l3-rrs674_300m_daily-rt',
                    'dataset-oc-bs-opt-olci-l3-rrs681_300m_daily-rt',
                    'dataset-oc-bs-opt-olci-l3-rrs709_300m_daily-rt']
    if ref == 40:
        product = 'OCEANCOLOUR_MED_CHL_L3_NRT_OBSERVATIONS_009_040'
        datasets = ['dataset-oc-med-chl-multi-l3-chl_1km_daily-rt-v02', 'dataset-oc-med-chl-olci-l3-chl_300m_daily-rt']

    if ref == 41:
        product = 'OCEANCOLOUR_MED_CHL_L4_NRT_OBSERVATIONS_009_041'
        datasets = ['dataset-oc-med-chl-multi-l4-interp_1km_daily-rt-v02']

    if ref == 38:
        product = 'OCEANCOLOUR_MED_OPTICS_L3_NRT_OBSERVATIONS_009_038'

        datasets = ['dataset-oc-med-opt-multi-l3-adg443_1km_daily-rt-v02',
                    'dataset-oc-med-opt-multi-l3-aph443_1km_daily-rt-v02',
                    'dataset-oc-med-opt-multi-l3-bbp443_1km_daily-rt-v02',
                    'dataset-oc-med-opt-multi-l3-kd490_1km_daily-rt-v02',
                    'dataset-oc-med-opt-multi-l3-rrs412_1km_daily-rt-v02',
                    'dataset-oc-med-opt-multi-l3-rrs443_1km_daily-rt-v02',
                    'dataset-oc-med-opt-multi-l3-rrs490_1km_daily-rt-v02',
                    'dataset-oc-med-opt-multi-l3-rrs510_1km_daily-rt-v02',
                    'dataset-oc-med-opt-multi-l3-rrs555_1km_daily-rt-v02',
                    'dataset-oc-med-opt-multi-l3-rrs670_1km_daily-rt-v02',
                    'dataset-oc-med-opt-olci-l3-kd490_300m_daily-rt',
                    'dataset-oc-med-opt-olci-l3-rrs400_300m_daily-rt',
                    'dataset-oc-med-opt-olci-l3-rrs412_300m_daily-rt',
                    'dataset-oc-med-opt-olci-l3-rrs443_300m_daily-rt',
                    'dataset-oc-med-opt-olci-l3-rrs490_300m_daily-rt',
                    'dataset-oc-med-opt-olci-l3-rrs510_300m_daily-rt',
                    'dataset-oc-med-opt-olci-l3-rrs560_300m_daily-rt',
                    'dataset-oc-med-opt-olci-l3-rrs620_300m_daily-rt',
                    'dataset-oc-med-opt-olci-l3-rrs665_300m_daily-rt',
                    'dataset-oc-med-opt-olci-l3-rrs674_300m_daily-rt',
                    'dataset-oc-med-opt-olci-l3-rrs681_300m_daily-rt',
                    'dataset-oc-med-opt-olci-l3-rrs709_300m_daily-rt']
    return product, datasets


def get_products_and_datasets_monthlynrt(ref, sensor):
    if ref == 45:
        if sensor == 'multi':
            product = 'OCEANCOLOUR_BS_CHL_L4_NRT_OBSERVATIONS_009_045'  # (01 / 01 / 2021)
            datasets = ['dataset-oc-bs-chl-multi-l4-chl_1km_monthly-rt-v02']
        if sensor == 'olci':
            product = 'OCEANCOLOUR_BS_CHL_L4_NRT_OBSERVATIONS_009_045'  # (15 / 02 / 2021)
            datasets = ['dataset-oc-bs-chl-olci-l4-chl_300m_monthly-rt']
    if ref == 43:
        if sensor == 'multi':
            product = 'OCEANCOLOUR_BS_OPTICS_L4_NRT_OBSERVATIONS_009_043'  # (01 / 01 / 2021)
            datasets = ['dataset-oc-bs-opt-multi-l4-kd490_1km_monthly-rt-v02']
        if sensor == 'olci':
            product = 'OCEANCOLOUR_BS_OPTICS_L4_NRT_OBSERVATIONS_009_043'  # (01 / 03 / 2021)
            datasets = ['dataset-oc-bs-opt-olci-l4-kd490_300m_monthly-rt']
    if ref == 39:
        if sensor == 'multi':
            product = 'OCEANCOLOUR_MED_OPTICS_L4_NRT_OBSERVATIONS_009_039'  # (01 / 01 / 2021)
            datasets = ['dataset-oc-med-opt-multi-l4-kd490_1km_monthly-rt-v02']
        if sensor == 'olci':
            product = 'OCEANCOLOUR_MED_OPTICS_L4_NRT_OBSERVATIONS_009_039'  # (01 / 03 / 2021)
            datasets = ['dataset-oc-med-opt-olci-l4-kd490_300m_monthly-rt']
    if ref == 41:
        if sensor == 'multi':
            product = 'OCEANCOLOUR_MED_CHL_L4_NRT_OBSERVATIONS_009_041'  # (01/01/2021)
            datasets = ['dataset-oc-med-chl-multi-l4-chl_1km_monthly-rt-v02']
        if sensor == 'olci':
            product = 'OCEANCOLOUR_MED_CHL_L4_NRT_OBSERVATIONS_009_041'  # (01 / 03 / 2021)
            datasets = ['dataset-oc-med-chl-olci-l4-chl_300m_monthly-rt']

    return product, datasets


def get_info_dataset(ftp_here, product, dataset):
    path = os.path.join('/Core', product, dataset)
    tam_all_months = []
    month_all_months = []
    year_all_months = []
    for y in range(2017, 2022):
        for m in range(1, 13):
            path_here = os.path.join(path, str(y), str(m).zfill(2))
            print(path_here)
            # if m < 9 and y == 1997:
            #     continue
            # # if m >= 2 and y >= 2022:
            # #     continue
            # if m >= 7 and y == 2021:
            #     continue
            # if y == 2022:
            #     continue
            tam_month = 0
            tam_month_mb = 0
            for s in ftp_here.nlst(path_here):
                t = ftp_here.size(s)
                tkb = t / 1024
                tmb = tkb / 1024
                tgb = tmb / 1024
                tam_month = tam_month + tgb
                tam_month_mb = tam_month_mb + tmb
            print('------------------------------------------------------------------>', y, m, tam_month_mb, tam_month)
            tam_all_months.append(tam_month)
            month_all_months.append(m)
            year_all_months.append(y)

    total_pid = 0
    n_pid = 0
    sum_months = [0] * 12
    n_months = [0] * 12
    avg_months = [0] * 12

    for i in range(len(tam_all_months)):
        yhere = year_all_months[i]
        mhere = month_all_months[i]
        there = tam_all_months[i]
        # if yhere >= 2021 or (yhere == 2022 and mhere == 1):
        if (1998 <= yhere <= 2020) or (yhere == 1997 and mhere >= 9) or (yhere == 2021 and mhere <= 6):
            total_pid = total_pid + there
            n_pid = n_pid + 1
            sum_months[mhere - 1] = sum_months[mhere - 1] + there
            n_months[mhere - 1] = n_months[mhere - 1] + 1

    sum_feb_jun = 0
    for m in range(12):
        avg_months[m] = sum_months[m] / n_months[m]
        sum_feb_jun = sum_feb_jun + avg_months[m]
        # mhere = m + 1
        # if 2 <= mhere <= 6:
        #     sum_feb_jun = sum_feb_jun + avg_months[m]
    avg_m = total_pid / n_pid
    print('NPid es: ', n_pid, 'avg_m', avg_m)
    total_18 = avg_m * 298
    total_eq = total_pid + sum_feb_jun

    svalue = f'{avg_m},{total_pid},{sum_feb_jun},{total_18},{total_eq}'
    for tm in avg_months:
        svalue = f'{svalue},{tm}'

    print(total_pid, sum_feb_jun, avg_m, total_18, total_eq)
    return svalue


def get_info_dataset_daily(ftp_here, product, dataset):
    path = os.path.join('/Core', product, dataset)
    tam_all_months = []
    month_all_months = []
    year_all_months = []
    for y in range(2021, 2023):
        for m in range(1, 13):
            path_here = os.path.join(path, str(y), str(m).zfill(2))
            print(path_here)

            if m >= 2 and y >= 2022:
                continue

            tam_month = 0
            tam_month_mb = 0
            for s in ftp_here.nlst(path_here):
                t = ftp_here.size(s)
                tkb = t / 1024
                tmb = tkb / 1024
                tgb = tmb / 1024
                tam_month = tam_month + tgb
                tam_month_mb = tam_month_mb + tmb
            print('------------------------------------------------------------------>', y, m, tam_month_mb, tam_month)
            tam_all_months.append(tam_month)
            month_all_months.append(m)
            year_all_months.append(y)

    total_pid = 0
    n_pid = 0
    sum_months = [0] * 12
    n_months = [0] * 12
    avg_months = [0] * 12

    for i in range(len(tam_all_months)):
        yhere = year_all_months[i]
        mhere = month_all_months[i]
        there = tam_all_months[i]
        if yhere >= 2021 or (yhere == 2022 and mhere == 1):
            total_pid = total_pid + there
            n_pid = n_pid + 1
            sum_months[mhere - 1] = sum_months[mhere - 1] + there
            n_months[mhere - 1] = n_months[mhere - 1] + 1

    sum_feb_jun = 0
    for m in range(12):
        avg_months[m] = sum_months[m] / n_months[m]
        mhere = m + 1
        if 2 <= mhere <= 6:
            sum_feb_jun = sum_feb_jun + avg_months[m]
    avg_m = total_pid / n_pid
    print('NPid es: ', n_pid, 'avg_m', avg_m)
    total_18 = avg_m * 18
    total_eq = total_pid + sum_feb_jun

    svalue = f'{avg_m},{total_pid},{sum_feb_jun},{total_18},{total_eq}'
    for tm in avg_months:
        svalue = f'{svalue},{tm}'

    print(total_pid, sum_feb_jun, avg_m, total_18, total_eq)
    return svalue


def get_info_dataset_monthly_rep(ftp_here, product, dataset):
    path = os.path.join('/Core', product, dataset)
    tam_all_months = []
    month_all_months = []
    year_all_months = []
    for y in range(1997, 2022):
        path_here = os.path.join(path, str(y))
        print(path_here)

        tam_month = 0
        tam_month_mb = 0
        for s in ftp_here.nlst(path_here):
            sn = s.split('/')[-1]
            m = int(sn[4:6])
            t = ftp_here.size(s)
            tkb = t / 1024
            tmb = tkb / 1024
            tgb = tmb / 1024
            tam_month = tam_month + tgb
            tam_month_mb = tam_month_mb + tmb
            print('------------------------------------------------------------------>', y, m, tam_month_mb, tam_month)
            tam_all_months.append(tam_month)
            month_all_months.append(m)
            year_all_months.append(y)

    total_pid = 0
    n_pid = 0
    sum_months = [0] * 12
    n_months = [0] * 12
    avg_months = [0] * 12

    for i in range(len(tam_all_months)):
        yhere = year_all_months[i]
        mhere = month_all_months[i]
        there = tam_all_months[i]
        if (1998 <= yhere <= 2020) or (yhere == 1997 and mhere >= 9) or (yhere == 2021 and mhere <= 6):
            total_pid = total_pid + there
            n_pid = n_pid + 1
            sum_months[mhere - 1] = sum_months[mhere - 1] + there
            n_months[mhere - 1] = n_months[mhere - 1] + 1

    sum_feb_jun = 0
    for m in range(12):
        avg_months[m] = sum_months[m] / n_months[m]
        sum_feb_jun = sum_feb_jun + avg_months[m]
        # mhere = m + 1
        # if 2 <= mhere <= 6:
        #     sum_feb_jun = sum_feb_jun + avg_months[m]
    avg_m = total_pid / n_pid
    print('NPid es: ', n_pid, 'avg_m', avg_m)
    total_18 = avg_m * 298
    total_eq = total_pid + sum_feb_jun

    svalue = f'{avg_m},{total_pid},{sum_feb_jun},{total_18},{total_eq}'
    for tm in avg_months:
        svalue = f'{svalue},{tm}'

    print(total_pid, sum_feb_jun, avg_m, total_18, total_eq)
    return svalue


def get_info_dataset_monthly_nrt(ftp_here, product, dataset):
    path = os.path.join('/Core', product, dataset)
    tam_all_months = []
    month_all_months = []
    year_all_months = []

    for y in range(2021, 2023):
        path_here = os.path.join(path, str(y))
        print(path_here)

        tam_month = 0
        tam_month_mb = 0
        for s in ftp_here.nlst(path_here):
            sn = s.split('/')[-1]
            m = int(sn[4:6])
            t = ftp_here.size(s)
            tkb = t / 1024
            tmb = tkb / 1024
            tgb = tmb / 1024
            tam_month = tam_month + tgb

            tam_month_mb = tam_month_mb + tmb
            print('------------------------------------------------------------------>', y, m, tam_month_mb, tam_month)
            tam_all_months.append(tam_month)
            month_all_months.append(m)
            year_all_months.append(y)

    total_pid = 0
    n_pid = 0
    sum_months = [0] * 12
    n_months = [0] * 12
    avg_months = [0] * 12

    for i in range(len(tam_all_months)):
        yhere = year_all_months[i]
        mhere = month_all_months[i]
        there = tam_all_months[i]
        if yhere == 2021 and mhere >= 3:
            total_pid = total_pid + there
            n_pid = n_pid + 1
        sum_months[mhere - 1] = sum_months[mhere - 1] + there
        n_months[mhere - 1] = n_months[mhere - 1] + 1

    sum_feb_jun = 0

    for m in range(12):
        if n_months[m] > 0:
            avg_months[m] = sum_months[m] / n_months[m]
        else:
            avg_months[m] = 0
        mhere = m + 1
        if 1 <= mhere <= 6:
            sum_feb_jun = sum_feb_jun + avg_months[m]
    avg_m = total_pid / n_pid
    print('NPid es: ', n_pid, 'total pid', total_pid, 'avg_m', avg_m, 'feb_jun ', sum_feb_jun)
    # total_18 = avg_m * 18
    total_18 = avg_m * 16
    # total_18 = max_month * 16
    total_eq = total_pid + sum_feb_jun

    svalue = f'{avg_m},{total_pid},{sum_feb_jun},{total_18},{total_eq}'
    for tm in avg_months:
        svalue = f'{svalue},{tm}'

    print(total_pid, sum_feb_jun, avg_m, total_18, total_eq)
    return svalue


def get_info_complete_dataset_daily(ftp_here, product, dataset, year_ini, month_ini, year_fin, month_fin):
    path = os.path.join('/Core', product, dataset)
    tam_total = 0
    n_month = 0
    for y in range(year_ini, year_fin + 1):
        for m in range(1, 13):
            if y == year_ini and m < month_ini:
                continue
            if y == year_fin and m > month_fin:
                continue

            path_here = os.path.join(path, str(y), str(m).zfill(2))
            print(path_here)

            tam_month = 0
            tam_month_mb = 0
            for s in ftp_here.nlst(path_here):
                t = ftp_here.size(s)
                tkb = t / 1024
                tmb = tkb / 1024
                tgb = tmb / 1024
                tam_month = tam_month + tgb
                tam_month_mb = tam_month_mb + tmb
            print('------------------------------------------------------------------>', y, m, tam_month_mb, tam_month)
            tam_total = tam_total + tam_month
            n_month = n_month + 1
    avg_month = tam_total / n_month
    s = f'{avg_month},{tam_total}'
    return s


def get_info_complete_dataset_monthly(ftp_here, product, dataset, year_ini, year_fin):
    path = os.path.join('/Core', product, dataset)
    tam_total = 0
    n_month = 0
    for y in range(year_ini, year_fin + 1):

        path_here = os.path.join(path, str(y))
        print(path_here)

        for s in ftp_here.nlst(path_here):
            t = ftp_here.size(s)
            tkb = t / 1024
            tmb = tkb / 1024
            tgb = tmb / 1024
            tam_total = tam_total + tgb
            n_month = n_month + 1
            print('------------------------------------------------------------------>', y, tmb, tgb)
    avg_month = tam_total / n_month
    s = f'{avg_month},{tam_total}'
    return s


def check_var():
    # file_name = 'CMEMS2_O2022001-plankton-bal-fr.nc'
    # dataset_name = 'cmems_obs-oc_bal_bgc-plankton_nrt_l3-olci-300m_P1D'
    # product_name = 'OCEANCOLOUR_BAL_BGC_L3_NRT_009_131'
    # check_var_impl(file_name, product_name, dataset_name)
    products = [
        'OCEANCOLOUR_BAL_BGC_L3_NRT_009_131',
        'OCEANCOLOUR_BAL_BGC_L3_NRT_009_131',
        'OCEANCOLOUR_BAL_BGC_L3_NRT_009_131',
        'OCEANCOLOUR_BAL_BGC_L3_NRT_009_131',
        'OCEANCOLOUR_BAL_BGC_L4_NRT_009_132',
        'OCEANCOLOUR_BAL_BGC_L3_MY_009_133',
        'OCEANCOLOUR_BAL_BGC_L3_MY_009_133',
        'OCEANCOLOUR_BAL_BGC_L3_MY_009_133',
        'OCEANCOLOUR_MED_BGC_L3_NRT_009_141',
        'OCEANCOLOUR_MED_BGC_L3_NRT_009_141',
        'OCEANCOLOUR_MED_BGC_L3_NRT_009_141',
        'OCEANCOLOUR_MED_BGC_L3_NRT_009_141',
        'OCEANCOLOUR_MED_BGC_L3_NRT_009_141',
        'OCEANCOLOUR_MED_BGC_L3_NRT_009_141',
        'OCEANCOLOUR_MED_BGC_L3_NRT_009_141',
        'OCEANCOLOUR_MED_BGC_L4_NRT_009_142',
        'OCEANCOLOUR_MED_BGC_L4_NRT_009_142',
        'OCEANCOLOUR_MED_BGC_L4_NRT_009_142',
        'OCEANCOLOUR_MED_BGC_L4_NRT_009_142',
        'OCEANCOLOUR_MED_BGC_L4_NRT_009_142',
        'OCEANCOLOUR_MED_BGC_L3_MY_009_143',
        'OCEANCOLOUR_MED_BGC_L3_MY_009_143',
        'OCEANCOLOUR_MED_BGC_L3_MY_009_143',
        'OCEANCOLOUR_MED_BGC_L3_MY_009_143',
        'OCEANCOLOUR_MED_BGC_L4_MY_009_144',
        'OCEANCOLOUR_MED_BGC_L4_MY_009_144',
        'OCEANCOLOUR_MED_BGC_L4_MY_009_144',
        'OCEANCOLOUR_BLK_BGC_L3_NRT_009_151',
        'OCEANCOLOUR_BLK_BGC_L3_NRT_009_151',
        'OCEANCOLOUR_BLK_BGC_L3_NRT_009_151',
        'OCEANCOLOUR_BLK_BGC_L3_NRT_009_151',
        'OCEANCOLOUR_BLK_BGC_L3_NRT_009_151',
        'OCEANCOLOUR_BLK_BGC_L3_NRT_009_151',
        'OCEANCOLOUR_BLK_BGC_L3_NRT_009_151',
        'OCEANCOLOUR_BLK_BGC_L4_NRT_009_152',
        'OCEANCOLOUR_BLK_BGC_L4_NRT_009_152',
        'OCEANCOLOUR_BLK_BGC_L4_NRT_009_152',
        'OCEANCOLOUR_BLK_BGC_L4_NRT_009_152',
        'OCEANCOLOUR_BLK_BGC_L4_NRT_009_152',
        'OCEANCOLOUR_BLK_BGC_L3_MY_009_153',
        'OCEANCOLOUR_BLK_BGC_L3_MY_009_153',
        'OCEANCOLOUR_BLK_BGC_L3_MY_009_153',
        'OCEANCOLOUR_BLK_BGC_L3_MY_009_153',
        'OCEANCOLOUR_BLK_BGC_L4_MY_009_154',
        'OCEANCOLOUR_BLK_BGC_L4_MY_009_154',
        'OCEANCOLOUR_BLK_BGC_L4_MY_009_154'
    ]

    datasets = [
        'cmems_obs-oc_bal_bgc-plankton_nrt_l3-olci-300m_P1D',
        'cmems_obs-oc_bal_bgc-transp_nrt_l3-olci-300m_P1D',
        'cmems_obs-oc_bal_bgc-reflectance_nrt_l3-olci-300m_P1D',
        'cmems_obs-oc_bal_bgc-optics_nrt_l3-olci-300m_P1D',
        'cmems_obs-oc_bal_bgc-plankton_nrt_l4-olci-300m_P1M',
        'cmems_obs-oc_bal_bgc-plankton_my_l3-multi-1km_P1D',
        'cmems_obs-oc_bal_bgc-reflectance_my_l3-multi-1km_P1D',
        'cmems_obs-oc_bal_bgc-transp_my_l3-multi-1km_P1D',
        'cmems_obs-oc_med_bgc-plankton_nrt_l3-multi-1km_P1D',
        'cmems_obs-oc_med_bgc-plankton_nrt_l3-olci-300m_P1D',
        'cmems_obs-oc_med_bgc-reflectance_nrt_l3-multi-1km_P1D',
        'cmems_obs-oc_med_bgc-reflectance_nrt_l3-olci-300m_P1D',
        'cmems_obs-oc_med_bgc-transp_nrt_l3-multi-1km_P1D',
        'cmems_obs-oc_med_bgc-transp_nrt_l3-olci-300m_P1D',
        'cmems_obs-oc_med_bgc-optics_nrt_l3-multi-1km_P1D',
        'cmems_obs-oc_med_bgc-plankton_nrt_l4-multi-1km_P1M',
        'cmems_obs-oc_med_bgc-plankton_nrt_l4-gapfree-multi-1km_P1D',
        'cmems_obs-oc_med_bgc-plankton_nrt_l4-olci-300m_P1M',
        'cmems_obs-oc_med_bgc-transp_nrt_l4-multi-1km_P1M',
        'cmems_obs-oc_med_bgc-transp_nrt_l4-olci-300m_P1M',
        'cmems_obs-oc_med_bgc-plankton_my_l3-multi-1km_P1D',
        'cmems_obs-oc_med_bgc-reflectance_my_l3-multi-1km_P1D',
        'cmems_obs-oc_med_bgc-transp_my_l3-multi-1km_P1D',
        'cmems_obs-oc_med_bgc-optics_my_l3-multi-1km_P1D',
        'cmems_obs-oc_med_bgc-plankton_my_l4-multi-1km_P1M',
        'cmems_obs-oc_med_bgc-plankton_my_l4-gapfree-multi-1km_P1D',
        'cmems_obs-oc_med_bgc-plankton_my_l4-multi-climatology-1km_P1D',
        'cmems_obs-oc_blk_bgc-plankton_nrt_l3-multi-1km_P1D',
        'cmems_obs-oc_blk_bgc-plankton_nrt_l3-olci-300m_P1D',
        'cmems_obs-oc_blk_bgc-reflectance_nrt_l3-multi-1km_P1D',
        'cmems_obs-oc_blk_bgc-reflectance_nrt_l3-olci-300m_P1D',
        'cmems_obs-oc_blk_bgc-transp_nrt_l3-multi-1km_P1D',
        'cmems_obs-oc_blk_bgc-transp_nrt_l3-olci-300m_P1D',
        'cmems_obs-oc_blk_bgc-optics_nrt_l3-multi-1km_P1D',
        'cmems_obs-oc_blk_bgc-plankton_nrt_l4-multi-1km_P1M',
        'cmems_obs-oc_blk_bgc-plankton_nrt_l4-gapfree-multi-1km_P1D',
        'cmems_obs-oc_blk_bgc-plankton_nrt_l4-olci-300m_P1M',
        'cmems_obs-oc_blk_bgc-transp_nrt_l4-multi-1km_P1M',
        'cmems_obs-oc_blk_bgc-transp_nrt_l4-olci-300m_P1M',
        'cmems_obs-oc_blk_bgc-plankton_my_l3-multi-1km_P1D',
        'cmems_obs-oc_blk_bgc-reflectance_my_l3-multi-1km_P1D',
        'cmems_obs-oc_blk_bgc-transp_my_l3-multi-1km_P1D',
        'cmems_obs-oc_blk_bgc-optics_my_l3-multi-1km_P1D',
        'cmems_obs-oc_blk_bgc-plankton_my_l4-multi-1km_P1M',
        'cmems_obs-oc_blk_bgc-plankton_my_l4-gapfree-multi-1km_P1D',
        'cmems_obs-oc_blk_bgc-plankton_my_l4-multi-climatology-1km_P1D'
    ]

    name_files = [
        'CMEMS2_O2022001-plankton-bal-fr.nc',
        'CMEMS2_O2022001-transp-bal-fr.nc',
        'CMEMS2_O2022001-rrs-bal-fr.nc',
        'CMEMS2_O2022001-optics-bal-fr.nc',
        'CMEMS2_O2021335365-chl_monthly-bal-fr.nc',
        'CMEMS2_C2020001-plankton-bal-hr.nc',
        'CMEMS2_C2020001-rrs-bal-hr.nc',
        'CMEMS2_C2020001-transp-bal-hr.nc',
        'CMEMS2_X2022001-plankton-med-hr.nc',
        'CMEMS2_O2022001-plankton-med-fr.nc',
        'CMEMS2_X2022001-rrs-med-hr.nc',
        'CMEMS2_O2022001-rrs-med-fr.nc',
        'CMEMS2_X2022001-transp-med-hr.nc',
        'CMEMS2_O2022001-transp-med-fr.nc',
        'CMEMS2_X2022001-optics-med-hr.nc',
        'CMEMS2_X2021335365-chl_monthly-med-hr.nc',
        'CMEMS2_X2022001-interp-med-hr.nc',
        'CMEMS2_O2021335365-chl_monthly-med-fr.nc',
        'CMEMS2_X2021335365-kd490_monthly-med-hr.nc',
        'CMEMS2_O2021335365-kd490_monthly-med-fr.nc',
        'CMEMS2_X2020001-plankton-med-hr.nc',
        'CMEMS2_X2020001-rrs-med-hr.nc',
        'CMEMS2_X2020001-transp-med-hr.nc',
        'CMEMS2_X2020001-optics-med-hr.nc',
        'CMEMS2_X2020336366-chl_monthly-med-hr.nc',
        'CMEMS2_X2020001-interp-med-hr.nc',
        'CMEMS2_19980101_d_20100101-OC_CNR-L4-CHL-MedOC4AD4_S_1KM-MED-CLIMATOLOGY-v02.nc',
        'CMEMS2_X2022001-plankton-bs-hr.nc',
        'CMEMS2_O2022001-plankton-bs-fr.nc',
        'CMEMS2_X2022001-rrs-bs-hr.nc',
        'CMEMS2_O2022001-rrs-bs-fr.nc',
        'CMEMS2_X2022001-transp-bs-hr.nc',
        'CMEMS2_O2022001-transp-bs-fr.nc',
        'CMEMS2_X2022001-optics-bs-hr.nc',
        'CMEMS2_X2021335365-chl_monthly-bs-hr.nc',
        'CMEMS2_X2022001-interp-bs-hr.nc',
        'CMEMS2_O2021335365-chl_monthly-bs-fr.nc',
        'CMEMS2_X2021335365-kd490_monthly-bs-hr.nc',
        'CMEMS2_O2021335365-kd490_monthly-bs-fr.nc',
        'CMEMS2_X2020001-plankton-bs-hr.nc',
        'CMEMS2_X2020001-rrs-bs-hr.nc',
        'CMEMS2_X2020001-transp-bs-hr.nc',
        'CMEMS2_X2020001-optics-bs-hr.nc',
        'CMEMS2_X2020336366-chl_monthly-bs-hr.nc',
        'CMEMS2_X2020001-interp-bs-hr.nc',
        'CMEMS2_19980101_d_20100101-OC_CNR-L4-CHL-BSAlg_S_1KM-BS-CLIMATOLOGY-v02.nc'
    ]
    for i in range(len(products)):
        check_var_impl(name_files[i], products[i], datasets[i])

    return True


def check_var_July2022():
    file_pdf = '/mnt/c/DATA_LUIS/OCTAC_WORK/EiSNovember2022/LuisWork.csv'
    import pandas as pd
    df = pd.read_csv(file_pdf, sep=';')
    for index, row in df.iterrows():
        product = row['Product']
        dataset = row['Dataset']
        testfile = row['TestFile']
        file = os.path.join('/mnt/c/DATA_LUIS/OCTAC_WORK/EiSNovember2022/TEST_FILES', dataset, testfile)
        if os.path.exists(file):
            check_var_impl(file,product,dataset)


def check_var_Nov2023():
    file_pdf = '/mnt/c/DATA_LUIS/OCTAC_WORK/EiSNovember2023/TestFiles.csv'
    import pandas as pd
    df = pd.read_csv(file_pdf, sep=';')
    for index, row in df.iterrows():
        product = row['Product']
        dataset = row['Dataset']
        testfile = row['TestFile']
        file = os.path.join('/mnt/c/DATA_LUIS/OCTAC_WORK/EiSNovember2023/TEST_FILES', product, testfile)
        if os.path.exists(file):
            check_var_impl(file, product, dataset)

def check_var_impl(file, product_name, dataset_name):
    # path_file = '/mnt/d/LUIS/OCTAC_WORK/EiSJuly2022/'
    #
    # file = os.path.join(path_file, file_name)

    print(file, '------------------------------------------')

    dataset = Dataset(file)
    ##Checking titel
    check_title = dataset.title == dataset_name
    if not check_title:
        print('ERROR in  file: ', file, 'Dataset in file: ', dataset.title, 'Dataset in PIT:', dataset_name)
    check_product_id = product_name == dataset.cmems_product_id
    if not check_product_id:
        print('ERROR in file: ', file, 'Product in file: ', dataset.cmems_product_id, 'Dataset in PIT:', product_name)

    ##Checking var dimensions
    check_var_dim = True
    lista_dim = ['lat', 'lon', 'time']
    for var in dataset.variables:
        check_var = True
        variable = dataset.variables[var]
        if 'valid_min' in variable.ncattrs() and 'valid_max' in variable.ncattrs():
            print('Variable: ', var, 'Valid min: ', variable.valid_min, ' Valid max: ', variable.valid_max)
        for dim in variable.dimensions:
            if not dim in lista_dim:
                check_var = False
        if check_var == False:
            print(var, '-->', check_var)
            check_var_dim = False

    print('*** PU_IT_TF_DatasetTitle: ', check_title)
    print('*** PU_IT_ProductID', check_product_id)
    print('*** PU_IT_TF_VarDimension: ', check_var_dim)
    print('*** PU_IT_TF_VarLimits Check limits above')

    return True


def checking_sizes():
    file_csv = '/mnt/d/LUIS/OCTAC_WORK/EiSJuly2022/References.csv'
    lines_fin = []
    with open(file_csv) as f:
        first_line = f.readline()
        print(first_line)
        line = first_line
        while line:
            line = f.readline()
            if len(line) == 0:
                continue
            col = line.split(',')
            product = col[1]
            dataset = col[2]
            mode = col[4]
            area = col[5]
            freq = col[10]
            sensor = col[11]
            start_date = datetime.strptime(col[12].strip(), '%d/%m/%Y')
            end_date = datetime.strptime(col[13].strip(), '%d/%m/%Y')
            year_ini = start_date.year
            year_fin = end_date.year
            month_ini = start_date.month
            month_fin = end_date.month

            if (sensor == 'multi' or sensor == 'olci') and mode == 'MY' and freq == 'd':
                svalue = retrieve_info_dataset_daily(mode, product, dataset, sensor, year_ini, month_ini, year_fin,
                                                     month_fin)
                line_fin = f'{line.strip()},{svalue.strip()}'
                lines_fin.append(line_fin)

            # if (sensor == 'multi' or sensor=='olci') and mode=='MY' and freq == 'm':
            #     svalue = retrieve_info_dataset_monthly(mode, product, dataset, sensor, year_ini, year_fin,)
            #     line_fin = f'{line.strip()},{svalue.strip()}'
            #     lines_fin.append(line_fin)

    print('Writting...')
    file_out = f'/mnt/d/luis/MY_MONTHLY.txt'
    with open(file_out, 'w') as f:
        for line_fin in lines_fin:
            f.write(line_fin)
            f.write('\n')
    print('Done')

    return True


def retrieve_info_dataset_daily(mode, product, dataset, sensor, year_ini, month_ini, year_fin, month_fin):
    usuario = 'lgonzalezvilas'
    password = 'MegaRoma17!'
    if mode == 'NRT':
        ftp_here = FTP('nrt-dev.cmems-du.eu', user=usuario, passwd=password)
    elif mode == 'MY':
        ftp_here = FTP('my.cmems-du.eu', user=usuario, passwd=password)
    ftp_here.login(user=usuario, passwd=password)

    path = os.path.join('/Core', product, dataset)
    tam_all_months = []
    month_all_months = []
    year_all_months = []
    tam_total = 0
    days_by_month = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    for y in range(year_ini, year_fin + 1):
        for m in range(1, 13):
            path_here = os.path.join(path, str(y), str(m).zfill(2))
            if y == year_ini and m < month_ini:
                continue
            if y == year_fin and m > month_fin:
                continue
            n_month = 0
            tam_month = 0
            tam_month_mb = 0
            for s in ftp_here.nlst(path_here):
                t = ftp_here.size(s)
                tkb = t / 1024
                tmb = tkb / 1024
                tgb = tmb / 1024
                tam_month = tam_month + tgb
                tam_month_mb = tam_month_mb + tmb
                n_month = n_month + 1
            print('------------------------------------------------------------------>', y, m, tam_month_mb, tam_month)
            if n_month >= days_by_month[m]:
                tam_all_months.append(tam_month)
                month_all_months.append(m)
                year_all_months.append(y)
            tam_total = tam_total + tam_month

    # Average by month
    sum_months = [0] * 12
    n_months = [0] * 12
    avg_months = [0] * 12
    sum_months_total = 0
    n_months_total = 0
    for i in range(len(tam_all_months)):
        mhere = month_all_months[i]
        there = tam_all_months[i]
        sum_months[mhere - 1] = sum_months[mhere - 1] + there
        n_months[mhere - 1] = n_months[mhere - 1] + 1
        sum_months_total = sum_months_total + there
        n_months_total = n_months_total + 1

    for m in range(12):
        if n_months[m] > 0:
            avg_months[m] = sum_months[m] / n_months[m]
        else:
            avg_months[m] = -1

    avg_m = sum_months_total / n_months_total

    print(avg_m, tam_total)

    svalue = f'{avg_m},{tam_total}'
    for tm in avg_months:
        svalue = f'{svalue},{tm}'
    return svalue


def retrieve_info_dataset_monthly(mode, product, dataset, sensor, year_ini, year_fin):
    usuario = 'lgonzalezvilas'
    password = 'MegaRoma17!'
    if mode == 'NRT':
        ftp_here = FTP('nrt-dev.cmems-du.eu', user=usuario, passwd=password)
    elif mode == 'MY':
        ftp_here = FTP('my.cmems-du.eu', user=usuario, passwd=password)
    ftp_here.login(user=usuario, passwd=password)

    path = os.path.join('/Core', product, dataset)
    tam_all_months = []
    month_all_months = []
    year_all_months = []
    tam_total = 0
    for y in range(year_ini, year_fin + 1):
        path_here = os.path.join(path, str(y))
        for s in ftp_here.nlst(path_here):
            sn = s.split('/')[-1]
            m = int(sn[4:6])
            t = ftp_here.size(s)
            tkb = t / 1024
            tmb = tkb / 1024
            tgb = tmb / 1024
            tam_month = tgb
            tam_month_mb = tmb
            print('------------------------------------------------------------------>', y, m, tam_month_mb, tam_month)
            tam_all_months.append(tam_month)
            month_all_months.append(m)
            year_all_months.append(y)
            tam_total = tam_total + tam_month

    # Average by month
    sum_months = [0] * 12
    n_months = [0] * 12
    avg_months = [0] * 12
    sum_months_total = 0
    n_months_total = 0
    for i in range(len(tam_all_months)):
        mhere = month_all_months[i]
        there = tam_all_months[i]
        sum_months[mhere - 1] = sum_months[mhere - 1] + there
        n_months[mhere - 1] = n_months[mhere - 1] + 1
        sum_months_total = sum_months_total + there
        n_months_total = n_months_total + 1

    for m in range(12):
        if n_months[m] > 0:
            avg_months[m] = sum_months[m] / n_months[m]
        else:
            avg_months[m] = -1

    avg_m = sum_months_total / n_months_total

    print(avg_m, tam_total)

    svalue = f'{avg_m},{tam_total}'
    for tm in avg_months:
        svalue = f'{svalue},{tm}'
    return svalue

def run_check_cnr_file(options,section):
    print('[INFO] Check CNR files')
    #section = 'CHECK_CNR_FILES'
    if not options.has_section(section):
        print(f'[ERROR] Section {section} is not available in the configuration file')
        return
    required_options = ['input_path','input_path_organization','name_file_format','name_file_date_format','start_date','end_date']
    make_check = True
    options_dict = {}
    for op in required_options:
        if options.has_option(section,op):
            options_dict[op] = options[section][op].strip()
        else:
            print(f'[ERROR] Option {op} is required for mode {section}.')
            make_check = False
    if not make_check:
        return

    if not os.path.isdir(options_dict['input_path']):
        print(f'[ERROR] Input path {options_dict["input_path"]} does not exist or not is a valid directory')
        return


    try:
        start_date = datetime.strptime(options_dict['start_date'],'%Y-%m-%d')
    except:
        print(f'[ERROR] Start date {options_dict["start_date"]} should be in format YYYY-mm-dd')
        return

    try:
        end_date = datetime.strptime(options_dict['end_date'],'%Y-%m-%d')
    except:
        print(f'[ERROR] End date {options_dict["end_date"]} should be in format YYYY-mm-dd')
        return




    name_file_format = options_dict['name_file_format']
    name_file_date_format = options_dict['name_file_date_format']
    if name_file_date_format == '%Y%j%j':
        start_date = start_date.replace(day=15)
        end_date  = end_date.replace(day=15)
    missing_files = []
    error_files = []
    notime_files = []
    invalid_time_files = []
    valid_files = []
    ntot = 0
    date_run = start_date

    while date_run <= end_date:
        ntot = ntot+1
        input_path = get_folder_date(options_dict['input_path'], options_dict['input_path_organization'], date_run)
        if name_file_date_format == '%Y%j%j':  ##format used for months
            nfiles_month = monthrange(date_run.year, date_run.month)[1]
            sdate = date_run.replace(day=1).strftime('%Y%j')
            edate = date_run.replace(day=nfiles_month).strftime('%j')
            date_file_str = f'{sdate}{edate}'
        else:
            date_file_str = date_run.strftime(name_file_date_format)
        name_file = name_file_format.replace('$DATE$', date_file_str)
        input_file = os.path.join(input_path, name_file)
        if not os.path.exists(input_file):
            #print(f'[WARNING] Input file {input_file} for date {date_run} is not available. Skiping...')
            missing_files.append((date_run,input_file))
        else:
            try:
                dataset = Dataset(input_file)
                if 'time' in dataset.variables:
                    time_file = datetime(1981,1,1,0,0,0,0)+timedelta(seconds=int(dataset.variables['time'][0]))
                    if name_file_date_format == '%Y%j%j':
                        time_file = time_file.replace(day=15)
                    if time_file.strftime('%Y%m%d')==date_run.strftime('%Y%m%d'):
                        valid_files.append((date_run,input_file))
                    else:
                        invalid_time_files.append((date_run,input_file))
                else:
                    notime_files.append((date_run,input_file))
                dataset.close()
            except:

                error_files.append((date_run,input_file))

        if name_file_date_format == '%Y%j%j':
            month_new = 1 if date_run.month == 12 else date_run.month + 1
            year_new = date_run.year + 1 if date_run.month == 12 else date_run.year
            date_run = date_run.replace(month=month_new, year=year_new)
        else:
            date_run = date_run + timedelta(hours=24)

    print(f'Number of expected files: {ntot}')
    print(f'Number of valid files: {len(valid_files)}')
    if len(valid_files)<ntot:
        if len(missing_files)>0:
            print(f'-> Missing files: {len(missing_files)}')
        if len(error_files)>0:
            print(f'-> Corrupt files: {len(error_files)}')
        if len(notime_files)>0:
            print(f'-> Files without time variable: {len(notime_files)}')
        if len(invalid_time_files)>0:
            print(f'-> Files without invalid date: {len(invalid_time_files)}')

        print('---------------------------------------------------------------')
        if len(missing_files) > 0:
            print('Missing files:')
            for tfile in missing_files:
                print(f'{tfile[0].strftime("%Y-%m-%d")} -> {tfile[1]}')

        print('---------------------------------------------------------------')
        if len(error_files) > 0:
            print('Corrupt files:')
            for tfile in error_files:
                print(f'{tfile[0].strftime("%Y-%m-%d")} -> {tfile[1]}')

        print('---------------------------------------------------------------')
        if len(notime_files) > 0:
            print('No time files:')
            for tfile in notime_files:
                print(f'{tfile[0].strftime("%Y-%m-%d")} -> {tfile[1]}')

        print('---------------------------------------------------------------')
        if len(invalid_time_files) > 0:
            print('Invalid time files:')
            for tfile in invalid_time_files:
                print(f'{tfile[0].strftime("%Y-%m-%d")} -> {tfile[1]}')



def main():
    if not args.config_file:
        print(f'[ERROR] Config file or input product should be defined for {args.mode} option. Exiting...')
        return
    if not os.path.exists(args.config_file):
        print(f'[ERROR] Config file {args.config_file} does not exist. Exiting...')
        return
    try:
        import configparser
        options = configparser.ConfigParser()
        options.read(args.config_file)
    except:
        print(f'[ERROR] Config file {args.config_file} could not be read. Exiting...')
        return



    if args.mode=='CHECK_CNR_FILES':
        section = args.section if args.section else args.mode
        run_check_cnr_file(options,section)

def get_folder_date(path_base, org, date_here):
    if org is None:
        return path_base
    if org == 'YYYYmmdd':
        date_here_str = date_here.strftime('%Y%m%d')
        path_date = os.path.join(path_base, date_here_str)
    if org == 'YYYY/mm/dd':
        year_str = date_here.strftime('%Y')
        month_str = date_here.strftime('%m')
        day_str = date_here.strftime('%d')
        path_date = os.path.join(path_base, year_str,month_str,day_str)
    if org == 'YYYY/mm':
        year_str = date_here.strftime('%Y')
        month_str = date_here.strftime('%m')
        path_date = os.path.join(path_base, year_str, month_str)
    if org == 'YYYY/jjj':
        year_str = date_here.strftime('%Y')
        jjj_str = date_here.strftime('%j')
        path_date = os.path.join(path_base,year_str, jjj_str)
    if org == 'YYYY':
        year_str = date_here.strftime('%Y')
        path_date = os.path.join(path_base, year_str)

    return path_date

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()