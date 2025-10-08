import argparse
import shutil
from datetime import datetime as dt
from datetime import timedelta

import numpy as np
import pandas as pd

from source_info import SourceInfo
import calendar
import os

##ATTENTION: SOME OPTIONS WERE MOVE TO OTHER PROJECTS:
# to map_toools in aceasy: CREATE_MASK, APPLY_MASK, CREATE_MASK_CDF

parser = argparse.ArgumentParser(description='Check upload')
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-m", "--mode", help="Mode.", type=str, required=True,
                    choices=['COPYAQUA', 'CHECKFTPCONTENTS', 'CHECKGRANULES', 'CHECKSOURCES', 'ZIPGRANULES',
                             'LOG_HYPSTAR',
                             'TEST', 'UPDATE_TIME_CMEMS_DAILY', 'UPDATE_TIME_CMEMS_MONTHLY', 'REMOVE_NR_SOURCES',
                             'CREATE_MASK',
                             'APPLY_MASK', 'CREATE_MASK_CDF', 'UPDATE_TIME_EXTRACTS','COMPARE_NC'])
parser.add_argument("-p", "--path", help="Input path")
parser.add_argument("-o", "--output_path", help="Output path")
parser.add_argument("-mvar", "--mask_variable", help="Mask variable")
parser.add_argument("-fref", "--file_ref", help="File ref with lat/lon variables for masking")
parser.add_argument("-fmask", "--file_mask", help="File mask for APPLY_MASK")
parser.add_argument("-ifile", "--input_file_name", help="Input file name for APPLY_MASK, DATE is replaced by YYYYjjj")
parser.add_argument("-sd", "--start_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-ed", "--end_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-pr", "--preffix", help="Preffix")
parser.add_argument("-sf", "--suffix", help="Suffix")
parser.add_argument("-tsources", "--type_sources", help="Sources type to REMOVE_NR_SOURCES option",
                    choices=["ZIP", "SEN3", "NC"], default="ZIP")
args = parser.parse_args()


def do_check():
    # import json
    # fproduct = '/mnt/c/DATA_LUIS/CODE_COPY/OCEANCOLOUR_MED_BGC_L4_MY_009_144.json'
    # f = open(fproduct, "r")
    #
    # pinfo = json.load(f)
    #
    #
    #
    # f.close()
    # from netCDF4 import Dataset
    # import numpy as np
    # from datetime import datetime as dt
    # path_hypernets = '/mnt/c/DATA_LUIS/INSITU_HYPSTAR/VEIT/2023/09/11'
    # file_out = '/mnt/c/DATA_LUIS/INSITU_HYPSTAR/data_hypstar_20230911.csv'
    # f1 = open(file_out, 'w')
    # first_line = 'File;Date;Time;solar_azimuth_angle;solar_zenith_angle;viewing_azimuth_angle;viewing_zenith_angle;quality_flag'
    # wavelenght = None
    # for name in os.listdir(path_hypernets):
    #     print(name)
    #     file_hypernets = os.path.join(path_hypernets, name)
    #     dataset = Dataset(file_hypernets, 'r')
    #     if wavelenght is None:
    #         wavelenght = np.array(dataset.variables['wavelength'])
    #         for val in wavelenght:
    #             val_rrs = f'rrs_{val}'
    #             first_line = f'{first_line};{val_rrs}'
    #         f1.write(first_line)
    #
    #     datetime_here = dt.utcfromtimestamp(float(dataset.variables['acquisition_time'][0]))
    #     date_str = datetime_here.strftime('%Y-%m-%d')
    #     time_str = datetime_here.strftime('%H:%M')
    #     saa = dataset.variables['solar_azimuth_angle'][0]
    #     sza = dataset.variables['solar_zenith_angle'][0]
    #     vaa = dataset.variables['viewing_azimuth_angle'][0]
    #     vza = dataset.variables['viewing_zenith_angle'][0]
    #     qf = dataset.variables['quality_flag'][0]
    #     line = f'{name};{date_str};{time_str};{saa};{sza};{vaa};{vza};{qf}'
    #     rrs = np.array(dataset.variables['reflectance'][:]) / np.pi
    #
    #     for r in rrs:
    #         line = f'{line};{r[0]}'
    #     f1.write('\n')
    #     f1.write(line)
    #
    #     dataset.close()
    # f1.close()
    # from netCDF4 import Dataset
    # import numpy as np
    # file_1 = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_TEST/MULTI/C2021196_kd490-arc-4km_prev.nc'
    # file_2 = '/mnt/c/DATA_LUIS/OCTAC_WORK/ARC_TEST/MULTI/C2021196_kd490-arc-4km.nc'
    # d1 = Dataset(file_1)
    # d2 = Dataset(file_2)
    # for name_var in d1.variables:
    #     var1 = np.array(d1.variables[name_var][:])
    #     var2 = np.array(d2.variables[name_var][:])
    #     #print(name_var,np.nanmin(var1),np.nanmax(var2))
    #     var_res = var1/var2
    #     print(name_var,np.nanmin(var_res),np.nanmax(var_res),np.nanmean(var_res))
    #
    # d1.close()
    # d2.close()

    return True


def do_check_cula():
    import pandas as pd
    dir_modelos = '/mnt/c/PERSONAL/ARTICULO_PSEUDONITZSCHIA/MODELOS_DATOS_FIN_PA_2/Modelos'
    dir_res = '/mnt/c/PERSONAL/ARTICULO_PSEUDONITZSCHIA/MODELOS_DATOS_FIN_PA_2/Modelos_ResModels'
    file_out = '/mnt/c/PERSONAL/ARTICULO_PSEUDONITZSCHIA/Results_GridSearch_BNB_COARSE.csv'
    f1 = open(file_out, 'w')
    f1.write('GridSearch;Gamma;Cost;Sensitivity;Specificity;Precission;TSS;F1-Score;AUC')
    params = [5, 6, 14, 11, 19, 20]
    for index in range(0, 29):
        line = 'Coarse'
        fmodelo = os.path.join(dir_modelos, f'Modelos_Best_Coarse_{index}.model')
        if not os.path.exists(fmodelo):
            continue
        dmodel = pd.read_csv(fmodelo, sep='=')
        gamma = dmodel.loc[1].iat[1]
        cost = dmodel.loc[2].iat[1]
        # dir_results = os.path.join(dir_res,f'Modelos_Best_Coarse_{index}')
        # fresults = os.path.join(dir_results,'Resultados.csv')
        # dres = pd.read_csv(fresults,sep=';')
        line = f'{line};{gamma};{cost}'
        # for iparam in params:
        #     val = dres.loc[iparam].at['DATOS_COMPLETO_TEST.csv']
        #     line = f'{line};{val}'
        print(index, line)
        f1.write('\n')
        f1.write(line)
    for index in range(0, 14):
        line = 'Coarse'
        fmodelo = os.path.join(dir_modelos, f'ModelTrain_Best_Fine1_{index}.model')
        if not os.path.exists(fmodelo):
            continue
        dmodel = pd.read_csv(fmodelo, sep='=')
        gamma = dmodel.loc[1].iat[1]
        cost = dmodel.loc[2].iat[1]
        dir_results = os.path.join(dir_res, f'Modelos_Best_Coarse_{index}')
        fresults = os.path.join(dir_results, 'Resultados.csv')
        dres = pd.read_csv(fresults, sep=';')
        line = f'{line};{gamma};{cost}'
        for iparam in params:
            val = dres.loc[iparam].at['DATOS_COMPLETO_TEST.csv']
            line = f'{line};{val}'
        print(index, line)
        f1.write('\n')
        f1.write(line)
    f1.close()
    return True


def do_check_tal():
    import pandas as pd
    dir_base = '/mnt/c/PERSONAL/ARTICULO_PSEUDONITZSCHIA/MODELOS_DATOS_FIN_PA_4/'
    file_out = '/mnt/c/PERSONAL/ARTICULO_PSEUDONITZSCHIA/Results_GridSearch_BNB.csv'
    f1 = open(file_out, 'w')
    f1.write('GridSearch;Gamma;Cost;Sensitivity;Specificity;Precission;TSS;F1-Score;AUC')
    params = [5, 6, 14, 11, 19, 20]
    for index in range(1, 130):
        line = 'Fine'
        dir_model = os.path.join(dir_base, f'CVLOU_{index}')
        fbest = os.path.join(dir_model, f'CVLOU_{index}_Best.csv')
        dbest = pd.read_csv(fbest, sep=';')
        gamma = dbest.loc[0].at['Gamma']
        cost = dbest.loc[0].at['Cost']
        line = f'{line};{gamma};{cost}'
        fres = os.path.join(dir_model, 'ResultadosCV.csv')
        dres = pd.read_csv(fres, sep=';')
        for iparam in params:
            val = dres.loc[iparam].at['OptTh']
            line = f'{line};{val}'
        print(index, line)
        f1.write('\n')
        f1.write(line)

    f1.close()
    return True


def do_log_hypstar():
    file_log = '/mnt/c/DATA_LUIS/ESA-POP_WORK/2023-11-sequence_12V-input.log'
    file_out = '/mnt/c/DATA_LUIS/ESA-POP_WORK/voltage_output_11.csv'
    fout = open(file_out, 'w')
    fout.write('Date;TimeSequence;Time;Datetime;Voltage;Current;TotalEnergyConsumed')
    f1 = open(file_log, 'r')

    for line in f1:
        line_s = line.split(' ')
        # for idx in range(len(line_s)):
        #     print(idx,line_s[idx])
        date = line_s[0][:10]
        time_sequence = line_s[0].split('-')[3]
        time = line_s[2]
        voltage = line_s[57]
        current = line_s[84]
        total_energy_consumed = line_s[112]

        # print(date,time_sequence)
        # print(line)

        line_out = f'{date};{time_sequence};{time};{date}T{time};{voltage};{current};{total_energy_consumed}'
        fout.write('\n')
        fout.write(line_out)
        print(line_out)
        # print('-------------')
    f1.close()
    fout.close()


def do_empty_copy():
    input_file = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/MonthlyEmptyFiles/O2023001031-kd490_monthly-bal-fr.nc'
    output_file = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/MonthlyEmptyFiles/O2023335365-kd490_monthly-bal-fr.nc'
    from datetime import datetime as dt

    date_jan = dt(1981, 1, 1, 0, 0, 0) + timedelta(seconds=1325376000)
    int_jan = (dt(2023, 1, 1, 0, 0, 0) - dt(1981, 1, 1, 0, 0, 0)).total_seconds()
    int_dec = (dt(2023, 12, 1, 0, 0, 0) - dt(1981, 1, 1, 0, 0, 0)).total_seconds()
    print(int_jan)
    print(int_dec)

    # start_date = '2023-12-01'
    # end_date = '2023-12-31'

    from netCDF4 import Dataset
    input_dataset = Dataset(input_file)
    ncout = Dataset(output_file, 'w', format='NETCDF4')

    # copy attributs
    for at in input_dataset.ncattrs():
        val = input_dataset.getncattr(at)
        ncout.setncattr(at, val)
    ncout.start_date = '2023-12-01'
    ncout.stop_date = '2023-12-31'

    # copy dimensions
    for name, dimension in input_dataset.dimensions.items():
        ncout.createDimension(
            name, (len(dimension) if not dimension.isunlimited() else None))

    for name, variable in input_dataset.variables.items():
        fill_value = None
        if '_FillValue' in list(variable.ncattrs()):
            fill_value = variable._FillValue

        ncout.createVariable(name, variable.datatype, variable.dimensions, fill_value=fill_value, zlib=True,
                             shuffle=True, complevel=6)
        # copy variable attributes all at once via dictionary
        ncout[name].setncatts(input_dataset[name].__dict__)

        if name == 'lat' or name == 'lon':
            ncout[name][:] = input_dataset[name][:]
        if name == 'time':
            ncout[name][:] = 1354233600

    ncout.close()
    input_dataset.close()

    return True


def resolve_CCOC_778():
    print('Resolving 778')
    # 1.CHECKING SENSOR MASK
    # path = '/dst04-data1/OC/OLCI/daily_v202311_bc'
    # path = '/store/COP2-OC-TAC/BAL_Evolutions/BAL_REPROC'
    path = '/store/COP2-OC-TAC/BAL_Evolutions/POLYMERWHPC'
    from datetime import datetime as dt
    from netCDF4 import Dataset
    date_work = dt(2016, 4, 26)
    date_fin = dt(2023, 12, 31)
    file_out = '/store/COP2-OC-TAC/BAL_Evolutions/CCOC-778/list_files_daily_final_2016_2023.csv'
    f1 = open(file_out, 'w')
    f1.write('Date;Status')
    while date_work <= date_fin:
        yyyy = date_work.strftime('%Y')
        jjj = date_work.strftime('%j')
        file_date = os.path.join(path, yyyy, jjj, f'O{yyyy}{jjj}-chl-bal-fr.nc')
        status = -1
        if os.path.exists(file_date):
            status = 0
            dataset = Dataset(file_date, 'r')
            if 'SENSORMASK' in dataset.variables:
                status = 1
            dataset.close()
        date_work_f = date_work.strftime('%Y-%m-%d')
        line = f'{date_work_f};{status}'
        f1.write('\n')
        f1.write(line)

        date_work = date_work + timedelta(hours=24)
    f1.close()

    # 2. Checking S3A and S3B
    # path = '/store/COP2-OC-TAC/BAL_Evolutions/BAL_REPROC'
    # input_file = '/store/COP2-OC-TAC/BAL_Evolutions/CCOC-778/list_files_2016_2022_zero.csv'
    # output_file = '/store/COP2-OC-TAC/BAL_Evolutions/CCOC-778/list_files_2016_2022_zero_check.csv'
    # from datetime import datetime as dt
    # f1 = open(input_file,'r')
    # fout = open(output_file,'w')
    # for line in f1:
    #     if line.startswith('Date'):
    #         line_output = f'{line.strip()};S3A;S3B'
    #         fout.write(line_output)
    #         continue
    #     lines = [x.strip() for x in line.split(';')]
    #     date_here = dt.strptime(lines[0],'%Y-%m-%d')
    #     yyyy = date_here.strftime('%Y')
    #     jjj = date_here.strftime('%j')
    #     path_date = os.path.join(path,yyyy,jjj)
    #     s3a = '0'
    #     s3b = '0'
    #     if os.path.isdir(path_date):
    #         for name in os.listdir(path_date):
    #             if name.startswith('Oa'):
    #                 s3a = '1'
    #             if name.startswith('Ob'):
    #                 s3b = '1'
    #     line_output = f'{line.strip()};{s3a};{s3b}'
    #     fout.write('\n')
    #     fout.write(line_output)
    # f1.close()
    # fout.close()

    # 3. Copy available files s3a (s3b is missing), from 2016 to 2022
    # path_proc = '/store/COP2-OC-TAC/BAL_Evolutions/BAL_REPROC'
    # path_polw = '/store/COP2-OC-TAC/BAL_Evolutions/POLYMER_WATER'
    # path_output = '/store/COP2-OC-TAC/BAL_Evolutions/POLYMERWHPC'
    # input_file = '/store/COP2-OC-TAC/BAL_Evolutions/CCOC-778/list_files_2020_onlymakes3a.csv'
    # f1 = open(input_file,'r')
    #
    # for line in f1:
    #     if line.startswith('Date'):
    #         continue
    #     lines = [x.strip() for x in line.split(';')]
    #     date_here = dt.strptime(lines[0],'%Y-%m-%d')
    #     yyyy = date_here.strftime('%Y')
    #     jjj = date_here.strftime('%j')
    #     path_proc_date = os.path.join(path_proc, yyyy, jjj)
    #     path_polw_date = os.path.join(path_polw,yyyy,jjj)
    #     path_output_year = os.path.join(path_output,yyyy)
    #     if not os.path.exists(path_output_year):
    #         os.mkdir(path_output_year)
    #     path_output_day = os.path.join(path_output_year,jjj)
    #     if not os.path.exists(path_output_day):
    #         os.mkdir(path_output_day)
    #     for name in os.listdir(path_proc_date):
    #         if name.startswith('Ob'):
    #             input_file = os.path.join(path_proc_date, name)
    #             output_file = os.path.join(path_output_day, name)
    #             shutil.copy(input_file,output_file)
    #     for name in os.listdir(path_polw_date):
    #         if name.startswith('S3B'):
    #             input_file = os.path.join(path_polw_date, name)
    #             output_file = os.path.join(path_output_day, name)
    #             shutil.copy(input_file,output_file)
    #
    # f1.close()

    # 2. Adding SENSORMASK ONLY WITH S3A
    # copy_path = '/store/COP2-OC-TAC/BAL_Evolutions/POLYMERWHPC'
    # path = '/store/COP2-OC-TAC/BAL_Evolutions/BAL_REPROC'
    # #copy_path = '/mnt/c/DATA_LUIS/OCTAC_WORK/CCOC-778/kk'
    # #path = '/mnt/c/DATA_LUIS/OCTAC_WORK/CCOC-778'
    #
    # date1 = dt(2020,7,29)
    # date2 = dt(2020,8,28)
    # date3 = dt(2020,8,31)
    # dates = [date1,date2,date3]
    # for date_work in dates:
    # # date_work = dt(2019,4,26)
    # # date_fin = dt(2018,5,15)
    # # while date_work<=date_fin:
    #     yyyy = date_work.strftime('%Y')
    #     jjj = date_work.strftime('%j')
    #     path_date = os.path.join(path,yyyy,jjj)
    #     prename = f'O{yyyy}{jjj}-'
    #     copy_path_yyyy = os.path.join(copy_path,yyyy)
    #     if not os.path.isdir(copy_path_yyyy):
    #         os.mkdir(copy_path_yyyy)
    #     copy_path_jjj = os.path.join(copy_path_yyyy,jjj)
    #     if not os.path.exists(copy_path_jjj):
    #         os.mkdir(copy_path_jjj)
    #     if os.path.isdir(path_date):
    #         for name in os.listdir(path_date):
    #             input_file = os.path.join(path_date, name)
    #             if name.startswith(prename) and name.find('bal')>0:
    #                 output_file = os.path.join(path,name)
    #                 if name.find('coverage') < 0:
    #                     print(input_file, '-->', output_file)
    #                     create_copy_with_sensor_mask(input_file,output_file)
    #                     os.rename(output_file,input_file)
    #             copy_file = os.path.join(copy_path_jjj,name)
    #             shutil.copy(input_file,copy_file)
    #     #date_work = date_work + timedelta(hours=24)

    ## extra step: copy from BAL_REPROC to POLYMERWHPC
    # path_proc = '/store/COP2-OC-TAC/BAL_Evolutions/BAL_REPROC'
    # # path_polw = '/store/COP2-OC-TAC/BAL_Evolutions/POLYMER_WATER'
    # path_output = '/store/COP2-OC-TAC/BAL_Evolutions/POLYMERWHPC'
    # input_file = '/store/COP2-OC-TAC/BAL_Evolutions/CCOC-778/list_files_to_copy.csv'
    # f1 = open(input_file,'r')
    #
    # for line in f1:
    #     date_here = dt.strptime(line.strip(), '%Y-%m-%d')
    #     yyyy = date_here.strftime('%Y')
    #     jjj = date_here.strftime('%j')
    #     path_date = os.path.join(path_proc, yyyy, jjj)
    #     copy_path_yyyy = os.path.join(path_output, yyyy)
    #     if not os.path.isdir(copy_path_yyyy):
    #         os.mkdir(copy_path_yyyy)
    #     copy_path_jjj = os.path.join(copy_path_yyyy,jjj)
    #     if not os.path.exists(copy_path_jjj):
    #         os.mkdir(copy_path_jjj)
    #     if os.path.isdir(path_date):
    #         for name in os.listdir(path_date):
    #             input_file = os.path.join(path_date, name)
    #             copy_file = os.path.join(copy_path_jjj, name)
    #             shutil.copy(input_file,copy_file)
    #
    # f1.close()

    return True


def create_copy_with_sensor_mask(input_file, output_file):
    import numpy as np
    from netCDF4 import Dataset
    input_dataset = Dataset(input_file)
    ncout = Dataset(output_file, 'w', format='NETCDF4')

    # copy global attributes all at once via dictionary
    ncout.setncatts(input_dataset.__dict__)

    # copy dimensions
    for name, dimension in input_dataset.dimensions.items():
        ncout.createDimension(
            name, (len(dimension) if not dimension.isunlimited() else None))

    for name, variable in input_dataset.variables.items():
        fill_value = None
        if '_FillValue' in list(variable.ncattrs()):
            fill_value = variable._FillValue

        if name != 'time' and name != 'lat' and name != 'lon' and name != 'QI':
            data = np.array(input_dataset[name][:])
            data[data != -999] = 1
            data[data == 999] = np.nan
        ncout.createVariable(name, variable.datatype, variable.dimensions, fill_value=fill_value, zlib=True,
                             complevel=6)

        # copy variable attributes all at once via dictionary
        ncout[name].setncatts(input_dataset[name].__dict__)

        # copy data
        ncout[name][:] = input_dataset[name][:]

    var = ncout.createVariable('SENSORMASK', 'i4', ('time', 'lat', 'lon'), zlib=True, complevel=6)
    ncout['SENSORMASK'][:] = data
    var.long_name = 'Sensor Mask'
    var.comment = 'OLCI Sentinel-3A=1; OLCI Sentinel-3B=2. Each SENSORMASK pixel is the sum of all available sensor values. For example, if a pixel is observed by OLCI Sentinel-3A and OLCI Sentinel-3B then SENSORMASK = 3'
    var.type = 'surface'
    var.units = '1'
    var.valid_min = 1
    var.valid_max = 31
    ncout.close()
    input_dataset.close()

    return True


def check_med():
    mode = 'NRT'
    from product_info import ProductInfo
    from datetime import datetime as dt
    pinfo = ProductInfo()
    pinfo.set_dataset_info('OCEANCOLOUR_MED_BGC_L3_NRT_009_141', 'cmems_obs-oc_med_bgc-plankton_nrt_l3-multi-1km_P1D')
    date_here = dt(2024, 5, 7)
    from s3buckect import S3Bucket
    sb = S3Bucket()
    sb.update_params_from_pinfo(pinfo)
    conn = sb.star_client()
    s3bname, key, isuploaded = sb.check_daily_file(mode, pinfo, date_here, False)
    print(s3bname)
    print(key)
    print(isuploaded)
    return True


def check_download():
    from product_info import ProductInfo
    # from s3buckect import S3Bucket
    # pinfo = ProductInfo()
    # pinfo.set_dataset_info('OCEANCOLOUR_MED_BGC_L3_NRT_009_141', 'cmems_obs-oc_med_bgc-plankton_nrt_l3-multi-1km_P1D')
    # date_here = dt(2024, 5, 14)
    # sb = S3Bucket()
    # sb.update_params_from_pinfo(pinfo)
    # conn = sb.star_client()
    # s3bname, key, isuploaded = sb.check_daily_file('NRT', pinfo, date_here, False)
    # path_out = '/mnt/c/DATA_LUIS/OCTACWORK'
    # sb.download_daily_file('NRT', pinfo, date_here, path_out, True, True)
    from nasa_download import  NASA_DOWNLOAD
    ndownload  = NASA_DOWNLOAD()
    import obdaac_download as od
    url = ndownload.get_url_download('PACE_OCI_20241108T122004_L2_OC_AOP_V2_0_NRT.nc')
    od.do_download(url, '/mnt/c/DATA', ndownload.apikey)
    return True


def resolve_CCOC_878(year):
    file_slurm = f'/mnt/c/DATA_LUIS/OCTAC_WORK/CCOC-878/resolve_ccoc_878_{year}.slurm'
    fw = open(file_slurm, 'w')
    fw.write('#!/bin/bash')
    start_lines = [
        '#SBATCH --nodes=1',
        '#SBATCH --ntasks=1',
        '#SBATCH -p octac',
        '#SBATCH --mail-type=BEGIN,END,FAIL',
        '#SBATCH --mail-user=luis.gonzalezvilas@artov.ismar.cnr.it,lorenzo.amodio@artov.ismar.cnr.it',
        '',
        '',
        'source /home/gosuser/load_miniconda3.source',
        'conda activate op_proc_202211v2',
        'cd /store/COP2-OC-TAC/OCTACMANAGER/CCOC-878',
        '',
        ''
    ]
    for line in start_lines:
        fw.write('\n')
        fw.write(line)

    start_date = dt(year, 1, 1)
    end_date = dt(year, 12, 31)
    if year == 1997:
        start_date = dt(year, 9, 16)
    if year == 2024:
        start_date = dt(year, 9, 5)
        end_date = dt(year, 9, 10)

    base_folder = '/store3/OC/MULTI/daily_v202311_x'
    work_date = start_date
    while work_date <= end_date:
        yyyy = work_date.strftime('%Y')
        jjj = work_date.strftime('%j')
        file_med = os.path.join(base_folder, yyyy, jjj, f'X{yyyy}{jjj}-pft-med-hr.nc')
        file_bs = os.path.join(base_folder, yyyy, jjj, f'X{yyyy}{jjj}-pft-bs-hr.nc')
        fw = resolve_CCOC_878_impl(file_med, file_bs, fw)
        work_date = work_date + timedelta(hours=24)

    fw.close()

    return True


def resolve_CCOC_878_impl(file_med, file_bs, fw):
    med = {
        'DIATO': {
            'valid_min': 0.0001,
            'valid_max': 4.0
        },
        'DINO': {
            'valid_min': 0.00001,
            'valid_max': 1.0
        },
        'CRYPTO': {
            'valid_min': 0.00001,
            'valid_max': 2.0
        },
        'HAPTO': {
            'valid_min': 0.001,
            'valid_max': 1.0
        },
        'GREEN': {
            'valid_min': 0.00001,
            'valid_max': 1.0
        },
        'PROKAR': {
            'valid_min': 0.001,
            'valid_max': 1.0
        },
        'MICRO': {
            'valid_min': 0.0001,
            'valid_max': 4.0
        },
        'NANO': {
            'valid_min': 0.0001,
            'valid_max': 2.0
        },
        'PICO': {
            'valid_min': 0.0001,
            'valid_max': 1.0
        }
    }

    bs = {
        'MICRO': {
            'valid_min': 0.001,
            'valid_max': 50.0
        },
        'NANO': {
            'valid_min': 0.01,
            'valid_max': 2.0
        },
        'PICO': {
            'valid_min': 0.01,
            'valid_max': 1.0
        }
    }

    for var in med:
        line_min = f'ncatted -O -a valid_min,{var},o,f,{med[var]["valid_min"]} {file_med}'
        line_max = f'ncatted -O -a valid_max,{var},o,f,{med[var]["valid_max"]} {file_med}'
        fw.write('\n')
        fw.write(line_min)
        fw.write('\n')
        fw.write(line_max)

    for var in bs:
        line_min = f'ncatted -O -a valid_min,{var},o,f,{bs[var]["valid_min"]} {file_bs}'
        line_max = f'ncatted -O -a valid_max,{var},o,f,{bs[var]["valid_max"]} {file_bs}'
        fw.write('\n')
        fw.write(line_min)
        fw.write('\n')
        fw.write(line_max)

    fw.write('\n')
    fw.write('\n')

    return fw


def check_CCOC_878():
    from netCDF4 import Dataset
    med = {
        'DIATO': {
            'valid_min': 0.0001,
            'valid_max': 4.0
        },
        'DINO': {
            'valid_min': 0.00001,
            'valid_max': 1.0
        },
        'CRYPTO': {
            'valid_min': 0.00001,
            'valid_max': 2.0
        },
        'HAPTO': {
            'valid_min': 0.001,
            'valid_max': 1.0
        },
        'GREEN': {
            'valid_min': 0.00001,
            'valid_max': 1.0
        },
        'PROKAR': {
            'valid_min': 0.001,
            'valid_max': 1.0
        },
        'MICRO': {
            'valid_min': 0.0001,
            'valid_max': 4.0
        },
        'NANO': {
            'valid_min': 0.0001,
            'valid_max': 2.0
        },
        'PICO': {
            'valid_min': 0.0001,
            'valid_max': 1.0
        }
    }

    bs = {
        'MICRO': {
            'valid_min': 0.001,
            'valid_max': 50.0
        },
        'NANO': {
            'valid_min': 0.01,
            'valid_max': 2.0
        },
        'PICO': {
            'valid_min': 0.01,
            'valid_max': 1.0
        }
    }

    base_folder = '/store3/OC/MULTI/daily_v202311_x'
    for year in range(1997, 2025):
        work_date = dt(year, 1, 1)
        end_date = dt(year, 12, 31)
        nfound_med = 0
        ngood_med = 0
        nfound_bs = 0
        ngood_bs = 0

        while work_date <= end_date:
            yyyy = work_date.strftime('%Y')
            jjj = work_date.strftime('%j')
            file_med = os.path.join(base_folder, yyyy, jjj, f'X{yyyy}{jjj}-pft-med-hr.nc')
            file_bs = os.path.join(base_folder, yyyy, jjj, f'X{yyyy}{jjj}-pft-bs-hr.nc')
            if os.path.exists(file_med):
                nfound_med = nfound_med + 1
                valid = True
                dataset_med = Dataset(file_med)
                for var in med:
                    if var in dataset_med.variables:
                        dif_min = abs(dataset_med.variables[var].valid_min - med[var]["valid_min"])
                        dif_max = abs(dataset_med.variables[var].valid_max - med[var]["valid_max"])
                        if dif_min > 1e-8:
                            print(var, dataset_med.variables[var].valid_min, '<->', med[var]['valid_min'])
                            valid = False
                        if dif_max > 1e-8:
                            print(var, dataset_med.variables[var].valid_max, '<->', med[var]['valid_max'])
                            valid = False
                if valid:
                    ngood_med = ngood_med + 1
                else:
                    print(f'[ERROR] Error in med file: {file_med}')
                dataset_med.close()

            if os.path.exists(file_bs):
                nfound_bs = nfound_bs + 1
                valid = True
                dataset_bs = Dataset(file_bs)
                for var in bs:
                    if var in dataset_bs.variables:
                        dif_min = abs(dataset_bs.variables[var].valid_min - bs[var]["valid_min"])
                        dif_max = abs(dataset_bs.variables[var].valid_max - bs[var]["valid_max"])
                        if dif_min > 1e-8:
                            print(var, dataset_bs.variables[var].valid_min, '<->', bs[var]['valid_min'])
                            valid = False
                        if dif_max > 1e-8:
                            print(var, dataset_bs.variables[var].valid_min, '<->', bs[var]['valid_max'])
                            valid = False
                if valid:
                    ngood_bs = ngood_bs + 1
                else:
                    print(f'[ERROR] Error in bs file: {file_bs}')
                dataset_bs.close()

            work_date = work_date + timedelta(hours=24)

        print(f'YEAR: {year} MED: {ngood_med}/{nfound_med} BS: {ngood_bs}/{nfound_bs}')

    return True


def move_msi_sources():
    folder_orig = '/store/TARA_VALIDATION/sources_msi'
    folder_dest = '/store/TARA_VALIDATION/sources'
    for name in os.listdir(folder_orig):
        file_orig = os.path.join(folder_orig, name)
        date_orig = dt.strptime(name.split('_')[0], '%Y%m%d')
        file_dest = os.path.join(folder_dest, date_orig.strftime('%Y'), date_orig.strftime('%j'), name)
        print(f'Moving {file_orig} -> {file_dest}')
        os.rename(file_orig, file_dest)


def kk():
    dir_base = '/mnt/c/DATA_LUIS/TARA_TEST/station_match-ups/extracts_chl_CMEMS_OLCI_BAL'
    for name in os.listdir(dir_base):
        line = f'ncrename -v satellite_chl,satellite_CHL {os.path.join(dir_base, name)}'
        print(line)
    return True


def update_time_impl(input_file, output_file, date_here, date_last):
    # print(f'Updating file: {input_file}')
    # output_file = input_file.replace('X2023','X2024')
    # print(output_file)
    from netCDF4 import Dataset
    from datetime import datetime as dt
    from datetime import timedelta
    date_ref = dt(1981, 1, 1, 0, 0, 0)
    seconds_new = (date_here - date_ref).total_seconds()

    input_dataset = Dataset(input_file)
    ncout = Dataset(output_file, 'w', format='NETCDF4')

    # copy global attributes all at once via dictionary
    ncout.setncatts(input_dataset.__dict__)

    # copy dimensions
    for name, dimension in input_dataset.dimensions.items():
        ncout.createDimension(
            name, (len(dimension) if not dimension.isunlimited() else None))

    for name, variable in input_dataset.variables.items():
        fill_value = None
        if '_FillValue' in list(variable.ncattrs()):
            fill_value = variable._FillValue

        ncout.createVariable(name, variable.datatype, variable.dimensions, fill_value=fill_value, zlib=True,
                             shuffle=True, complevel=6)

        # copy variable attributes all at once via dictionary
        ncout[name].setncatts(input_dataset[name].__dict__)

        if name == 'time':
            seconds_prev = input_dataset[name][0]
            seconds_prev = int(seconds_prev)
            date_prev = date_ref + timedelta(seconds=seconds_prev)
            date_new = date_ref + timedelta(seconds=seconds_new)
            print(f'[INFO ] Updating date from {date_prev} to {date_new}')
            ncout[name][:] = int(seconds_new)
        else:
            ncout[name][:] = input_dataset[name][:]
    ncout.start_date = date_here.strftime('%Y-%m-%d')
    ncout.stop_date = date_last.strftime('%Y-%m-%d')
    ncout.close()
    input_dataset.close()


def remove_nr_sources_impl(path, start_date_str, end_date_str, type_s):
    from datetime import datetime as dt
    from datetime import timedelta
    if not os.path.isdir(path):
        print(f'[ERROR] {path} does not exist or it is not a directory')
        return
    try:
        start_date = dt.strptime(start_date_str, '%Y-%m-%d')
    except:
        print(f'[ERROR] Start date is not a valid date')
        return
    try:
        end_date = dt.strptime(end_date_str, '%Y-%m-%d')
    except:
        print(f'[ERROR] End date {end_date_str} is not a valid date')
        return
    if end_date < start_date:
        print(f'[ERROR] End date {end_date_str} should be greater or equal to {start_date_str}')
        return

    date_here = start_date
    while date_here <= end_date:
        print(f'[INFO] Working with date: {date_here}')
        yearstr = date_here.strftime('%Y')
        jjj = date_here.strftime('%j')
        path_date = os.path.join(path, yearstr, jjj)
        if not os.path.exists(path_date):
            continue
        for name in os.listdir(path_date):
            if name.find('_NR_') > 0:
                if type_s == "ZIP" and name.endswith('.zip'):
                    file_remove = os.path.join(path_date, name)
                    os.remove(file_remove)
                if type_s == "NC" and name.endswith('.nc'):
                    file_remove = os.path.join(path_date, name)
                    os.remove(file_remove)
                if type_s == "SEN3" and name.endswith('.SEN3'):
                    dir_remove = os.path.join(path_date, name)
                    if os.path.isdir(dir_remove):
                        for name_r in os.listdir(dir_remove):
                            os.remove(os.path.join(dir_remove, name_r))
                        os.rmdir(dir_remove)
        date_here = date_here + timedelta(hours=24)


def update_time_daily(path, start_date_str, end_date_str, preffix, suffix):
    from datetime import datetime as dt
    from datetime import timedelta
    if not os.path.isdir(path):
        print(f'[ERROR] {path} does not exist or it is not a directory')
        return
    try:
        start_date = dt.strptime(start_date_str, '%Y-%m-%d')
    except:
        print(f'[ERROR] Start date is not a valid date')
        return
    try:
        end_date = dt.strptime(end_date_str, '%Y-%m-%d')
    except:
        print(f'[ERROR] End date {end_date_str} is not a valid date')
        return
    if end_date < start_date:
        print(f'[ERROR] End date {end_date_str} should be greater or equal to {start_date_str}')
        return

    date_here = start_date
    while date_here <= end_date:
        print(f'[INFO] Working with date: {date_here}')
        yearstr = date_here.strftime('%Y')
        jjj = date_here.strftime('%j')
        path_date = os.path.join(path, yearstr, jjj)

        if not os.path.exists(path_date):
            date_here = date_here + timedelta(hours=24)
            continue
        for name in os.listdir(path_date):
            print(name, preffix, suffix)
            if not name.startswith(preffix): continue
            if not name.endswith(suffix): continue
            print('-----')
            file_in = os.path.join(path_date, name)
            name_out = f'{preffix}{yearstr}{jjj}{suffix}'
            file_out = os.path.join(path_date, name_out)
            file_temp = os.path.join(path_date, 'Temp.nc')
            update_time_impl(file_in, file_temp, date_here, date_here)
            os.rename(file_temp, file_out)
            # TEST
            # file_med = os.path.join(path,yearstr,jjj,f'X2023{jjj}-pp-med-lr.nc')
            #
            # file_blk = os.path.join(path,yearstr,jjj,f'X2023{jjj}-pp-bs-lr.nc')
            # if os.path.exists(file_med):
            #     update_time_impl(file_med,date_here)
            # if os.path.exists(file_blk):
            #     update_time_impl(file_blk, date_here)
        date_here = date_here + timedelta(hours=24)


def update_time_monthly(path, start_date_str, end_date_str, preffix, suffix):
    from datetime import datetime as dt
    from datetime import timedelta
    import calendar
    if not os.path.isdir(path):
        print(f'[ERROR] {path} does not exist or it is not a directory')
        return
    try:
        start_date = dt.strptime(start_date_str, '%Y-%m-%d')
    except:
        print(f'[ERROR] Start date is not a valid date')
        return
    try:
        end_date = dt.strptime(end_date_str, '%Y-%m-%d')
    except:
        print(f'[ERROR] End date {end_date_str} is not a valid date')
        return
    if end_date < start_date:
        print(f'[ERROR] End date {end_date_str} should be greater or equal to {start_date_str}')
        return

    date_here = start_date
    while date_here <= end_date:
        yearstr = date_here.strftime('%Y')
        print(f'[INFO] Working with year: {yearstr}')
        path_date = os.path.join(path, yearstr)
        if not os.path.exists(path_date):
            continue
        for name in os.listdir(path_date):
            if not name.startswith(preffix): continue
            if not name.endswith(suffix): continue

            date_file_str = name[name.find(preffix) + len(preffix):name.find(suffix)]
            date_file = dt.strptime(date_file_str[:-3], '%Y%j')
            date_out_start = dt(date_here.year, date_file.month, 1)
            date_out_end = dt(date_here.year, date_file.month, calendar.monthrange(date_here.year, date_file.month)[1])
            name_out = f'{preffix}{yearstr}{date_out_start.strftime("%j")}{date_out_end.strftime("%j")}{suffix}'
            file_in = os.path.join(path_date, name)
            file_out = os.path.join(path_date, name_out)
            update_time_impl(file_in, file_out, date_out_start, date_out_end)

        date_here = date_here + timedelta(days=365)


def check_sources_from_source_list_files(list_files, dir_out, fout):
    all_files = {}
    for file in list_files:
        f1 = open(file, 'r')
        for line in f1:
            line_s = [x.strip() for x in line.split(';')]
            if line_s[0] not in all_files.keys():
                all_files[line_s[0]] = [line_s[1]]
            else:
                if line_s[1] not in all_files[line_s[0]]:
                    all_files[line_s[0]].append(line_s[1])
        f1.close()

    fw = open(fout, 'w')
    fw.write('Date;NAll;NAvailable;Complete')
    for date in all_files:
        date_here = dt.strptime(date, '%Y-%m-%d')
        file_out_date = os.path.join(dir_out, date_here.strftime('%Y'), date_here.strftime('%j'))
        nall = len(all_files[date])
        ndate = 0
        for name_here in all_files[date]:
            file_here = os.path.join(file_out_date, f'{name_here}.zip')
            print(file_here)
            if os.path.exists(file_here):
                ndate = ndate + 1
        complete = 1 if ndate == nall else 0
        line = f'{date};{nall};{ndate};{complete}'
        print(line)
        fw.write('\n')
        fw.write(line)
    fw.close()


def run_distance(params):
    import numpy as np
    yPixels = params[0]
    xPixels = params[1]
    y = params[2]
    x = params[3]
    mask_land = params[4]
    dLand = params[5]
    print(f'Computing distance from: {y} , {x}')
    min_dist_yx = None
    if mask_land[y, x] == 0:  ##WATER PIXELS
        dist_yx = ((y - yPixels) ** 2) + ((x - xPixels) ** 2)
        dist_yx = dist_yx[mask_land == 1]
        min_dist_yx = np.min(dist_yx)
        # dLand[y,x] = min_dist_yx

    return min_dist_yx


def create_mask_cfc(file_mask, mask_variable, file_cfc):
    from netCDF4 import Dataset
    input_nc = Dataset(file_mask, 'r')
    land_mask = input_nc.variables[mask_variable][:]
    lat_array = input_nc.variables['lat'][:]
    lon_array = input_nc.variables['lon'][:]
    # input_nc.close()

    input_cfc = Dataset(file_cfc, 'r')
    lat_cfc = input_cfc.variables['lat'][:]
    lon_cfc = input_cfc.variables['lon'][:]
    input_cfc.close()

    nlat = lat_array.shape[0]
    nlon = lon_array.shape[0]
    y_cfc = np.zeros((nlat, nlon))
    x_cfc = np.zeros((nlat, nlon))
    index_cfc = np.zeros((nlat, nlon))
    y_cfc[:] = -999
    x_cfc[:] = -999
    index_cfc[:] = -999
    nlat_cfc = lat_cfc.shape[0]
    nlon_cfc = lon_cfc.shape[0]
    land_mask_cfc = np.zeros((nlat_cfc, nlon_cfc))

    for y in range(nlat):
        print(y, '/', nlat)
        for x in range(nlon):
            row, column = find_row_column_from_lat_lon(lat_cfc, lon_cfc, lat_array[y], lon_array[x])
            index = (row * nlon_cfc) + column
            y_cfc[y, x] = row
            x_cfc[y, x] = column
            index_cfc[y, x] = index
            if land_mask[y, x] == 1:
                land_mask_cfc[row, column] = 1

    y_middle = int(nlat_cfc / 2)
    x_ini = -1
    for x in range(nlon_cfc):
        if land_mask_cfc[y_middle, x] == 1:
            x_ini = x
            break
    print(f'[INFO] XIni: {x_ini}')
    if x_ini >= 0: land_mask_cfc[:, 0:x_ini] = 1

    x_end = -1
    for x in range(nlon_cfc - 1, 0, -1):
        if land_mask_cfc[y_middle, x] == 1:
            x_end = x + 1
            break
    print(f'[INFO] XEnd: {x_end}')
    if x_end < nlon_cfc: land_mask_cfc[:, x_end:nlon_cfc] = 1

    x_middle = int(nlon_cfc / 2)
    y_ini = -1
    for y in range(nlat_cfc):
        if land_mask_cfc[y, x_middle] == 1:
            y_ini = y
            break
    print(f'[INFO] YIni: {y_ini}')
    if y_ini >= 0: land_mask_cfc[0:y_ini, :] = 1
    y_end = -1
    for y in range(nlat_cfc - 1, 0, -1):
        if land_mask_cfc[y, x_middle] == 1:
            y_end = y + 1
            break
    print(f'[INFO] YEnd: {y_end}')
    if y_end < nlat_cfc: land_mask_cfc[y_end:nlat_cfc, :] = 1

    cfc_mask = land_mask.copy()
    for y in range(nlat):

        print(y, '-->', nlat)
        for x in range(nlon):
            row = int(y_cfc[y, x])
            column = int(x_cfc[y, x])
            # print(row,column)
            if land_mask_cfc[row, column] == 1:
                cfc_mask[y, x] = 1

    file_out = f'{file_mask[:-3]}_CFC.nc'
    print(f'[INFO] Creating output file: {file_out}')
    ncout = Dataset(file_out, 'w')
    ncout.createDimension('lat', nlat)
    ncout.createDimension('lon', nlon)
    ncout.createDimension('lat_cfc', nlat_cfc)
    ncout.createDimension('lon_cfc', nlon_cfc)
    for name, variable in input_nc.variables.items():
        fill_value = None
        if '_FillValue' in list(variable.ncattrs()):
            fill_value = variable._FillValue
        ncout.createVariable(name, variable.datatype, variable.dimensions, fill_value=fill_value, zlib=True,
                             complevel=6)
        # copy variable attributes all at once via dictionary
        ncout[name].setncatts(input_nc[name].__dict__)
        # copy data
        ncout[name][:] = input_nc[name][:]
    input_nc.close()

    var = ncout.createVariable('CFC_Mask', 'i2', ('lat', 'lon'), zlib=True, complevel=6)
    var[:] = cfc_mask
    var = ncout.createVariable('CFC_Y', 'i2', ('lat', 'lon'), zlib=True, complevel=6)
    var[:] = y_cfc
    var = ncout.createVariable('CFC_X', 'i2', ('lat', 'lon'), zlib=True, complevel=6)
    var[:] = x_cfc
    var = ncout.createVariable('CFC_Index', 'i2', ('lat', 'lon'), zlib=True, complevel=6)
    var[:] = index_cfc

    var_lat = ncout.createVariable('lat_cfc', 'f4', ('lat_cfc',), zlib=True, complevel=6, fill_value=-999)
    var_lat[:] = lat_cfc
    var_lon = ncout.createVariable('lon_cfc', 'f4', ('lon_cfc',), zlib=True, complevel=6, fill_value=-999)
    var_lon[:] = lon_cfc
    var_land = ncout.createVariable('Land_Mask_CFC', 'i2', ('lat_cfc', 'lon_cfc'), zlib=True, complevel=6)
    var_land[:] = land_mask_cfc

    ncout.close()

    print(f'[INFO] Completed')


def create_mask(file_in, file_out, mask_variable, file_ref):
    from netCDF4 import Dataset
    import numpy as np
    input_nc = Dataset(file_in, 'r')
    mask_land = input_nc.variables[mask_variable][:]
    nlat = mask_land.shape[0]
    nlon = mask_land.shape[1]
    input_nc.close()

    ref_nc = Dataset(file_ref, 'r')
    lat = ref_nc.variables['lat'][:]
    lon = ref_nc.variables['lon'][:]
    ref_nc.close()
    if nlat != lat.shape[0]:
        print('[ERROR] Dimension lat in file_ref does not coincide with the first dimension in the mask')
        return
    if nlon != lon.shape[0]:
        print('[ERROR] Dimension lat in file_ref does not coincide with the first dimension in the mask')
        return

    lon_limit = 13.070
    x_min = -1
    for x in range(lon.shape[0]):
        if lon[x] >= lon_limit:
            x_min = x
            break
    print(f'[INFO] Setting min_x to: {x_min} (Longitude: {lon[x_min]})')

    # dLand = np.zeros((nlat,nlon))
    # xPixels = np.tile(np.arange(nlon),(nlat,1))
    # yPixels = np.tile(np.array([np.arange(nlat)]).transpose(), (1, nlon))
    # from multiprocessing import Pool
    #
    # param_list = []
    # all_ypoints = []
    # all_xpoints = []
    # for y in range(0,nlat):
    #     print(y)
    #     for x in range(0,nlon):
    #         if mask_land[y,x]==0:#WATER
    #             param_list.append([yPixels,xPixels,y,x,mask_land,dLand])
    #             all_ypoints.append(y)
    #             all_xpoints.append(x)
    #
    # print(f'[INFO] Number of distances to be computed: {len(param_list)}')
    # for idx in range(0,len(param_list),1000):
    #     start = idx
    #     end = idx+1000
    #     if end>len(param_list):
    #         end = len(param_list)
    #     param_list_here = param_list[start:end]
    #     poolhere = Pool(8)
    #     res_here = poolhere.map(run_distance,param_list_here)
    #     ypoints = all_ypoints[start:end]
    #     xpoints = all_xpoints[start:end]
    #     print(len(ypoints))
    #     print(len(xpoints))
    #     dLand[ypoints,xpoints] = res_here[:]
    # for idx in range(len(res_here)):
    #     y = param_list[idx][2]
    #     x = param_list[idx][3]
    #     dLand[y,x] = res_here[idx]

    # res = poolhere.map(run_distance,param_list)
    #

    # for params,val in zip(param_list,poolhere.map(run_distance,param_list)):
    #     y = params[2]
    #     x = params[3]
    #     print(y,x,val)
    #     if val is not None:
    #         dLand[y,x] = val
    #         print(y,x,'-->',val)
    # poolhere.map(run_distance, param_list)

    # for y in range(60):
    #     print(f'[INFO] Computing distances for line: {y}')
    #     for x in range(nlon):
    #         if mask_land[y,x]==0:##WATER PIXELS
    #             dist_yx = ((y-yPixels)**2) + ((x-xPixels)**2)
    #             dist_yx = dist_yx[mask_land==1]
    #             min_dist_yx = np.min(dist_yx)
    #             dLand[y,x] = min_dist_yx

    # dLand = np.square(dLand)

    print('[INFO] Creating output file...')
    nc_out = Dataset(file_out, 'w')
    nc_out.createDimension('lat', nlat)
    nc_out.createDimension('lon', nlon)
    var_lat = nc_out.createVariable('lat', 'f4', ('lat',), zlib=True, complevel=6, fill_value=-999)
    var_lat[:] = lat
    var_lon = nc_out.createVariable('lon', 'f4', ('lon',), zlib=True, complevel=6, fill_value=-999)
    var_lon[:] = lon
    var_land = nc_out.createVariable('Land_Mask', 'i2', ('lat', 'lon'), zlib=True, complevel=6)
    mask_land[:, 0:x_min] = 1
    var_land[:] = mask_land
    # var_dist= nc_out.createVariable('Dist_Land', 'f4', ('lat', 'lon'), zlib=True, complevel=6)
    # var_dist[:] = dLand

    nc_out.close()
    print(f'[INFO] Completed')


def apply_mask(input_path, name_file, file_mask, mask_variable, start_date, end_date):
    from netCDF4 import Dataset
    dmask = Dataset(file_mask)
    if not mask_variable in dmask.variables:
        print(f'[ERROR] Mask variable {mask_variable} is not available in file {file_mask}')
        dmask.close()
        return
    mask = dmask.variables[mask_variable][:]
    dmask.close()
    work_date = start_date
    while work_date <= end_date:
        yyyy = work_date.strftime('%Y')
        if name_file.find('DATEM') > 0:
            path_date = os.path.join(input_path, yyyy)
            date_month_ini = dt(work_date.year, work_date.month, 1)
            date_month_end = dt(work_date.year, work_date.month,
                                calendar.monthrange(work_date.year, work_date.month)[1])
            monthstr = f'{yyyy}{date_month_ini.strftime("%j")}{date_month_end.strftime("%j")}'
            input_file = os.path.join(path_date, name_file.replace('DATEM', f'{monthstr}'))
        else:
            jjj = work_date.strftime('%j')
            path_date = os.path.join(input_path, yyyy, jjj)
            input_file = os.path.join(path_date, name_file.replace('DATE', f'{yyyy}{jjj}'))
        if not os.path.exists(input_file):
            print(f'[WARNING] Input file {input_file} does not exist. Skipping...')
            work_date = work_date + timedelta(hours=24)
            continue
        print(f'[INFO] -----------------------------------------------------------------------------------------------')
        print(f'[INFO] Worning with: {input_file}')
        file_tmp = os.path.join(path_date, 'Temp.nc')
        mask_applied = apply_mask_impl(input_file, mask, file_tmp)
        if mask_applied:
            print(f'[INFO] Masking completed')
            os.rename(file_tmp, input_file)
        else:
            print(f'[WARNING] Masking was not applied as input file has not changed')
            os.remove(file_tmp)
        print(f'[INFO] -----------------------------------------------------------------------------------------------')
        if name_file.find('DATEM') > 0:
            if work_date.month == 12:
                work_date = dt(work_date.year + 1, 1, 15)
            else:
                work_date = dt(work_date.year, work_date.month + 1, 15)
        else:
            work_date = work_date + timedelta(days=1)

    return


def apply_mask_impl(input_file, mask, output_file):
    mask_applied = False
    from netCDF4 import Dataset
    nc_input = Dataset(input_file, 'r')
    nc_out = Dataset(output_file, 'w')

    # copy global attributes all at once via dictionary
    nc_out.setncatts(nc_input.__dict__)

    # copy dimensions
    for name, dimension in nc_input.dimensions.items():
        nc_out.createDimension(
            name, (len(dimension) if not dimension.isunlimited() else None))

    for name, variable in nc_input.variables.items():
        fill_value = -999.0
        if '_FillValue' in list(variable.ncattrs()):
            fill_value = variable._FillValue

        nc_out.createVariable(name, variable.datatype, variable.dimensions, fill_value=fill_value, zlib=True,
                              complevel=6)

        # copy variable attributes all at once via dictionary
        nc_out[name].setncatts(nc_input[name].__dict__)

        if len(variable.dimensions) == 3 and nc_input.variables[name].shape[1] == mask.shape[0] and \
                nc_input.variables[name].shape[2] == mask.shape[1]:
            print(f'[INFO] Applying mask to variable: {name}')
            array = np.squeeze(nc_input[name][:])
            array[mask == 1] = fill_value
            nc_out[name][0, :, :] = array[:, :]
            mask_applied = True
        else:
            nc_out[name][:] = nc_input[name][:]

    nc_input.close()
    nc_out.close()

    return mask_applied


def test_impl(input_file, output_file):
    from netCDF4 import Dataset
    import numpy as np
    nc_input = Dataset(input_file, 'r')
    nc_out = Dataset(output_file, 'w')

    # copy global attributes all at once via dictionary
    nc_out.setncatts(nc_input.__dict__)

    # copy dimensions
    for name, dimension in nc_input.dimensions.items():
        nc_out.createDimension(
            name, (len(dimension) if not dimension.isunlimited() else None))

    variables_to_flipup = ['RRS412', 'RRS443', 'RRS490', 'RRS510', 'RRS560', 'RRS665']
    for name, variable in nc_input.variables.items():
        fill_value = -999.0
        if '_FillValue' in list(variable.ncattrs()):
            fill_value = variable._FillValue

        nc_out.createVariable(name, variable.datatype, variable.dimensions, fill_value=fill_value, zlib=True,
                              complevel=6)

        # copy variable attributes all at once via dictionary
        nc_out[name].setncatts(nc_input[name].__dict__)

        if name in variables_to_flipup:
            array = np.array(nc_input[name][:])
            array = np.flipud(array)
            nc_out[name][:] = array
        else:
            nc_out[name][:] = nc_input[name][:]

    nc_input.close()
    nc_out.close()


def tal():
    from netCDF4 import Dataset
    # file_old = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION_202411/MASKS/M1997263.0000.bal.all_products.CCI.20Sep970000.v0.19972630000.data.nc'
    # file_new = os.path.join(os.path.dirname(file_old),'M2024132.0000.bal.all_products.CCI.11May240000.v0.20241320000.data.nc')
    #
    # dold = Dataset(file_old)
    # lat_old = dold.variables['latitude'][:]
    # lon_old = dold.variables['longitude'][:]
    # dold.close()
    #
    # dnew = Dataset(file_new)
    # lat_new = dnew.variables['lat'][:]
    # lon_new = dnew.variables['lon'][:]
    # dnew.close()
    #
    # fout = os.path.join(os.path.dirname(file_old),'LatLonBal2.csv')
    # fw = open(fout,'w')
    # first_line = 'Index;LatOld(1147);LonOld(1185);LatNew(1210);LonNew(1992)'
    # fw.write(first_line)
    # for index in range(1992):
    #     lat_old_v = lat_old[index] if index < len(lat_old) else -999
    #     lon_old_v = lon_old[index] if index < len(lon_old) else -999
    #     lat_new_v = lat_new[index] if index < len(lat_new) else -999
    #     lon_new_v = lon_new[index] if index < len(lon_new) else -999
    #     line = f'{index};{lat_old_v};{lon_old_v};{lat_new_v};{lon_new_v}'
    #     fw.write('\n')
    #     fw.write(line)
    # fw.close()
    dir_base = '/store3/OC/CCI_v2017/V6_incoming'
    # dir_base = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION_202411/MASKS'
    file_out = os.path.join(dir_base, 'variables_by_file_2.csv')
    fw = open(file_out, 'w')
    fw.write('File;NVariables;Variables;DimLat;DimLon')
    for name in os.listdir(dir_base):
        if name.endswith('data.nc'):
            file_nc = os.path.join(dir_base, name)
            dataset = Dataset(file_nc)
            nlat = dataset.variables['Rrs_412'].shape[1]
            nlon = dataset.variables['Rrs_412'].shape[2]
            variables = list(dataset.variables)
            var_str = ','.join(variables)
            line = f'{name};{len(variables)};{var_str};{nlat};{nlon}'
            fw.write('\n')
            fw.write(line)
            dataset.close()

    fw.close()

    return True


def cual():
    from netCDF4 import Dataset
    dir_base = '/store3/OC/OLCI_BAL/dailyolci_202411'
    work_date = dt(2024, 10, 1)
    end_date = dt(2024, 11, 10)
    while work_date <= end_date:
        yyyy = work_date.strftime('%Y')
        jjj = work_date.strftime('%j')
        file_nc = os.path.join(dir_base, yyyy, jjj, f'O{yyyy}{jjj}-chl-bal-fr.nc')
        dataset = Dataset(file_nc)
        varmask = dataset.variables['SENSORMASK']
        print(work_date.strftime('%Y-%m-%d'), varmask.dimensions)
        dataset.close()
        work_date = work_date + timedelta(hours=24)


def add_quality_control_var(dir_base):
    for name in os.listdir(dir_base):
        if name.startswith('HYPERNETS_W_DAY') and name.endswith('.nc'):
            file_in = os.path.join(dir_base, name)
            file_out = os.path.join(dir_base, f'Temp_{name}')
            if add_quality_control_var_impl(file_in, file_out):
                os.rename(file_out, file_in)
    return True


def add_quality_control_var_impl(file_in, file_out):
    from netCDF4 import Dataset
    input_dataset = Dataset(file_in)
    ncout = Dataset(file_out, 'w', format='NETCDF4')

    # copy global attributes all at once via dictionary
    ncout.setncatts(input_dataset.__dict__)

    # copy dimensions
    for name, dimension in input_dataset.dimensions.items():
        ncout.createDimension(
            name, (len(dimension) if not dimension.isunlimited() else None))

    # copy variables
    for name, variable in input_dataset.variables.items():
        fill_value = None
        if '_FillValue' in list(variable.ncattrs()):
            fill_value = variable._FillValue

        ncout.createVariable(name, variable.datatype, variable.dimensions, fill_value=fill_value, zlib=True,
                             shuffle=True, complevel=6)
        # copy variable attributes all at once via dictionary
        ncout[name].setncatts(input_dataset[name].__dict__)

        ncout[name][:] = input_dataset[name][:]

    quality_flag = input_dataset.variables['l2_quality_flag'][:]
    epsilon = input_dataset.variables['l2_epsilon'][:]
    nseries = len(quality_flag)
    quality_control_array = np.zeros((nseries,))
    for idx in range(nseries):
        if quality_flag[idx] == 0 and (-0.05) <= epsilon[idx] <= 0.05:
            quality_control_array[idx] = 1
    nvalid = np.sum(quality_control_array)
    ninvalid = nseries - nvalid
    print(
        f'[INFO] {os.path.basename(file_in)} -> NTotal:{len(quality_control_array)} NValid: {nvalid} NInvalid: {ninvalid}')

    var = ncout.createVariable('quality_control', 'i2', ('series',), complevel=6, zlib=True)
    var[:] = quality_control_array
    var.description = 'Valid sequence after passing quality control protocols: l2_quality_flag=0, (-0.05)<=l2_epsilon<=0.05'

    ncout.close()
    input_dataset.close()

    return True


def check_lat_lon_certo():
    from netCDF4 import Dataset
    dir_base = '/store3/DOORS/CERTO_SOURCES'
    work_date = dt(2024, 6, 1)
    end_date = dt(2024, 6, 19)
    while work_date <= end_date:
        yyyy = work_date.strftime('%Y')
        jjj = work_date.strftime('%j')
        dir_date = os.path.join(dir_base, yyyy, jjj)
        if not os.path.isdir(dir_date):
            continue
        for name in os.listdir(dir_date):
            if name.find('MSI') > 0:
                file_nc = os.path.join(dir_date, name)
                dataset = Dataset(file_nc)
                lat_array = dataset.variables['lat'][:]
                lon_array = dataset.variables['lon'][:]
                min_lat = np.min(lat_array)
                max_lat = np.max(lat_array)
                min_lon = np.min(lon_array)
                max_lon = np.max(lon_array)
                line = f'{work_date.strftime("%Y-%m-%d")};{min_lat};{max_lat};{min_lon};{max_lon}'
                print(line)
                dataset.close()

        work_date = work_date + timedelta(hours=24)

    return True


def update_time_extracts(dir_extracts_certo, dir_extracts_cmems):
    from netCDF4 import Dataset
    print(f'[INFO] Update time of CMEMS time extracts using time stamps from CERTO extracts')
    # dir_extracts_certo = '/store3/DOORS/extracts/certo_olci'
    # dir_extracts_cmems = '/store3/DOORS/extracts/cmems_olci'
    # dir_extracts_certo = '/mnt/c/DATA_LUIS/DOORS_WORK/Extracts_2024/extracts_certo_olci'
    # dir_extracts_cmems = '/mnt/c/DATA_LUIS/DOORS_WORK/Extracts_2024/extracts_cmems_olci'
    start_date = dt(2016, 4, 1)
    end_date = dt(2024, 11, 30)
    olci_date_timestamps = {}
    file_temp = '/mnt/c/DATA_LUIS/DOORS_WORK/INSITU/in_situ_orig/DOOR_insitu_BlackSea_AeronetOC_Section-7_msi_2023.csv'
    # fw = open(file_temp,'w')
    # fw.write('System;Station;Date;Time;Lat;Long;Source')
    # preline = 'Danube Delta;Section-7_Platform;'
    # postline = ';12:00;44.5458;29.4466;AERONET-OC'
    for name in os.listdir(dir_extracts_certo):
        file_extract = os.path.join(dir_extracts_certo, name)
        dataset = Dataset(file_extract)

        ts = np.float64(dataset.variables['satellite_time'][:])
        lat_array = dataset.variables['satellite_latitude'][:]
        dataset.close()
        date_here = dt.utcfromtimestamp(ts)
        if start_date <= date_here <= end_date:
            date_str = date_here.strftime('%Y%m%d')
            olci_date_timestamps[date_str] = ts
            print(f'[INFO] Time stamp for {date_str} is {date_here.strftime("%Y-%m-%d %H:%M:%S")} {lat_array[0,12,12]}')
            # line = f'{preline}{date_here.strftime("%Y-%m-%d")}{postline}'
            # fw.write('\n')
            # fw.write(line)
            # print(f'{date_here.strftime("%Y-%m-%d %H:%M:%S")}')
    #fw.close()
    if dir_extracts_cmems is None:
        return
    for name in os.listdir(dir_extracts_cmems):
        file_extract = os.path.join(dir_extracts_cmems, name)
        dataset = Dataset(file_extract)
        ts = np.float64(dataset.variables['satellite_time'][:])
        dataset.close()
        date_here = dt.utcfromtimestamp(ts)
        if start_date <= date_here <= end_date:
            date_str = date_here.strftime('%Y%m%d')
            if date_str in olci_date_timestamps.keys():
                file_out = os.path.join(dir_extracts_cmems, f'Temp_{date_str}.nc')
                ts_new = olci_date_timestamps[date_str]
                print(
                    f'[INFO] Updating time in extract file {file_extract} from {date_here.strftime("%Y-%m-%d %H:%M:%S")} to {dt.utcfromtimestamp(ts_new).strftime("%Y-%m-%d %H:%M:%S")}')
                array_new = np.array([ts_new], dtype=np.float64)
                creating_copy_correcting_band_bis(file_extract, file_out, 'satellite_time', array_new)
                os.rename(file_out, file_extract)


def creating_copy_correcting_band_bis(file_in, file_out, band_to_correct, new_array):
    # reader = MDB_READER('', True)
    from netCDF4 import Dataset
    import numpy as np
    ncin = Dataset(file_in)
    ncout = Dataset(file_out, 'w', format='NETCDF4')

    # copy global attributes all at once via dictionary
    ncout.setncatts(ncin.__dict__)

    # copy dimensions
    for name, dimension in ncin.dimensions.items():
        ncout.createDimension(name, (len(dimension) if not dimension.isunlimited() else None))

    # copy variables
    for name, variable in ncin.variables.items():
        fill_value = None
        if '_FillValue' in list(ncin.ncattrs()):
            fill_value = variable._FillValue

        ncout.createVariable(name, variable.datatype, variable.dimensions, fill_value=fill_value, zlib=True,
                             shuffle=True, complevel=6)

        # copy variable attributes all at once via dictionary
        ncout[name].setncatts(ncin[name].__dict__)

        # copy variable data
        ncout[name][:] = ncin[name][:]
        if name == 'satellite_Rrs' and band_to_correct is None and new_array is None:
            ncout[name][:, 1, :, :] = ncin[name][:, 1, :, :] * np.pi
        if band_to_correct is not None and name == band_to_correct:
            ncout[name][:] = new_array[:]

    ncout.close()
    ncin.close()
    return True

def correct_time():
    from netCDF4 import Dataset
    import pytz
    dir_base = '/mnt/c/DATA_LUIS/DOORS_WORK/Extracts_2024/extracts_certo_msi'
    all_dates = {}
    dir_refs = ['certo_msi_gloria','certo_msi_section-7']
    dir_mod = os.path.join(dir_base,'certo_msi_galata')
    file_mod_times = os.path.join(dir_base,'galata_old_new.csv')
    # fw = open(file_mod_times,'w')
    # fw.write('File;OldTime;NewTime')
    # for ref in dir_refs:
    #     dir_ref = os.path.join(dir_base,ref)
    #     for name in os.listdir(dir_ref):
    #
    #         file = os.path.join(dir_ref,name)
    #         dataset = Dataset(file,'r')
    #         ts = np.float64(dataset.variables['satellite_time'][:])
    #         time_here = dt.utcfromtimestamp(ts)
    #         ref_time = time_here.strftime('%Y%m%d')
    #
    #         all_dates[ref_time] = time_here.strftime('%Y-%m-%d %H:%M:%S')
    #         ts_new = dt.strptime(all_dates[ref_time],'%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc).timestamp()
    #         print('-->', name, '??', all_dates[ref_time],'->',ts,' ',ts_new)
    #         dataset.close()
    # for name in os.listdir(dir_mod):
    #     file_mod = os.path.join(dir_mod,name)
    #     dataset = Dataset(file_mod, 'r')
    #     ts = np.float64(dataset.variables['satellite_time'][:])
    #     time_here = dt.utcfromtimestamp(ts)
    #     ref_time = time_here.strftime('%Y%m%d')
    #     old_time = time_here.strftime('%Y-%m-%d %H:%M:%S')
    #     dataset.close()
    #     new_time = 'NaN'
    #     if ref_time in all_dates:
    #         new_time = all_dates[ref_time]
    #     print(f'{name}-->{old_time},-->{new_time}')
    #     line = f'{name};{old_time};{new_time}'
    #     fw.write('\n')
    #     fw.write(line)
    #
    # fw.close()

    df = pd.read_csv(file_mod_times,sep=';')
    for index,row in df.iterrows():
        file = os.path.join(dir_mod,row['File'])
        error = int(row['with_error'])
        if error==1:
            new_time_str = row['NewTime']
            time_new =   dt.strptime(new_time_str,'%Y-%m-%d %H:%M').replace(tzinfo=pytz.utc)

            print(file,'->',os.path.exists(file),'::',time_new)
            date_str = time_new.strftime('%Y%m%d')
            file_out = os.path.join(dir_mod, f'Temp_{date_str}.nc')
            ts_new = time_new.timestamp()
            array_new = np.array([ts_new], dtype=np.float64)
            creating_copy_correcting_band_bis(file, file_out, 'satellite_time', array_new)
            os.rename(file_out, file)


def do_test():
    file_nc = "/mnt/c/Users/LuisGonzalez/OneDrive - NOLOGIN OCEANIC WEATHER SYSTEMS S.L.U/CNR/ITALIAN_SITES_VALIDATION_PUBLICATION/OCI/VEIT/MDB_rc_PACE_OCI_1KM_HYPSTAR_VEIT_COMMONMU_V3WL.nc"
    from netCDF4 import Dataset
    dataset = Dataset(file_nc)
    sat_time = dataset.variables['satellite_time'][:]
    mu_valid_common = dataset.variables['mu_valid_common'][:]
    for muv,stime in zip(mu_valid_common,sat_time):
        if muv==1:
            stime = dt.utcfromtimestamp(stime)
            print(stime.strftime('%Y-%m-%d'))
    dataset.close()

def do_test2():
    # dir_base = '/mnt/c/DATA/FICE2025/GROUP_ASSIGMENTS/pysas/output/L1A'
    # file_hdf = os.path.join(dir_base,'pySAS004_20250506_003348_L1A.hdf')
    # file_out = os.path.join(dir_base,'pySAS004_20250506_003348_L1A_out.hdf')
    # from h5py import Dataset
    # import h5py
    # print(os.path.exists(file_hdf))
    # f = h5py.File(file_hdf, 'r')
    # print(f.keys())
    dir_base = '/mnt/c/DATA/FICE2025/GROUP_ASSIGMENTS/out'
    fout = os.path.join(os.path.dirname(dir_base),'OutWaypoints_05062025.csv')
    fw = open(fout,'w')
    for name in os.listdir(dir_base):
        print(name)
        file_in = os.path.join(dir_base,name)
        fr = open(file_in,'r')
        n = 0
        for line in fr:
            if n>=2:
                fw.write(line)
            n = n +1


        fr.close()
    fw.close()


def ele():
    dir_base = '/mnt/c/DATA/satdata'
    file_pace = os.path.join(dir_base,'PACE_OCI.20250505T060115.L2.OC_AOP.V3_0.NRT.nc')
    from netCDF4 import Dataset
    dataset = Dataset(file_pace)
    lat_array = dataset.groups['navigation_data'].variables['latitude'][:]
    lon_array = dataset.groups['navigation_data'].variables['longitude'][:]
    dist_squared = ((lat_array - 13.1091) ** 2) + ((lon_array - 88.8514) ** 2)
    print(lat_array.shape)
    print(lon_array.shape)
    print(dist_squared.shape)
    r, c = np.unravel_index(np.argmin(dist_squared),
                            lat_array.shape)  # index to the closest in the latitude and longitude arrays

    print(r,c)
    print(lat_array[r,c])
    print(lon_array[r,c])
    dataset.close()
    return True

def compare_files(file1,file2):
    from netCDF4 import Dataset
    import warnings
    warnings.filterwarnings("ignore")
    dataset1 = Dataset(file1)
    dataset2 = Dataset(file2)
    #dimensions
    dims1 = dataset1.dimensions
    dims2 = dataset2.dimensions
    status = 'OK'
    errors = []
    if len(dims1)!=len(dims2):
        errors.append(f'Number of dimensions is not the same: {len(dims1)} vs. {len(dims2)}')
    for dim in dims1:
        if dim not in dims2:
            errors.append(f'Dimension {dim} is in file1 but not in file2')
        else:
            size1 = len(dataset1.dimensions[dim])
            size2 = len(dataset2.dimensions[dim])
            if size1 != size2:
                errors.append(f'Dimensions {dim} have different sizes: {size1} vs. {size2}')
    for dim in dims2:
        if dim not in dims1:
            errors.append(f'Dimension {dim} is in file2 but not in file1')
    if len(errors)>0:
        status = 'ERROR'
    print(f'Dimensions: {status}')
    if len(errors)>0:
        print(f'{len(errors)} errors')
        for error in errors:
            print(f'[ERROR]{error}')
    print('------------------------------------')

    # variables
    vars1 = dataset1.variables
    vars2 = dataset2.variables
    status = 'OK'
    errors = []
    if len(vars1) != len(vars2):
        errors.append(f'Number of variables is not the same: {len(vars1)} vs. {len(vars2)}')
    for var in vars1:
        if var not in vars2:
            errors.append(f'Variable {var} is in file1 but not in file2')
        else:
            size1 = dataset1.variables[var].shape
            size2 = dataset2.variables[var].shape
            if size1 != size2:
                errors.append(f'Variables {var} have different sizes: {size1} vs. {size2}')
    for var in vars2:
        if var not in vars1:
            errors.append(f'Variable {var} is in file2 but not in file1')
    if len(errors) > 0:
        status = 'ERROR'
    print(f'Variables: {status}')
    if len(errors) > 0:
        print(f'{len(errors)} errors')
        for error in errors:
            print(f'[ERROR]{error}')
    print('------------------------------------')

    #global attrs
    attrs1 = dataset1.__dict__
    attrs2 = dataset2.__dict__
    compare_attrs('Global attrs',attrs1,attrs2)
    print('------------------------------------')

    for var in vars1:
        if var in vars2:
            attrs1 = dataset1.variables[var].__dict__
            attrs2 = dataset2.variables[var].__dict__
            compare_attrs(f'Variable {var}',attrs1,attrs2)
    print('------------------------------------------------------')
    print('DATA CHECK:')
    for var in vars1:
        if var in vars2:
            array1 = dataset1.variables[var][:]
            array2 = dataset2.variables[var][:]
            div = array1/array2
            print(f'Var {var} {np.min(div)} {np.max(div)}')


    dataset1.close()
    dataset2.close()

def compare_attrs(ref,attrs1,attrs2):
    errors = []
    warnings = []
    for key in attrs1:
        if not key in attrs2:
            errors.append(f'{key} is available in file 1 but not available in file 2')
        else:
            if attrs1[key] != attrs2[key]:
                warnings.append(f'{key}: {attrs1[key]} not equal to {attrs2[key]}')
    for key in attrs2:
        if not key in attrs1:
            errors.append(f'{key} is available in file 1 but not available in file 2')

    status = 'OK'
    if len(errors)>0:
        status = 'ERROR'
    elif len(warnings)>0 and len(errors)==0:
        status = 'WARNING'

    print(f'{ref}->{status}')
    if len(errors)>0:
        print(f'{len(errors)} errors')
        for error in errors:
            print(f'[ERROR]{error}')
    if len(warnings)>0:
        print(f'{len(warnings)} warnings')
        for warning in warnings:
            print(f'[WARNING]{warning}')

def main():
    # if ele():
    #     return
    # if tal():
    #     return
    #if check_download():
    #    return
    if args.mode == 'TEST':


        from html_download import  OC_CCI_V6_Download
        cciDownload = OC_CCI_V6_Download()
        cciDownload.overwritte = False
        status = cciDownload.download_date(dt(2024,3,24),'/mnt/c/DATA')
        if status==0 and cciDownload.check_file_date('/mnt/c/DATA',dt(2024,3,24)):
            print('termina')
        # do_test2()
        # from netCDF4 import Dataset
        # file_nc = '/mnt/c/DATA_LUIS/DOORS_WORK/Extracts_2024/AERONET_OC/MDB_CERTO_OLCI_300M_CERTO-OLCI-L3_20190827T000000_20240818T000000_AERONET_Section-7_Platform.nc'
        # dataset = Dataset(file_nc,'r')
        # time = dataset.variables['satellite_time'][:]
        # for t in time:
        #     print(dt.utcfromtimestamp(t))
        # dataset.close()
        # if check_lat_lon_certo():
        #     return
        # if add_quality_control_var('/store3/HYPERNETS/INSITU_HYPSTARv2.1.0_DEV_QC/TOSHARE/NC'):
        #     return
        # if tal():
        #     return
        # input_path = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION_202411/MATCH-UPS_ANALYSIS_2024/extracts_complete/M1997267.0000.bal.all_products.CCI.24Sep970000.v0.19972670000.data_BAL202411_prev.nc'
        # output_path = os.path.join(os.path.dirname(input_path),'Temp.nc')
        # input_path = '/store3/OC/CCI_v2017/daily_v202411'
        # start_date, end_date = get_dates()
        # if start_date is None or end_date is None:
        #     return
        # name_file = 'MDATE1.0000.bal.all_products.CCI.DATE20000.v0.DATE10000.data_BAL202411.nc'
        # work_date = start_date
        # while work_date <= end_date:
        #     yyyy = work_date.strftime('%Y')
        #     jjj = work_date.strftime('%j')
        #     path_date = os.path.join(input_path, yyyy, jjj)
        #     input_file = os.path.join(path_date, name_file.replace('DATE1', f'{yyyy}{jjj}'))
        #     input_file = input_file.replace('DATE2', work_date.strftime('%d%b%y'))
        #     if not os.path.exists(input_file):
        #         print(f'[WARNING] Input file {input_file} does not exist. Skipping...')
        #         work_date = work_date + timedelta(hours=24)
        #         continue
        #     output_path = os.path.join(path_date, 'TempKK.nc')
        #     test_impl(input_file, output_path)
        #     os.rename(output_path, input_file)
        #
        #     work_date = work_date + timedelta(hours=24)

        # from eumdac_lois import EUMDAC_LOIS
        # edac = EUMDAC_LOIS(True, None)

        # file_base_list_files = '/store3/OC/OLCI_BAL/org_scripts'
        # #file_base_list_files = '/mnt/c/DATA_LUIS/OCTACWORK'
        # list_files = []
        # for name in os.listdir(file_base_list_files):
        #     if name.startswith('GranulesToDownload2024'):
        #         list_files.append(os.path.join(file_base_list_files,name))
        #
        # check_sources_from_source_list_files(list_files,'/store3/OC/OLCI_BAL/SOURCES',os.path.join(file_base_list_files,'SOURCES_2024_CHECK.txt'))
        # check_sources_from_source_list_files(list_files, '/store3/OC/OLCI_BAL/SOURCES_EXTRA',
        #                                      os.path.join(file_base_list_files, 'SOURCES_EXTRA_2024_CHECK.txt'))
        # check_sources_from_source_list_files(list_files, '/store/COP2-OC-TAC/BAL_Evolutions/sources',
        #                                      os.path.join(file_base_list_files, 'BAL_EVOLUTION_sources_2024_CHECK.txt'))

        # for year in range(1998,2025):
        #     resolve_CCOC_878(year)
        # resolve_CCOC_878(2024)
        # check_CCOC_878()
        ##move_msi_sources()
        ##kk()

        # update_time(args.path,args.start_date,args.end_date)
        #correct_tim
        # dir_extracts_cmems = None
        # if args.path:
        #     dir_extracts_certo = args.path
        # if args.output_path:
        #     dir_extracts_cmems = args.output_path
        # if dir_extracts_certo is not None:
        #     update_time_extracts(dir_extracts_certo,dir_extracts_cmems)
        return

    if args.mode == 'COMPARE_NC':
        #dir_base1='/mnt/c/Users/LuisGonzalez/OneDrive - NOLOGIN OCEANIC WEATHER SYSTEMS S.L.U/CNR/OCTAC_WORK/UPLOAD_CODE_NEW_VERSION/COMPARISON/OLD'
        #dir_base2='/mnt/c/Users/LuisGonzalez/OneDrive - NOLOGIN OCEANIC WEATHER SYSTEMS S.L.U/CNR/OCTAC_WORK/UPLOAD_CODE_NEW_VERSION/COMPARISON/NEW'
        dir_base1='/store/COP2-OC-TAC/OCTACMANAGER/UPLOAD_TEST_NEWVERSION/COMPARISON/OLD'
        dir_base2='/store/COP2-OC-TAC/OCTACMANAGER/UPLOAD_TEST_NEWVERSION/COMPARISON/NEW'
        ref = args.path if args.path else None
        for name in os.listdir(dir_base1):
            if ref is not None:
                if name.find(ref)<0:
                    continue
            if not name.endswith('.nc'):
                continue
            file1 = os.path.join(dir_base1,name)
            file2 = os.path.join(dir_base2,name)
            if os.path.exists(file1) and os.path.exists(file2):
                compare_files(file1,file2)

    if args.mode == 'REMOVE_NR_SOURCES':
        remove_nr_sources_impl(args.path, args.start_date, args.end_date, args.type_sources)

    if args.mode == 'UPDATE_TIME_CMEMS_DAILY':
        update_time_daily(args.path, args.start_date, args.end_date, args.preffix, args.suffix)
    if args.mode == 'UPDATE_TIME_CMEMS_MONTHLY':
        update_time_monthly(args.path, args.start_date, args.end_date, args.preffix, args.suffix)

    if args.mode == 'CREATE_MASK':
        create_mask(args.path, args.output_path, args.mask_variable, args.file_ref)

    if args.mode == 'APPLY_MASK':
        # args.file_ref; Mask file w
        start_date, end_date = get_dates()
        if start_date is None or end_date is None:
            return
        if not args.path:
            print(f'[ERROR] Input path (-p or --path) is required')
            return
        if not args.file_mask:
            print(f'[ERROR] File mask (-fmask or --file_mask) is required')
            return
        mask_variable = 'Land_Mask'
        if args.mask_variable:
            mask_variable = args.mask_variable
        if not args.input_file_name:
            print(f'[ERROR] Input file name (-ifile or --input_file_name) is required')
            return

        if not os.path.isdir(args.path):
            print(f'[ERROR] {args.path} does not exist')
            return
        if not os.path.exists(args.file_mask):
            print(f'[ERROR] {args.file_mask} does not exist')
            return
        apply_mask(args.path, args.input_file_name, args.file_mask, mask_variable, start_date, end_date)

    if args.mode == 'CREATE_MASK_CDF':
        mask_variable = 'Land_Mask'
        if args.mask_variable:
            mask_variable = args.mask_variable
        create_mask_cfc(args.path, mask_variable, args.file_ref)

    if args.mode == 'COPYAQUA':
        copy_aqua()
    if args.mode == 'CHECKFTPCONTENTS':
        check_ftp_contents()
    if args.mode == 'CHECKGRANULES':
        check_granules()
    if args.mode == 'CHECKSOURCES':
        check_sources()
    if args.mode == 'ZIPGRANULES':
        check_zip()
    if args.mode == 'LOG_HYPSTAR':
        do_log_hypstar()


def check_zip():
    # MED and BLK
    # source_dir = '/dst04-data1/OC/OLCI/sources_baseline_3.01/2023'
    # output_file = '/mnt/c/DATA_LUIS/TEMPORAL/zip_granules_blk.slurm'
    # file_new_granules = '/mnt/c/DATA_LUIS/TEMPORAL/2023/new_granules_blk_rr.csv'
    # BAL
    # download_dir = '/store/COP2-OC-TAC/OLCI_FTP_EUMETSAT/cdrftp.eumetsat.int/cdrftp/olci_l1l2_2023'
    # source_dir = '/store/COP2-OC-TAC/BAL_Evolutions/sources/2023'
    # file_new_granules = '/mnt/c/DATA_LUIS/TEMPORAL/2023/new_granules_bal.csv'
    # dir_sensor = 'OL_1_EFR'
    # output_file = '/mnt/c/DATA_LUIS/TEMPORAL/zip_granules_bal.slurm'

    # ARC
    is_arc = True
    download_dir = '/store/COP2-OC-TAC/OLCI_FTP_EUMETSAT/cdrftp.eumetsat.int/cdrftp/olci_l1l2_2023'
    source_dir = '/store/COP2-OC-TAC/arc/sources'
    file_new_granules = '/mnt/c/DATA_LUIS/TEMPORAL/2023/new_granules_arc.csv'
    dir_sensor = 'OL_2_WFR'
    output_file = '/mnt/c/DATA_LUIS/TEMPORAL/zip_granules_arc.slurm'

    sbatch_lines = [
        '#SBATCH --nodes=1',
        '#SBATCH --ntasks=1',
        '#SBATCH -p octac_rep',
        '#SBATCH --mail-type=BEGIN,END,FAIL',
        '#SBATCH --mail-user=luis.gonzalezvilas@artov.ismar.cnr.it',
        '',
        'source /home/gosuser/load_miniconda3.source',
        'conda activate op_proc_202211v1',
        'cd /store/COP2-OC-TAC/OLCI_FTP_EUMETSAT/eistools',
        ''
    ]

    fout = open(output_file, 'w')
    fout.write('#!/bin/bash')
    for line in sbatch_lines:
        fout.write('\n')
        fout.write(line)

    import pandas as pd
    dset = pd.read_csv(file_new_granules, sep=';')
    for index, row in dset.iterrows():

        jjj = str(row['Day'])
        granule = str(row['Granule'])
        dir_platform = granule[0:3]
        dir_input = os.path.join(download_dir, dir_platform, dir_sensor, '2023', jjj)
        file_input = os.path.join(dir_input, f'{granule}.zip')
        if is_arc:
            date_here = dt.strptime(f'2023{jjj}', '%Y%j')
            dir_output = os.path.join(source_dir, date_here.strftime('%Y%m%d'))
        else:
            dir_output = os.path.join(source_dir, jjj)
        file_output = os.path.join(dir_output, f'{granule}.zip')

        cmd = f'cd {dir_input} && zip -r {granule}.zip {granule} && mv {file_input} {file_output}'
        print(cmd)
        fout.write('\n')
        fout.write(cmd)

    fout.close()


def check_sources():
    dir_orig = '/store/COP2-OC-TAC/OLCI_FTP_EUMETSAT'
    # dir_orig = '/mnt/c/DATA_LUIS/TEMPORAL'

    # arc
    prename = 'ToRemove_'
    dir_sources = '/store/COP2-OC-TAC/arc/sources'
    check_sources_impl(dir_orig, dir_sources, 'arc', prename, '_OL_2_WFR_')

    # med and blk
    # dir_sources = '/dst04-data1/OC/OLCI/sources_baseline_3.01'
    # prename = 'ToRemove_'
    # check_sources_impl(dir_orig,dir_sources,'med',prename,'_OL_2_WFR_')
    # check_sources_impl(dir_orig, dir_sources, 'blk',prename,'_OL_2_WFR_')
    # prename = 'ToRemoveRR_'
    # check_sources_impl(dir_orig, dir_sources, 'med_rr',prename,'_OL_2_WRR_')
    # check_sources_impl(dir_orig, dir_sources, 'blk_rr',prename,'_OL_2_WRR_')
    # dir_sources = '/dst04-data1/OC/OLCI/trimmed_sources'
    # prename = 'ToRemoveTrim_'
    # check_sources_impl(dir_orig, dir_sources, 'med',prename,'_OL_2_WFR_')
    # check_sources_impl(dir_orig, dir_sources, 'blk',prename,'_OL_2_WFR_')
    # prename = 'ToRemoveTrimRR_'
    # check_sources_impl(dir_orig, dir_sources, 'med_rr',prename,'_OL_2_WRR_')
    # check_sources_impl(dir_orig, dir_sources, 'blk_rr',prename,'_OL_2_WRR_')


def check_sources_impl(dir_orig, dir_sources, region, prename, wce):
    file_orig = os.path.join(dir_orig, f'new_granules_{region}.csv')
    list_granules_dict = {}
    list_granules_date = []
    date_prev_str = None
    f1 = open(file_orig, 'r')
    for line in f1:
        jjj = line.split(';')[0].strip()
        if jjj == 'Day':  # FIRST LINE
            continue
        granule = line.split(';')[1].strip()
        date_here_str = f'2023{jjj}'
        if date_prev_str is None:
            list_granules_date = [granule]
            date_prev_str = date_here_str
        else:
            if date_here_str == date_prev_str:
                list_granules_date.append(granule)
            else:
                date_here = dt.strptime(date_prev_str, '%Y%j')
                if region == 'arc':
                    folder_date = os.path.join(dir_sources, date_here.strftime('%Y%m%d'))
                else:
                    folder_date = os.path.join(dir_sources, '2023', date_here.strftime('%j'))
                list_granules_dict[date_prev_str] = {
                    'list': list_granules_date,
                    'folder': folder_date
                }
                list_granules_date = [granule]
                date_prev_str = date_here_str
    date_here = dt.strptime(date_prev_str, '%Y%j')
    if region == 'arc':
        folder_date = os.path.join(dir_sources, date_here.strftime('%Y%m%d'))
    else:
        folder_date = os.path.join(dir_sources, '2023', date_here.strftime('%j'))
    list_granules_dict[date_prev_str] = {
        'list': list_granules_date,
        'folder': folder_date
    }
    f1.close()

    file_remove = os.path.join(dir_orig, f'{prename}{region}.sh')
    fw = open(file_remove, 'w')

    for date_ref in list_granules_dict:
        list = list_granules_dict[date_ref]['list']
        folder = list_granules_dict[date_ref]['folder']
        list_applied = [0] * len(list)
        for name in os.listdir(folder):
            ifind = name.find(wce)
            if not name.startswith('S3'):
                continue
            if ifind < 0:
                continue
            index_g = check_grunule_in_list(name, list)
            if index_g >= 0:
                list_applied[index_g] = 1
                fout = os.path.join(folder, name)
                fw.write('\n')
                fw.write(f'mv {fout} /store/COP2-OC-TAC/OLCI_FTP_EUMETSAT/2023')

        for i in range(len(list_applied)):
            if list_applied[i] == 0:
                print(f'[INFO] Not found: {date_ref} -> {list[i]}')

    fw.close()


def check_granules():
    ##WFR LIST
    file_new_granules = '/mnt/c/DATA_LUIS/TEMPORAL/2023/S3_WFR_Granules.csv'
    # #check_granules_region('arc',file_new_granules)
    # check_granules_region('med',file_new_granules)
    # check_granules_region('blk', file_new_granules)
    check_granules_region('arc', file_new_granules)

    ##WRR LIST
    # file_new_granules = '/mnt/c/DATA_LUIS/TEMPORAL/2023/S3_WRR_Granules.csv'
    # check_granules_region('med_rr', file_new_granules)
    # check_granules_region('blk_rr', file_new_granules)

    ##EFR LIST
    # file_new_granules = '/mnt/c/DATA_LUIS/TEMPORAL/2023/S3_EFR_Granules.csv'
    # check_granules_region('bal', file_new_granules)


def check_granules_region(region, file_new_granules):
    input_path = '/mnt/c/DATA_LUIS/TEMPORAL/2023'
    import pandas as pd

    sbatch_lines = [
        '#SBATCH --nodes=1',
        '#SBATCH --ntasks=1',
        '#SBATCH -p octac_rep',
        '#SBATCH --mail-type=BEGIN,END,FAIL',
        '#SBATCH --mail-user=luis.gonzalezvilas@artov.ismar.cnr.it',
        '',
        'source /home/gosuser/load_miniconda3.source',
        'conda activate op_proc_202211v1',
        'cd /store/COP2-OC-TAC/OLCI_FTP_EUMETSAT/eistools',
        ''
    ]

    output_file = f'{input_path}/new_granules_{region}.csv'
    f1 = open(output_file, 'w')
    f1.write('Day;Granule')

    output_ftp_files = f'{input_path}/ftp_granules_{region}.slurm'
    output_mv_files = f'{input_path}/mv_granules_{region}.slurm'
    output_ftp_folder = '/store/COP2-OC-TAC/OLCI_FTP_EUMETSAT'
    start = 'wget -m --user=cdr_ro --password=LNnh73tfAavaC3YmqXfzafVn  ftp://cdrftp.eumetsat.int'

    if region == 'bal':
        input_ftp_folders = {
            'S3A': '/cdrftp/olci_l1l2_2023/S3A/OL_1_EFR/',
            'S3B': '/cdrftp/olci_l1l2_2023/S3B/OL_1_EFR/',
        }
        source_folder = '/store/COP2-OC-TAC/BAL_Evolutions/sources/'
    if region == 'med' or region == 'blk' or region == 'arc':
        input_ftp_folders = {
            'S3A': '/cdrftp/olci_l1l2_2023/S3A/OL_2_WFR/',
            'S3B': '/cdrftp/olci_l1l2_2023/S3B/OL_2_WFR/',
        }
        if region == 'med' or region == 'blk':
            source_folder = '/dst04-data1/OC/OLCI/sources_baseline_3.01/'
        if region == 'arc':
            source_folder = '/store/COP2-OC-TAC/arc/sources'
    if region == 'med_rr' or region == 'blk_rr':
        input_ftp_folders = {
            'S3A': '/cdrftp/olci_l1l2_2023/S3A/OL_2_WRR/',
            'S3B': '/cdrftp/olci_l1l2_2023/S3B/OL_2_WRR/',
        }
        source_folder = '/dst04-data1/OC/OLCI/sources_baseline_3.01/'

    fftp = open(output_ftp_files, 'w')
    fmv = open(output_mv_files, 'w')
    fftp.write('#!/bin/bash')
    fmv.write('#!/bin/bash')
    for line in sbatch_lines:
        fftp.write('\n')
        fftp.write(line)
        fmv.write('\n')
        fmv.write(line)

    dset = pd.read_csv(file_new_granules, sep=';')
    date_here_prev = None
    list_granules = []
    for index, row in dset.iterrows():
        day = row['Day']
        granule = row['Granule']
        date_here_str = dt.strptime(f'2023{day}', '%Y%j').strftime('%Y%m%d')
        if date_here_prev is None or date_here_str != date_here_prev:
            flist = os.path.join(input_path, f'eum_filelist_{region}_{date_here_str}.txt')
            list_granules = []
            if os.path.exists(flist):
                fg = open(flist)
                for g in fg:
                    list_granules.append(g)
                fg.close()
            date_here_prev = date_here_str
        if len(list_granules) > 0:
            index_g = check_grunule_in_list(granule, list_granules)
            if index_g >= 0:
                f1.write('\n')
                f1.write(f'{day};{granule}')

                platform = granule.split('_')[0]
                input_ftp_folder = input_ftp_folders[platform]
                fftp.write('\n')
                str = f'{start}{input_ftp_folder}2023/{day}/{granule} -P {output_ftp_folder}'
                fftp.write(str)
                fmv.write('\n')
                str = f'mv {output_ftp_folder}/cdrftp.eumetsat.int{input_ftp_folder}2023/{day}/{granule} {source_folder}2023/{day}'
                fmv.write(str)
    f1.close()
    fftp.close()
    fmv.close()


def check_grunule_in_list(granule, list_granules):
    format = '%Y%m%dT%H%M%S'
    start_date_granule = dt.strptime(granule.split('_')[7], format)
    end_date_granule = dt.strptime(granule.split('_')[8], format)
    platform = granule.split('_')[0]
    for indexg in range(len(list_granules)):
        g = list_granules[indexg]
        if g.startswith(platform):
            start_date_g = dt.strptime(g.split('_')[7], format)
            end_date_g = dt.strptime(g.split('_')[8], format)
            overlap = compute_overlap(start_date_granule, end_date_granule, start_date_g, end_date_g)
            if overlap > 0.50:
                return indexg

    return -1


def compute_overlap(sd, ed, sdcheck, edcheck):
    overlap = 0
    total_s = (ed - sd).total_seconds()
    if edcheck < sd:
        return overlap
    if sdcheck > ed:
        return overlap
    if sdcheck <= sd <= edcheck:
        overlap = (edcheck - sd).total_seconds() / total_s
    if sdcheck >= sd and edcheck <= ed:
        overlap = (edcheck - sdcheck).total_seconds() / total_s
    if sdcheck < sd and edcheck > ed:
        overlap = 1
    if sdcheck <= ed <= edcheck:
        overlap = (ed - sdcheck).total_seconds() / total_s
    return overlap


def check_ftp_contents():
    from ftplib import FTP
    ftp_orig = FTP('cdrftp.eumetsat.int', 'cdr_ro', 'LNnh73tfAavaC3YmqXfzafVn')
    rpath = '/cdrftp/olci_l1l2_2023/S3B/OL_1_EFR/2023'
    output_path = '/mnt/c/DATA_LUIS/TEMPORAL/2023/S3B_EFR_Granules.csv'
    f1 = open(output_path, 'w')
    f1.write('Day;Granule')
    for jjj in range(108, 132):
        ftp_orig.cwd(rpath)
        jjjs = str(jjj)
        if jjjs in ftp_orig.nlst():
            print(f'[INFO] Day: {jjjs}')
            jjj_path = f'{rpath}/{jjjs}'

            ftp_orig.cwd(jjj_path)
            for name in ftp_orig.nlst():
                f1.write('\n')
                f1.write(f'{jjjs};{name}')
    f1.close()


def copy_aqua():
    start_date, end_date = get_dates()
    sinfo = SourceInfo('202207')
    sinfo.start_source('AQUA')
    for y in range(start_date.year, end_date.year + 1):
        monthIni = 1
        monthEnd = 12
        if y == start_date.year:
            monthIni = start_date.month
        if y == end_date.year:
            monthEnd = end_date.month
        for m in range(monthIni, monthEnd + 1):

            day_ini = 1
            day_fin = calendar.monthrange(y, m)[1]
            if m == start_date.month and y == start_date.year:
                day_ini = start_date.day
            if m == end_date.month and y == end_date.year:
                day_fin = end_date.day

            for d in range(day_ini, day_fin + 1):
                date_here = dt(y, m, d)
                if args.verbose:
                    print('----------------------------------------------')
                    print(f'[INFO] Copying files for date: {date_here}')
                copy_aqua_impl(sinfo, date_here, 'MED')
                copy_aqua_impl(sinfo, date_here, 'BS')


def copy_aqua_impl(sinfo, date_here, region):
    sinfo.get_last_session_id('NRT', region, date_here)
    proc_folder = sinfo.get_processing_folder()
    if args.verbose:
        print(f'[INFO]   Region: ', region)
        print(f'[INFO]   Session ID: ', sinfo.sessionid)
        print(f'[INFO]   Processing folder: ', proc_folder)
    flist = os.path.join(proc_folder, 'daily_L2_files.list')
    file_list = get_files_aqua_from_list(proc_folder, flist)
    if len(file_list) > 0:
        for f in file_list:
            name = f.split('/')[-1]
            year = date_here.strftime('%Y')
            jday = date_here.strftime('%j')
            fout = f'/store3/OC/MODISA/sources/{year}/{jday}/{name}'
            if not os.path.exists(fout):
                if args.verbose:
                    print(f'[INFO]   Copying {f} to {fout}')
                shutil.copy(f, fout)


def get_files_aqua_from_list(proc_folder, file_list):
    file1 = open(file_list, 'r')
    filelist = []
    for line in file1:
        line = line.strip()
        if line.startswith('AQUA_MODIS'):
            datehere = dt.strptime(line.split('.')[1], '%Y%m%dT%H%M%S')
            datehere = datehere.replace(second=0)
            datehereold = datehere.strftime('%Y%j%H%M%S')
            fname = f'A{datehereold}.L2_LAC_OC.nc'
            filea = os.path.join(proc_folder, fname)
            if os.path.exists(filea):
                filelist.append(filea)
        else:
            filea = os.path.join(proc_folder, line.strip())
            if os.path.exists(filea):
                filelist.append(filea)
    file1.close()
    return filelist


def get_dates():
    ##DATES SELECTION
    if not args.start_date and not args.end_date:
        print(f'[ERROR] Start date(-sd) is not given.')
        return [None] * 2
    start_date_p = args.start_date
    if args.end_date:
        end_date_p = args.end_date
    else:
        end_date_p = start_date_p
    start_date = get_date_from_param(start_date_p)
    end_date = get_date_from_param(end_date_p)
    if start_date is None:
        print(
            f'[ERROR] Start date {start_date_p} is not in the correct format. It should be YYYY-mm-dd or integer (relative days')
        return
    if end_date is None:
        print(
            f'[ERROR] End date {end_date_p} is not in the correct format. It should be YYYY-mm-dd or integer (relative days')
        return [None] * 2
    if start_date > end_date:
        print(f'[ERROR] End date should be greater or equal than start date')
        return [None] * 2
    if args.verbose:
        print(f'[INFO] Start date: {start_date} End date: {end_date}')

    return start_date, end_date


def get_date_from_param(dateparam):
    datefin = None
    try:
        ndays = int(dateparam)
        datefin = dt.now().replace(hour=12, minute=0, second=0, microsecond=0) + timedelta(days=ndays)
    except:
        try:
            datefin = dt.strptime(dateparam, '%Y-%m-%d')
        except:
            pass
    return datefin


def find_row_column_from_lat_lon(lat, lon, lat0, lon0):
    # % closest squared distance
    # % lat and lon are arrays of MxN
    # % lat0 and lon0 is the coordinates of one point
    if contain_location(lat, lon, lat0, lon0):
        if lat.ndim == 1 and lon.ndim == 1:
            r = np.argmin(np.abs(lat - lat0))
            c = np.argmin(np.abs(lon - lon0))
        else:
            dist_squared = (lat - lat0) ** 2 + (lon - lon0) ** 2
            r, c = np.unravel_index(np.argmin(dist_squared),
                                    lon.shape)  # index to the closest in the latitude and longitude arrays
    else:
        # print('Warning: Location not contained in the file!!!')
        r = np.nan
        c = np.nan
    return r, c


def contain_location(lat, lon, in_situ_lat, in_situ_lon):
    if lat.min() <= in_situ_lat <= lat.max() and lon.min() <= in_situ_lon <= lon.max():
        contain_flag = 1
    else:
        contain_flag = 0

    return contain_flag


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
