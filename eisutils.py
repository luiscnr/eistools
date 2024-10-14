import argparse
import shutil
from datetime import datetime as dt
from datetime import timedelta
from source_info import SourceInfo
import calendar
import os

parser = argparse.ArgumentParser(description='Check upload')
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-m", "--mode", help="Mode.", type=str, required=True,
                    choices=['COPYAQUA', 'CHECKFTPCONTENTS', 'CHECKGRANULES', 'CHECKSOURCES', 'ZIPGRANULES', 'LOG_HYPSTAR','TEST','UPDATE_TIME_CMEMS_DAILY','UPDATE_TIME_CMEMS_MONTHLY'])
parser.add_argument("-p", "--path", help="Path")
parser.add_argument("-sd", "--start_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-ed", "--end_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-pr", "--preffix", help="Preffix")
parser.add_argument("-sf", "--suffix", help="Suffix")
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
    from netCDF4 import Dataset
    import numpy as np
    from datetime import datetime as dt
    path_hypernets = '/mnt/c/DATA_LUIS/INSITU_HYPSTAR/VEIT/2023/09/11'
    file_out = '/mnt/c/DATA_LUIS/INSITU_HYPSTAR/data_hypstar_20230911.csv'
    f1 = open(file_out,'w')
    first_line = 'File;Date;Time;solar_azimuth_angle;solar_zenith_angle;viewing_azimuth_angle;viewing_zenith_angle;quality_flag'
    wavelenght = None
    for name in os.listdir(path_hypernets):
        print(name)
        file_hypernets = os.path.join(path_hypernets,name)
        dataset = Dataset(file_hypernets,'r')
        if wavelenght is None:
            wavelenght = np.array(dataset.variables['wavelength'])
            for val in wavelenght:
                val_rrs = f'rrs_{val}'
                first_line = f'{first_line};{val_rrs}'
            f1.write(first_line)

        datetime_here = dt.utcfromtimestamp(float(dataset.variables['acquisition_time'][0]))
        date_str = datetime_here.strftime('%Y-%m-%d')
        time_str = datetime_here.strftime('%H:%M')
        saa = dataset.variables['solar_azimuth_angle'][0]
        sza = dataset.variables['solar_zenith_angle'][0]
        vaa = dataset.variables['viewing_azimuth_angle'][0]
        vza = dataset.variables['viewing_zenith_angle'][0]
        qf = dataset.variables['quality_flag'][0]
        line = f'{name};{date_str};{time_str};{saa};{sza};{vaa};{vza};{qf}'
        rrs = np.array(dataset.variables['reflectance'][:])/np.pi

        for r in rrs:
            line = f'{line};{r[0]}'
        f1.write('\n')
        f1.write(line)


        dataset.close()
    f1.close()
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
    for index in range(0,29):
        line = 'Coarse'
        fmodelo = os.path.join(dir_modelos,f'Modelos_Best_Coarse_{index}.model')
        if not os.path.exists(fmodelo):
            continue
        dmodel = pd.read_csv(fmodelo,sep='=')
        gamma = dmodel.loc[1].iat[1]
        cost = dmodel.loc[2].iat[1]
        # dir_results = os.path.join(dir_res,f'Modelos_Best_Coarse_{index}')
        # fresults = os.path.join(dir_results,'Resultados.csv')
        # dres = pd.read_csv(fresults,sep=';')
        line = f'{line};{gamma};{cost}'
        # for iparam in params:
        #     val = dres.loc[iparam].at['DATOS_COMPLETO_TEST.csv']
        #     line = f'{line};{val}'
        print(index,line)
        f1.write('\n')
        f1.write(line)
    for index in range(0,14):
        line = 'Coarse'
        fmodelo = os.path.join(dir_modelos,f'ModelTrain_Best_Fine1_{index}.model')
        if not os.path.exists(fmodelo):
            continue
        dmodel = pd.read_csv(fmodelo,sep='=')
        gamma = dmodel.loc[1].iat[1]
        cost = dmodel.loc[2].iat[1]
        dir_results = os.path.join(dir_res,f'Modelos_Best_Coarse_{index}')
        fresults = os.path.join(dir_results,'Resultados.csv')
        dres = pd.read_csv(fresults,sep=';')
        line = f'{line};{gamma};{cost}'
        for iparam in params:
            val = dres.loc[iparam].at['DATOS_COMPLETO_TEST.csv']
            line = f'{line};{val}'
        print(index,line)
        f1.write('\n')
        f1.write(line)
    f1.close()
    return True
def do_check_tal():
    import pandas as pd
    dir_base = '/mnt/c/PERSONAL/ARTICULO_PSEUDONITZSCHIA/MODELOS_DATOS_FIN_PA_4/'
    file_out = '/mnt/c/PERSONAL/ARTICULO_PSEUDONITZSCHIA/Results_GridSearch_BNB.csv'
    f1 = open(file_out,'w')
    f1.write('GridSearch;Gamma;Cost;Sensitivity;Specificity;Precission;TSS;F1-Score;AUC')
    params = [5,6,14,11,19,20]
    for index in range(1,130):
        line = 'Fine'
        dir_model = os.path.join(dir_base,f'CVLOU_{index}')
        fbest = os.path.join(dir_model,f'CVLOU_{index}_Best.csv')
        dbest = pd.read_csv(fbest,sep=';')
        gamma = dbest.loc[0].at['Gamma']
        cost = dbest.loc[0].at['Cost']
        line = f'{line};{gamma};{cost}'
        fres = os.path.join(dir_model,'ResultadosCV.csv')
        dres = pd.read_csv(fres,sep=';')
        for iparam in params:
            val = dres.loc[iparam].at['OptTh']
            line = f'{line};{val}'
        print(index,line)
        f1.write('\n')
        f1.write(line)

    f1.close()
    return True

def do_log_hypstar():
    file_log = '/mnt/c/DATA_LUIS/ESA-POP_WORK/2023-11-sequence_12V-input.log'
    file_out = '/mnt/c/DATA_LUIS/ESA-POP_WORK/voltage_output_11.csv'
    fout = open(file_out,'w')
    fout.write('Date;TimeSequence;Time;Datetime;Voltage;Current;TotalEnergyConsumed')
    f1 = open(file_log,'r')

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
        #print(line)

        line_out = f'{date};{time_sequence};{time};{date}T{time};{voltage};{current};{total_energy_consumed}'
        fout.write('\n')
        fout.write(line_out)
        print(line_out)
        #print('-------------')
    f1.close()
    fout.close()

def do_empty_copy():
    input_file = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/MonthlyEmptyFiles/O2023001031-kd490_monthly-bal-fr.nc'
    output_file = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/MonthlyEmptyFiles/O2023335365-kd490_monthly-bal-fr.nc'
    from datetime import datetime as dt

    date_jan = dt(1981,1,1,0,0,0)+timedelta(seconds=1325376000)
    int_jan = (dt(2023,1,1,0,0,0)-dt(1981,1,1,0,0,0)).total_seconds()
    int_dec = (dt(2023,12,1,0,0,0)-dt(1981,1,1,0,0,0)).total_seconds()
    print(int_jan)
    print(int_dec)

    #start_date = '2023-12-01'
    #end_date = '2023-12-31'

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

        if name=='lat' or name=='lon':
            ncout[name][:] = input_dataset[name][:]
        if name=='time':
            ncout[name][:] = 1354233600

    ncout.close()
    input_dataset.close()


    return True


def resolve_CCOC_778():
    print('Resolving 778')
    #1.CHECKING SENSOR MASK
    # path = '/dst04-data1/OC/OLCI/daily_v202311_bc'
    # path = '/store/COP2-OC-TAC/BAL_Evolutions/BAL_REPROC'
    path = '/store/COP2-OC-TAC/BAL_Evolutions/POLYMERWHPC'
    from datetime import datetime as dt
    from netCDF4 import Dataset
    date_work = dt(2016,4,26)
    date_fin = dt(2023,12,31)
    file_out = '/store/COP2-OC-TAC/BAL_Evolutions/CCOC-778/list_files_daily_final_2016_2023.csv'
    f1 = open(file_out,'w')
    f1.write('Date;Status')
    while date_work<=date_fin:
        yyyy = date_work.strftime('%Y')
        jjj = date_work.strftime('%j')
        file_date = os.path.join(path,yyyy,jjj,f'O{yyyy}{jjj}-chl-bal-fr.nc')
        status = -1
        if os.path.exists(file_date):
            status = 0
            dataset = Dataset(file_date,'r')
            if 'SENSORMASK' in dataset.variables:
                status = 1
            dataset.close()
        date_work_f = date_work.strftime('%Y-%m-%d')
        line = f'{date_work_f};{status}'
        f1.write('\n')
        f1.write(line)

        date_work = date_work + timedelta(hours=24)
    f1.close()

    #2. Checking S3A and S3B
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

    #3. Copy available files s3a (s3b is missing), from 2016 to 2022
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

    #2. Adding SENSORMASK ONLY WITH S3A
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

def create_copy_with_sensor_mask(input_file,output_file):
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

        if name!='time' and name!='lat' and name!='lon' and name!='QI':
            data = np.array(input_dataset[name][:])
            data[data!=-999] = 1
            data[data==999] = np.nan
        ncout.createVariable(name, variable.datatype, variable.dimensions, fill_value=fill_value, zlib=True, complevel=6)

        # copy variable attributes all at once via dictionary
        ncout[name].setncatts(input_dataset[name].__dict__)

        # copy data
        ncout[name][:] = input_dataset[name][:]


    var = ncout.createVariable('SENSORMASK','i4',('time','lat','lon'),zlib=True,complevel=6)
    ncout['SENSORMASK'][:] = data
    var.long_name='Sensor Mask'
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
    pinfo.set_dataset_info('OCEANCOLOUR_MED_BGC_L3_NRT_009_141','cmems_obs-oc_med_bgc-plankton_nrt_l3-multi-1km_P1D')
    date_here = dt(2024,5,7)
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
    from s3buckect import S3Bucket
    pinfo = ProductInfo()
    pinfo.set_dataset_info('OCEANCOLOUR_MED_BGC_L3_NRT_009_141', 'cmems_obs-oc_med_bgc-plankton_nrt_l3-multi-1km_P1D')
    date_here = dt(2024, 5, 14)
    sb = S3Bucket()
    sb.update_params_from_pinfo(pinfo)
    conn = sb.star_client()
    s3bname, key, isuploaded = sb.check_daily_file('NRT', pinfo, date_here, False)
    path_out = '/mnt/c/DATA_LUIS/OCTACWORK'
    sb.download_daily_file('NRT',pinfo,date_here,path_out,True,True)
    return True

def resolve_CCOC_878(year):
    file_slurm = f'/mnt/c/DATA_LUIS/OCTAC_WORK/CCOC-878/resolve_ccoc_878_{year}.slurm'
    fw = open(file_slurm,'w')
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

    start_date = dt(year,1,1)
    end_date = dt(year,12,31)
    if year==1997:
        start_date = dt(year,9,16)
    if year==2024:
        start_date = dt(year, 9, 5)
        end_date = dt(year,9,10)

    base_folder = '/store3/OC/MULTI/daily_v202311_x'
    work_date =start_date
    while work_date<=end_date:
        yyyy = work_date.strftime('%Y')
        jjj = work_date.strftime('%j')
        file_med = os.path.join(base_folder,yyyy,jjj,f'X{yyyy}{jjj}-pft-med-hr.nc')
        file_bs = os.path.join(base_folder,yyyy,jjj,f'X{yyyy}{jjj}-pft-bs-hr.nc')
        fw = resolve_CCOC_878_impl(file_med,file_bs,fw)
        work_date = work_date + timedelta(hours=24)


    fw.close()

    return True

def resolve_CCOC_878_impl(file_med,file_bs,fw):

    med = {
        'DIATO':{
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
        'PROKAR':{
            'valid_min': 0.001,
            'valid_max': 1.0
        },
        'MICRO':{
            'valid_min': 0.0001,
            'valid_max': 4.0
        },
        'NANO':{
            'valid_min': 0.0001,
            'valid_max': 2.0
        },
        'PICO':{
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
    for year in range(1997,2025):
        work_date = dt(year,1,1)
        end_date = dt(year,12,31)
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
                nfound_med = nfound_med+1
                valid = True
                dataset_med = Dataset(file_med)
                for var in med:
                    if var in dataset_med.variables:
                        dif_min = abs(dataset_med.variables[var].valid_min - med[var]["valid_min"])
                        dif_max = abs(dataset_med.variables[var].valid_max - med[var]["valid_max"])
                        if dif_min>1e-8:
                            print(var,dataset_med.variables[var].valid_min,'<->',med[var]['valid_min'])
                            valid = False
                        if dif_max>1e-8:
                            print(var, dataset_med.variables[var].valid_max, '<->', med[var]['valid_max'])
                            valid = False
                if valid:
                    ngood_med = ngood_med + 1
                else:
                    print(f'[ERROR] Error in med file: {file_med}')
                dataset_med.close()

            if os.path.exists(file_bs):
                nfound_bs = nfound_bs+1
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
        file_orig = os.path.join(folder_orig,name)
        date_orig = dt.strptime(name.split('_')[0],'%Y%m%d')
        file_dest = os.path.join(folder_dest,date_orig.strftime('%Y'),date_orig.strftime('%j'),name)
        print(f'Moving {file_orig} -> {file_dest}')
        os.rename(file_orig,file_dest)

def kk():
    dir_base = '/mnt/c/DATA_LUIS/TARA_TEST/station_match-ups/extracts_chl_CMEMS_OLCI_BAL'
    for name in os.listdir(dir_base):
        line = f'ncrename -v satellite_chl,satellite_CHL {os.path.join(dir_base,name)}'
        print(line)
    return True

def update_time_impl(input_file,output_file,date_here,date_last):
    # print(f'Updating file: {input_file}')
    # output_file = input_file.replace('X2023','X2024')
    # print(output_file)
    from netCDF4 import Dataset
    from datetime import datetime as dt
    from datetime import timedelta
    date_ref = dt(1981,1,1,0,0,0)
    seconds_new = (date_here-date_ref).total_seconds()

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

        if name=='time':
            seconds_prev = input_dataset[name][0]
            seconds_prev = int(seconds_prev)
            date_prev = date_ref+timedelta(seconds=seconds_prev)
            date_new = date_ref+timedelta(seconds=seconds_new)
            print(f'[INFO ] Updating date from {date_prev} to {date_new}')
            ncout[name][:] = int(seconds_new)
        else:
            ncout[name][:] = input_dataset[name][:]
    ncout.start_date = date_here.strftime('%Y-%m-%d')
    ncout.stop_date = date_last.strftime('%Y-%m-%d')
    ncout.close()
    input_dataset.close()

def update_time_daily(path,start_date_str,end_date_str,preffix,suffix):
    from datetime import datetime as dt
    from datetime import timedelta
    if not os.path.isdir(path):
        print(f'[ERROR] {path} does not exist or it is not a directory')
        return
    try:
        start_date = dt.strptime(start_date_str,'%Y-%m-%d')
    except:
        print(f'[ERROR] Start date is not a valid date')
        return
    try:
        end_date = dt.strptime(end_date_str, '%Y-%m-%d')
    except:
        print(f'[ERROR] End date {end_date_str} is not a valid date')
        return
    if end_date<start_date:
        print(f'[ERROR] End date {end_date_str} should be greater or equal to {start_date_str}')
        return

    date_here = start_date
    while date_here<=end_date:
        print(f'[INFO] Working with date: {date_here}')
        yearstr = date_here.strftime('%Y')
        jjj = date_here.strftime('%j')
        path_date = os.path.join(path,yearstr,jjj)
        if not os.path.exists(path_date):
            continue
        for name in os.listdir(path_date):
            if not name.startswith(preffix):continue
            if not name.endswith(suffix):continue

            file_in = os.path.join(path_date,name)
            name_out = f'{preffix}{yearstr}{jjj}{suffix}'
            file_out = os.path.join(path_date,name_out)
            update_time_impl(file_in, file_out,date_here,date_here)
            # TEST
            # file_med = os.path.join(path,yearstr,jjj,f'X2023{jjj}-pp-med-lr.nc')
            #
            # file_blk = os.path.join(path,yearstr,jjj,f'X2023{jjj}-pp-bs-lr.nc')
            # if os.path.exists(file_med):
            #     update_time_impl(file_med,date_here)
            # if os.path.exists(file_blk):
            #     update_time_impl(file_blk, date_here)
        date_here = date_here + timedelta(hours=24)

def update_time_monthly(path,start_date_str,end_date_str,preffix,suffix):
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
            if not name.startswith(preffix):continue
            if not name.endswith(suffix):continue

            date_file_str = name[name.find(preffix)+len(preffix):name.find(suffix)]
            date_file = dt.strptime(date_file_str[:-3],'%Y%j')
            date_out_start = dt(date_here.year,date_file.month,1)
            date_out_end = dt(date_here.year,date_file.month,calendar.monthrange(date_here.year, date_file.month)[1])
            name_out = f'{preffix}{yearstr}{date_out_start.strftime("%j")}{date_out_end.strftime("%j")}{suffix}'
            file_in = os.path.join(path_date,name)
            file_out = os.path.join(path_date,name_out)
            update_time_impl(file_in,file_out,date_out_start,date_out_end)

        date_here = date_here + timedelta(days=365)

def check_sources_from_source_list_files(list_files,dir_out,fout):
    all_files = {}
    for file in list_files:
        f1 = open(file,'r')
        for line in f1:
            line_s = [x.strip() for x in line.split(';')]
            if line_s[0] not in all_files.keys():
                all_files[line_s[0]] = [line_s[1]]
            else:
                if line_s[1] not in all_files[line_s[0]]:
                    all_files[line_s[0]].append(line_s[1])
        f1.close()

    fw = open(fout,'w')
    fw.write('Date;NAll;NAvailable;Complete')
    for date in all_files:
        date_here = dt.strptime(date,'%Y-%m-%d')
        file_out_date = os.path.join(dir_out,date_here.strftime('%Y'),date_here.strftime('%j'))
        nall = len(all_files[date])
        ndate = 0
        for name_here  in all_files[date]:
            file_here = os.path.join(file_out_date,f'{name_here}.zip')
            print(file_here)
            if os.path.exists(file_here):
                ndate = ndate + 1
        complete = 1 if ndate==nall else 0
        line = f'{date};{nall};{ndate};{complete}'
        print(line)
        fw.write('\n')
        fw.write(line)
    fw.close()
def main():
    if args.mode=='TEST':
        file_base_list_files = '/store3/OC/OLCI_BAL/org_scripts'
        #file_base_list_files = '/mnt/c/DATA_LUIS/OCTACWORK'
        list_files = []
        for name in os.listdir(file_base_list_files):
            if name.startswith('GranulesToDownload2024'):
                list_files.append(os.path.join(file_base_list_files,name))

        check_sources_from_source_list_files(list_files,'/store3/OC/OLCI_BAL/SOURCES',os.path.join(file_base_list_files,'SOURCES_2024_CHECK.txt'))
        check_sources_from_source_list_files(list_files, '/store3/OC/OLCI_BAL/SOURCES_EXTRA',
                                             os.path.join(file_base_list_files, 'SOURCES_EXTRA_2024_CHECK.txt'))
        check_sources_from_source_list_files(list_files, '/store/COP2-OC-TAC/BAL_Evolutions/sources',
                                             os.path.join(file_base_list_files, 'BAL_EVOLUTION_sources_2024_CHECK.txt'))

        # for year in range(1998,2025):
        #     resolve_CCOC_878(year)
        # resolve_CCOC_878(2024)
        #check_CCOC_878()
        ##move_msi_sources()
        ##kk()

        #update_time(args.path,args.start_date,args.end_date)



        return
    # if check_download():
    #     return
    # if check_med():
    #     return
    # if resolve_CCOC_778():
    #     return
    # if do_empty_copy():
    #     return
    # if do_check():
    #     return
    if args.mode == 'UPDATE_TIME_CMEMS_DAILY':
        update_time_daily(args.path, args.start_date, args.end_date,args.preffix,args.suffix)
    if args.mode == 'UPDATE_TIME_CMEMS_MONTHLY':
        update_time_monthly(args.path,args.start_date,args.end_date,args.preffix,args.suffix)




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
    #MED and BLK
    # source_dir = '/dst04-data1/OC/OLCI/sources_baseline_3.01/2023'
    # output_file = '/mnt/c/DATA_LUIS/TEMPORAL/zip_granules_blk.slurm'
    # file_new_granules = '/mnt/c/DATA_LUIS/TEMPORAL/2023/new_granules_blk_rr.csv'
    #BAL
    # download_dir = '/store/COP2-OC-TAC/OLCI_FTP_EUMETSAT/cdrftp.eumetsat.int/cdrftp/olci_l1l2_2023'
    # source_dir = '/store/COP2-OC-TAC/BAL_Evolutions/sources/2023'
    # file_new_granules = '/mnt/c/DATA_LUIS/TEMPORAL/2023/new_granules_bal.csv'
    # dir_sensor = 'OL_1_EFR'
    # output_file = '/mnt/c/DATA_LUIS/TEMPORAL/zip_granules_bal.slurm'

    #ARC
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

    fout = open(output_file,'w')
    fout.write('#!/bin/bash')
    for line in sbatch_lines:
        fout.write('\n')
        fout.write(line)

    import pandas as pd
    dset = pd.read_csv(file_new_granules,sep=';')
    for index,row in dset.iterrows():

        jjj = str(row['Day'])
        granule = str(row['Granule'])
        dir_platform = granule[0:3]
        dir_input = os.path.join(download_dir,dir_platform,dir_sensor,'2023',jjj)
        file_input = os.path.join(dir_input,f'{granule}.zip')
        if is_arc:
            date_here = dt.strptime(f'2023{jjj}','%Y%j')
            dir_output = os.path.join(source_dir,date_here.strftime('%Y%m%d'))
        else:
            dir_output = os.path.join(source_dir,jjj)
        file_output = os.path.join(dir_output, f'{granule}.zip')

        cmd = f'cd {dir_input} && zip -r {granule}.zip {granule} && mv {file_input} {file_output}'
        print(cmd)
        fout.write('\n')
        fout.write(cmd)

    fout.close()


def check_sources():
    dir_orig = '/store/COP2-OC-TAC/OLCI_FTP_EUMETSAT'
    #dir_orig = '/mnt/c/DATA_LUIS/TEMPORAL'

    # arc
    prename = 'ToRemove_'
    dir_sources = '/store/COP2-OC-TAC/arc/sources'
    check_sources_impl(dir_orig, dir_sources, 'arc',prename,'_OL_2_WFR_')

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




def check_sources_impl(dir_orig, dir_sources, region, prename,wce):
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
        list_applied = [0]*len(list)
        for name in os.listdir(folder):
            ifind = name.find(wce)
            if not name.startswith('S3'):
                continue
            if ifind<0:
                continue
            index_g = check_grunule_in_list(name, list)
            if index_g>=0:
                list_applied[index_g] = 1
                fout = os.path.join(folder, name)
                fw.write('\n')
                fw.write(f'mv {fout} /store/COP2-OC-TAC/OLCI_FTP_EUMETSAT/2023')

        for i in range(len(list_applied)):
            if list_applied[i]==0:
                print(f'[INFO] Not found: {date_ref} -> {list[i]}')

    fw.close()


def check_granules():
    ##WFR LIST
    file_new_granules = '/mnt/c/DATA_LUIS/TEMPORAL/2023/S3_WFR_Granules.csv'
    # #check_granules_region('arc',file_new_granules)
    # check_granules_region('med',file_new_granules)
    # check_granules_region('blk', file_new_granules)
    check_granules_region('arc',file_new_granules)

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

    if region=='bal':
        input_ftp_folders = {
            'S3A': '/cdrftp/olci_l1l2_2023/S3A/OL_1_EFR/',
            'S3B': '/cdrftp/olci_l1l2_2023/S3B/OL_1_EFR/',
        }
        source_folder = '/store/COP2-OC-TAC/BAL_Evolutions/sources/'
    if region=='med' or region=='blk' or region=='arc':
        input_ftp_folders  = {
            'S3A': '/cdrftp/olci_l1l2_2023/S3A/OL_2_WFR/',
            'S3B': '/cdrftp/olci_l1l2_2023/S3B/OL_2_WFR/',
        }
        if region == 'med' or region == 'blk':
            source_folder = '/dst04-data1/OC/OLCI/sources_baseline_3.01/'
        if region == 'arc':
            source_folder = '/store/COP2-OC-TAC/arc/sources'
    if region=='med_rr' or region=='blk_rr':
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
            if index_g>=0:
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
        return
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
        return
    if start_date > end_date:
        print(f'[ERROR] End date should be greater or equal than start date')
        return
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


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
