import argparse,os
import numpy as np
import pandas as pd
from datetime import datetime as dt

parser = argparse.ArgumentParser(description='Identifity PDU from CSVr files')
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-m", "--mode", help="Mode.", type=str, required=True,choices=['OLCI'])
parser.add_argument("-i", "--input_path", help="Input csv path with: OLCI: Sat_Time (as YYYY-mm-dd HH:MM), Platform and Site",required=True)
parser.add_argument("-e", "--folder_extracts",help="Extracts folders",required=True)
args = parser.parse_args()


def make_olci():
    print('[STARTED]')
    input_file = args.input_path
    extract_folder = args.folder_extracts
    if not os.path.isfile(input_file):
        print(f'[ERROR] Input path {input_file} does not exist or it is not a valid file')
        return
    if not os.path.isdir(extract_folder):
        print(f'[ERROR] Extract folder {extract_folder} does not exist or it is not a valid directory')
        return

    try:
        df = pd.read_csv(input_file,sep=';')
    except:
        print(f'[ERROR] {input_file} is not a valid csv file')
        return

    col_names =  ['Sat_Time','Platform','Site']
    for col_name in col_names:
        if not col_name in df.columns:
            print(f'[ERROR] Column {col_name} is not avaialable in the CSV file')
            return

    sat_times = df['Sat_Time']
    platforms = df['Platform']
    sites = df['Site']
    sites_u = np.unique(sites)
    extract_list = {site:{} for site in sites_u}
    print(f'[INFO] Checking extract paths...')
    for site in sites_u:
        path_site = os.path.join(extract_folder,site)
        if not os.path.isdir(path_site):
            print(f'[WARNING] Path for site {site} is not avaiable. Skipping...')
            continue
        for name in os.listdir(path_site):
            if name.startswith('S3A_OL_2_WFR') or name.startswith('S3B_OL_2_WFR'):
                date_str = name.split('_')[7]
                date_here = dt.strptime(date_str,'%Y%m%dT%H%M%S')
                date_ref = date_here.strftime('%Y-%m-%d %H:%M')
                ref = f'{name[0:3]}_{date_ref}'
                extract_list[site][ref] = name[0:99]

    print(f'[INFO] Assigning PDU...')
    ndata = len(sat_times)
    pdu = [None]*ndata
    for idx in range(ndata):
        sat_time = sat_times[idx]
        site = sites[idx]
        platform = platforms[idx]
        ref = f'S3{platform}_{sat_time}'
        if ref in extract_list[site].keys():
            pdu[idx] = extract_list[site][ref]
            print(f'[INFO] -> {site}_{ref}->{pdu[idx]}')

    df['PRODUCT'] = pdu
    df.to_csv(input_file,sep=';')
    print(f'[INFO] Completed')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    if args.mode=='OLCI':
        make_olci()