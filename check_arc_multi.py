from datetime import datetime as dt
from datetime import timedelta
import os


def main():
    ##CHECK LAST FILE IN SERVER FOR PRODUCT 107
    from s3buckect import S3Bucket
    from cmems_lois import CMEMS_LOIS
    clois = CMEMS_LOIS(True)
    cmems_options = {
        'product':'OCEANCOLOUR_GLO_BGC_L3_MY_009_107',
        'dataset':'c3s_obs-oc_glo_bgc-reflectance_my_l3-multi-4km_P1D',
        'endpoint':S3Bucket().S3_ENDPOINT,
        'bucket':'mdl-native-16',
        'tag':'202303'
    }
    last_date_server = clois.check_last_file(cmems_options).replace(hour=0,second=0,minute=0,microsecond=0)

    ##CHECK LAST FILE PROCESSED IN OUR SERVER
    dir_base = '/store/COP2-OC-TAC/arc/multi'
    #dir_base = '/mnt/c/DATA_LUIS/OCTACWORK'
    work_date = dt.now()
    min_date = dt(2000,1,1)
    last_date_local = None
    while work_date>=min_date:
        path_file = os.path.join(dir_base,work_date.strftime('%Y'),work_date.strftime('%j'),f'C{work_date.strftime("%Y%j")}_rrs-arc-4km.nc')
        if os.path.exists(path_file):
            last_date_local = work_date.replace(hour=0,minute=0,second=0,microsecond=0)
            break
        work_date = work_date - timedelta(hours=24)

    print(f'[INFO] Last Date Server: {last_date_server}')
    print(f'[INFO] Last Date Local: {last_date_local}')

    fout = os.path.join(dir_base, 'CheckDateMulti.mail')
    if last_date_server==last_date_local:
        print(f'[INFO] No more data are available for processing')
        if os.path.exists(fout):
            os.remove(fout)

    if last_date_server>=last_date_local:
        first_date_processing = last_date_local+timedelta(hours=24)
        print(f'[INFO] Data are avaialable for processing')
        fw = open(fout, 'w')
        fw.write('DATA ARE AVAILABLE FOR PROCESSING MULTI ARC')
        fw.write('\n')
        fw.write(f'Start date: {first_date_processing.strftime("%Y-%m-%d")}')
        fw.write('\n')
        fw.write(f'End date: {last_date_server.strftime("%Y-%m-%d")}')
        fw.write('\n')
        fw.write('\n')
        fw.write('Please launch the following script as gosuser to process the new data: ')
        fw.write('\n')
        fw.write('\n')
        fw.write('cd /store/COP2-OC-TAC/arc/code/scriptsmulti')
        fw.write('\n')
        fw.write(f'sh lancia_arc_op_multi.sh {first_date_processing.strftime("%Y-%m-%d")} {last_date_server.strftime("%Y-%m-%d")}')

        fw.close()



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()