import argparse, os
from datetime import datetime as dt
from datetime import timedelta

parser = argparse.ArgumentParser(description='Download tool')
parser.add_argument("-c", "--config_file", help="Configuration file")

parser.add_argument("-s", "--sensor", help="Sensor", choices=["OLCIA", "OLCIB", "OLCI", "CMEMS","CCI_4KM"], required=True)
parser.add_argument("-od", "--output_directory",
                    help="Output directory for downloading (only if make download is enabled)")
parser.add_argument("-ods", "--output_directory_structure", help="Output directory structure")

parser.add_argument("-sd", "--start_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-ed", "--end_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-date_list","--date_list_file",help="Date list file (format yyyy-mm-dd)")
parser.add_argument("-download", "--make_download", help="Make download", action="store_true")

# cmems options
parser.add_argument("-pname", "--product_name", help="Product name")
parser.add_argument("-dname", "--dataset_name", help="Dataset name")
parser.add_argument("-bname", "--bucket_name", help="Bucket name")
parser.add_argument("-tname", "--tag_name", help="Tag name")
parser.add_argument("-endpoint", "--endpoint_name", help="End point name")

##olci options
parser.add_argument("-r", "--region", help="Mode", choices=["BAL", "MED", "BLK", "ARC"])
parser.add_argument("-olcit", "--olci_timeliness", help="OLCI Timeliness. By default, it is set depending of the date",
                    choices=["NR", "NT"])
parser.add_argument("-olcir", "--olci_resolution", choices=["FR", "RR"], help="Resolution. (FR or RR). Default: FR")
parser.add_argument("-olcil", "--olci_level", choices=["L1B", "L2"], help="Level. (L1B or L2). Default: L2")

parser.add_argument("-ow", "--overwrite", help="Overwrite output file(s).", action="store_true")
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")

##cci options
##region -reg_cci --region_cci
parser.add_argument("-reg_cci", "--region_cci", help="Region", choices=["GLOBAL","BAL", "MED", "BLK", "ARC","MED+BLK"],default="GLOBAL")
##-var --var_group
parser.add_argument("-var_cci", "--var_cci_group", help="Variable group for cci, combining using _: all, rrs, chl, iop, kd, owt, obs, rrsfile, chlfile, iopfile, kdfile",default='all')
##-type_cci --type_cci_product
parser.add_argument("-type_cci", "--type_cci_product", help="CCI Type product",choices=["subset","all","rrs","chl","iop","kd"],default='subset')


args = parser.parse_args()


def main():
    path_file = os.path.dirname(__file__)
    file_credentials = os.path.join(path_file, 'config_download_tool.ini')
    if not os.path.isfile(file_credentials) and args.sensor.startswith('OLCI'):
        print(f'[ERROR] EUMETSAT Credential file {file_credentials} does not exist.')
        return

    ods = '%Y/%j'
    output_directory = None
    if args.make_download:
        output_directory = args.output_directory
        if not create_if_not_exists(output_directory):
            print(f'[ERROR] Output directory does not exist and could not be created')
            return

        if args.output_directory_structure:
            ods = args.output_directory_structure
            if ods.lower() == 'none': ods = None

    if args.sensor == 'CMEMS':
        cmems_options = get_cmems_options()
        if cmems_options is None:
            print(f'[ERROR] Error retrieving CMEMS options')
            return

        make_cmems_download(cmems_options, args.make_download, output_directory, ods)

    if args.sensor == 'CCI_4KM':
        print(f'[INFO] Started download for sensor CCI_4KM. Make download: {args.make_download}')
        cci_options = get_cci_options()
        if args.make_download:
            make_cci_download(cci_options,output_directory, ods)

    # if args.sensor.startswith('OLCI'):
    #     include_sensors = set_sensors()
    #     if check_olci(include_sensors):
    #         from eumdac_lois import EUMDAC_LOIS
    #         edac = EUMDAC_LOIS(True, file_config)



def make_cmems_download(cmems_options, make_download, output_directory, ods):
    from cmems_lois import CMEMS_LOIS
    clois = CMEMS_LOIS(args.verbose)
    clois.make_cmems_download(cmems_options,make_download,output_directory,ods,args.overwrite)

def make_cci_download(cci_options,output_directory,ods):
    from html_download import OC_CCI_V6_Download

    cci_download = OC_CCI_V6_Download(cci_options['type_product'])
    cci_download.make_bulk_download(cci_options,output_directory,ods,args.overwrite)


def create_if_not_exists(folder):
    if not os.path.exists(folder):
        try:
            os.mkdir(folder)
            os.chmod(folder, 0o775)
        except:
            return False

    return True

def get_cci_options():
    region = None if args.region_cci=="GLOBAL" else args.region_cci
    cci_options = {
        'region': region,
        'var_group': args.var_cci_group,
        'geo_limits': None,
        'var_list': None,
        'date_list': None,
        'start_date': None,
        'end_date':None,
        'type_product': args.type_cci_product
    }
    if args.date_list_file:
        date_list = []
        fr = open(args.date_list_file,'r')
        for line in fr:
            try:
                date_here = dt.strptime(line.strip(),'%Y-%m-%d')
                date_list.append(date_here)
            except:
                pass
        cci_options['date_list'] = date_list
        cci_options['start_date'] = None
        cci_options['end_date'] = None
    else:
        start_date, end_date = get_start_end_dates()
        cci_options['start_date'] = start_date
        cci_options['end_date'] = end_date
        work_date = start_date
        date_list = []
        while work_date <= end_date:
            date_list.append(work_date)
            work_date = work_date + timedelta(hours=24)
        cci_options['date_list'] = date_list

    return cci_options

def get_cmems_options():
    from product_info import ProductInfo
    from s3buckect import S3Bucket

    keyList = ['start_date', 'end_date', 'date_list','product', 'dataset', 'endpoint', 'bucket', 'tag']
    cmems_options = {key: None for key in keyList}
    if args.date_list_file:
        date_list = []
        fr = open(args.date_list_file,'r')
        for line in fr:
            try:
                date_here = dt.strptime(line.strip(),'%Y-%m-%d')
                date_list.append(date_here)
            except:
                pass
        cmems_options['date_list'] = date_list
        cmems_options['start_date'] = None
        cmems_options['end_date'] = None
    else:
        start_date, end_date = get_start_end_dates()
        cmems_options['start_date'] = start_date
        cmems_options['end_date'] = end_date
        work_date = start_date
        date_list = []
        while work_date <= end_date:
            date_list.append(work_date)
            work_date = work_date + timedelta(hours=24)
        cmems_options['date_list'] = date_list
    if args.config_file:
        print(f'[ERROR] --config_file (-c) is not implemented')
        return None
    else:
        if not args.product_name:
            print(f'product_name (-pname) is required')
            return None
        if not args.dataset_name:
            print(f'dataset_name (-dname) is required')
            return None
        cmems_options['product'] = args.product_name
        cmems_options['dataset'] = args.dataset_name
        cmems_options['endpoint'] = args.endpoint_name
        cmems_options['bucket'] = args.bucket_name
        cmems_options['tag'] = args.tag_name
        if cmems_options['bucket'] is None or cmems_options['tag'] is None:

            pinfo = ProductInfo()
            if pinfo.set_dataset_info(cmems_options['product'], cmems_options['dataset']):
                if cmems_options['bucket'] is None:
                    cmems_options['bucket'] = pinfo.dinfo['s3_bucket_name']
                if cmems_options['tag'] is None:
                    cmems_options['tag'] = pinfo.dinfo['remote_dataset_tag'][1:]
            else:
                print(
                    f'[WARNING] Info on {args.product_name}/{args.dataset_name} is not available. Please provide bucket (-bname) and tag (-tname).')
        if cmems_options['endpoint'] is None:
            cmems_options['endpoint'] = S3Bucket().S3_ENDPOINT

    valid_options = True
    for key in cmems_options:
        if cmems_options[key] is None:
            print(f'[ERROR] Option {key} is not defined')
            valid_options = False

    if valid_options:
        return cmems_options
    else:
        return None


def get_start_end_dates():
    from datetime import datetime as dt
    if args.config_file:
        return None, None
    else:
        if args.start_date:
            try:
                start_date = dt.strptime(args.start_date, '%Y-%m-%d')
            except:
                print(f'[ERROR] Start date {args.start_date} is not in the correct format')
                return None, None
        else:
            start_date = None
        if args.end_date:
            try:
                end_date = dt.strptime(args.end_date, '%Y-%m-%d')
            except:
                print(f'[ERROR] End date {args.end_date} is not in the correct format')
                return None, None
        else:
            end_date = start_date

    return start_date, end_date


# DEPRECATED
# def check_olci(include_sensors):
#     if include_sensors[0] or include_sensors[1]:
#         return True
#     else:
#         return False
#
# def set_sensors():
#     # OLCI-A,OLCI-B
#     include_sensors = [False,False]
#     if args.sensor == 'OLCIA' or args.sensor == 'OLCI':
#         include_sensors[0] = True
#     if args.sensor == 'OLCIB' or args.sensor == 'OLCI':
#         include_sensors[1] = True
#
#     return include_sensors


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
