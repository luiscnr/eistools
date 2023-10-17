import argparse,configparser
import os
parser = argparse.ArgumentParser(description='Single average of NetCDF files for given dates')
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-c", "--config_file",help="Config file", required=True)
args = parser.parse_args()

def main():
    print(f'[INFO] Started single average')
    fconfig = args.config_file
    if not os.path.isfile(fconfig):
        print(f'[ERROR] Config file {fconfig} is not a valid file or does not exist')
        return
    options = configparser.ConfigParser()
    options.read(args.config_file)
    section = 'SINGLE_AVERAGE'
    if not options.has_section(section):
        print(f'[ERROR] Config file {fconfig} must contain a section {section}')
        return

    if not options.has_option(section,'input_path'):
        print(f'[ERROR] Input path option is required in section {section} in file: {fconfig}')
        return

    input_path = options[section]['input_path']
    input_path_organization = '%Y/%j'
    file_name = 'DATE_c3s_obs-oc_glo_bgc-reflectance_my_l3-multi-4km_P1D.nc'
    file_name_date_format = '%Y%m%d'


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()