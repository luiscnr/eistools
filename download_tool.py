import argparse
import os

parser = argparse.ArgumentParser(description='Download tool')
parser.add_argument("-c", "--config_file", help="Configuration file")
parser.add_argument("-r", "--region", help="Mode", choices=["BAL", "MED", "BLK", "ARC"])
parser.add_argument("-s", "--sensor", help="Sensor", choices=["OLCIA", "OLCIB", "OLCI"])
parser.add_argument("-o", "--output", help="Output. EMAIL, SCREEN, output_file")
parser.add_argument("-od", "--output_directory",
                    help="Output directory for downloadind (only if make download is enabled")

parser.add_argument("-olcit", "--olci_timeliness", help="OLCI Timeliness. By default, it is set depending of the date",
                    choices=["NR", "NT"])
parser.add_argument("-olcir", "--olci_resolution", choices=["FR", "RR"], help="Resolution. (FR or RR). Default: FR")
parser.add_argument("-olcil", "--olci_level", choices=["L1B", "L2"], help="Level. (L1B or L2). Default: L2")
parser.add_argument("-download", "--make_download", help="Make download", action="store_true")

args = parser.parse_args()


def main():
    if args.config_file:
        file_config = args.config_file
    else:
        path_file = os.path.dirname(__file__)
        file_config = os.path.join(path_file, 'config_download_tool.ini')
    if not os.path.isfile(file_config):
        print(f'[ERROR] Config. file {file_config} does not exist or no is a valid file')
        return

    include_sensors = set_sensors()
    print(include_sensors)

    if check_olci(include_sensors):
        from eumdac_lois import EUMDAC_LOIS
        edac = EUMDAC_LOIS(True, file_config)

def check_olci(include_sensors):
    if include_sensors[0] or include_sensors[1]:
        return True
    else:
        return False

def set_sensors():
    # OLCI-A,OLCI-B
    include_sensors = [False,False]
    if args.sensor == 'OLCIA' or args.sensor == 'OLCI':
        include_sensors[0] = True
    if args.sensor == 'OLCIB' or args.sensor == 'OLCI':
        include_sensors[1] = True
    print('fsaf',include_sensors)
    return include_sensors


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
