from nasa_download import NASA_DOWNLOAD
from datetime import datetime as dt
import argparse

def main():

    try:
        date_here = dt.strptime(args.date,'%Y%m%d')
        sensor = args.sensor
        if args.Region=='BlackSea':
            region = 'BS'
        elif args.Region=='Mediterranean':
            region = 'MED'
        #date_here = dt(2022,9,30)
    except:
        print('[ERROR] Error parsing arguments')
        exit(1)

    ndownload = NASA_DOWNLOAD()
    ndownload.get_list_files(date_here,sensor,region,'DT')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='NASA Get Scenes. Patch to retrieve DT scenes')
    parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
    parser.add_argument("-sen", "--sensor", help="Specify sensor: VIIRS, VIIRSJ, AQUA", required=True)
    parser.add_argument("-d", "--date", help="Specify a date in yyyymmdd format", required=True)
    parser.add_argument("-R", "--Region", help="specify the Region Label of the area: BlackSea or Mediterranean",
                        required=False)
    args = parser.parse_args()

    main()