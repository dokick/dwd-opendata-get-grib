"""
This module downloads the grib file from the opendata server and extracts the data
"""

from itertools import product
import os.path
from time import localtime
import requests



def download_grib_file(url: str, dest_folder: str):
    """Downloads the grib file

    Args:
        url (str): url with the file at the ending
        dest_foler (str): dir where file should be saved
    """
    if not os.path.exists(dest_folder):
        raise FileNotFoundError("Directory doesn't exist, script wont make on automatically")

    filename = url.split('/')[-1].replace(" ", "_")  # be careful with file names
    file_path = os.path.join(dest_folder, filename)

    #TODO: how does requests work
    req = requests.get(url, stream=True, timeout=30)
    if req.ok:
        print("saving to", os.path.abspath(file_path))
        with open(file_path, 'wb') as grib_file:
            for chunk in req.iter_content(chunk_size=1024 * 8):
                if chunk:
                    grib_file.write(chunk)
                    grib_file.flush()
                    os.fsync(grib_file.fileno())
    else:  # HTTP status code 4XX/5XX
        print(f"Download failed: status code {req.status_code}\n{req.text}")


def download_whole_day(dest_folder: str):
    """Downloads a whole day from the OpenData DWD Server

    Args:
        dest_folder (str): destination of the directory
    """
    url_to_icond2 = "http://opendata.dwd.de/weather/nwp/icon-d2/grib"
    models = ("icosahedral_model-level", "icosahedral_pressure_level",
              "regular-lat-lon_model-level", "regular-lat-lon_pressure-level")
    time_stamps = ("/00", "/03", "/06", "/09", "/12", "/15", "/18", "/21")
    fields = ("/u", "/v", "/w")

    year, month, day, *useless = localtime()
    # del useless
    today = f"{year}{month}{day}"

    bz2_file_begin = "/icon-d2_germany"
    bz2_file_end = ".grib2.bz2"

    for model, time_stamp, field in product(models, time_stamps, fields):
        path_to_dest_folder = fr"{dest_folder}{time_stamp}{field}"
        #TODO: wrong number, how iterate
        for i in range(5):
            for j in range(65):
                bz2_file = f"{bz2_file_begin}_{model}_{today}{time_stamp[1:]}_00{i}_{j}_{field[1:]}{bz2_file_end}"
                url_to_bz2_file = url_to_icond2.join((time_stamp, field, bz2_file))
                download_grib_file(url_to_bz2_file, path_to_dest_folder)


def extract_grib_data(grib_file: str):
    """Grib data is downloaded in .bz2 files. Extracting is necessary

    Args:
        grib_file (str): path to grib file
    """
    pass


def main():
    """For testing and debugging purposes"""
    path_to_model = os.path.join(os.path.expanduser("~"),
                                 "Dokumente", "_TU", "Bachelor", "DWD", "icond-d2")
    download_whole_day(path_to_model)


if __name__ == "__main__":
    main()
