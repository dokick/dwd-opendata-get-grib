"""
This module downloads the grib file from the opendata server and extracts the data

Flight levels (half levels) (m): 22000.000, 19401.852, 18013.409, 16906.264, 15958.169, 15118.009,
                                 14358.139, 13661.439, 13016.363, 12414.654, 11850.143, 11318.068,
                                 10814.653, 10336.841, 9882.112, 9448.359, 9033.796, 8636.893,
                                 8256.329, 7890.952, 7539.748, 7201.825, 6876.388, 6562.725,
                                 6260.200, 5968.239, 5686.321, 5413.976, 5150.773, 4896.323,
                                 4650.265, 4412.272, 4182.043, 3959.301, 3743.791, 3535.279,
                                 3333.549, 3138.402, 2949.656, 2767.143, 2590.708, 2420.213,
                                 2255.527, 2096.537, 1943.136, 1795.234, 1652.748, 1515.610,
                                 1383.761, 1257.155, 1135.760, 1019.556, 908.539, 802.721,
                                 702.132, 606.827, 516.885, 432.419, 353.586, 280.598,
                                 213.746, 153.438, 100.277, 55.212, 20.000, 0.000

Flight levels (full levels) (m): 20700.926, 18707.630, 17459.836, 16432.216, 15538.089, 14738.074,
                                 14009.789, 13338.901, 12715.508, 12132.398, 11584.105, 11066.360,
                                 10575.747, 10109.477, 9665.235, 9241.077, 8835.344, 8446.611,
                                 8073.640, 7715.350, 7370.787, 7039.106, 6719.557, 6411.462,
                                 6114.219, 5827.280, 5550.148, 5282.374, 5023.548, 4773.294,
                                 4531.269, 4297.157, 4070.672, 3851.546, 3639.535, 3434.414,
                                 3235.976, 3044.029, 2858.399, 2678.926, 2505.461, 2337.870,
                                 2176.032, 2019.836, 1869.185, 1723.991, 1584.179, 1449.686,
                                 1320.458, 1196.457, 1077.658, 964.048, 855.630, 752.427,
                                 654.479, 561.856, 474.652, 393.002, 317.092, 247.172,
                                 183.592, 126.857, 77.745, 37.606, 10.000

Pressure levels (hPa): 200, 250, 300, 400, 500, 600, 700, 850, 950, 975, 1000
"""

from itertools import product
import json
import os.path
from os import fsync
import subprocess
from time import localtime
import requests


def download_grib_file(url: str, dest_folder: str) -> None:
    """Downloads the grib file

    Args:
        url (str): url with the file at the ending
        dest_foler (str): dir where file should be saved
    """
    if not os.path.exists(dest_folder):
        raise FileNotFoundError(f"dir \"{dest_folder}\" doesn't exist; not automatically created")

    filename = url.split('/')[-1].replace(" ", "_")  # be careful with file names
    file_path = os.path.join(dest_folder, filename)

    req = requests.get(url, stream=True, timeout=30)
    if req.ok:
        print("saving to", os.path.abspath(file_path))
        with open(file_path, 'wb') as grib_file:
            for chunk in req.iter_content(chunk_size=1024 * 8):
                if chunk:
                    grib_file.write(chunk)
                    grib_file.flush()
                    # os.fsync(grib_file.fileno())
                    fsync(grib_file.fileno())
    else:  # HTTP status code 4XX/5XX
        print(f"Download failed: status code {req.status_code}\n{req.text}")


def provide_database(dest_folder: str, *, number_of_hours: int = 48) -> None:
    """Downloads a whole day from the OpenData DWD Server

    Args:
        dest_folder (str): destination of the directory
        number_of_hours (int, optional): number of desired hours. Defaults to 48.
    """
    url_to_icond2 = r"http://opendata.dwd.de/weather/nwp/icon-d2/grib"
    # models = ("icosahedral_model-level", "icosahedral_pressure_level",
            #   "regular-lat-lon_model-level", "regular-lat-lon_pressure-level")
    model  = "regular-lat-lon_model-level"
    time_stamps = ("00", "03", "06", "09", "12", "15", "18", "21")
    fields = ("u", "v", "w")

    year, month, day, *useless = localtime()
    today = f"{year}{month}{day}"
    number_of_flight_levels = 65

    file_begin = fr"icon-d2_germany_{model}_"
    bz2_file_end = r".grib2.bz2"
    grib_file_end = r".grib2"

    for time_stamp, field in product(time_stamps, fields):
        path_to_field_folder = os.path.join(dest_folder, time_stamp, field)
        for hour in range(number_of_hours + 1):
            for flight_level in range(number_of_flight_levels):
                file_begin_extended = fr"{file_begin}{today}{time_stamp}"
                bz2_file_end_extended = fr"{hour}_{flight_level+1}_{field}{bz2_file_end}"
                grib_file_end_extended = fr"{hour}_{flight_level+1}_{field}{grib_file_end}"
                if hour < 10:
                    bz2_file = fr"{file_begin_extended}_00{bz2_file_end_extended}"
                    grib_file = fr"{file_begin_extended}_00{grib_file_end_extended}"
                else:
                    bz2_file = fr"{file_begin_extended}_0{bz2_file_end_extended}"
                    grib_file = fr"{file_begin_extended}_0{grib_file_end_extended}"
                url_to_bz2_file = fr"{url_to_icond2}/{time_stamp}/{field}/{bz2_file}"
                path_to_file = os.path.join(path_to_field_folder, grib_file)
                download_grib_file(url_to_bz2_file, path_to_field_folder)
                extract_grib_file(os.path.join(path_to_field_folder, bz2_file))
                dump_grib_data(path_to_file)
        # delete_grib_files(path_to_field_folder)


def extract_grib_file(path_to_grib_file: str) -> None:
    """Grib data is downloaded in .bz2 files. Extracting is necessary

    Args:
        path_to_grib_file (str): path to grib file
    """
    if os.path.exists(path_to_grib_file):
        subprocess.run(["bzip2", "-d", path_to_grib_file], check=True)


def dump_grib_data(path_to_file: str) -> None:
    """Dump grib data with eccodes functions

    Args:
        path_to_file (str): path to file (with the file name at the end)
    """
    # file_name = os.path.basename(os.path.normpath(path_to_file))
    grib_stdout = subprocess.run(["grib_dump", "-j", path_to_file],
                                 capture_output=True, text=True, check=True)
    json_dict = json.loads(grib_stdout.stdout)
    json_dict = optimize_json(json_dict)

    path_to_json = f"{path_to_file[:-5]}.json"
    with open(path_to_json, "w", encoding="utf8") as json_file:
        json.dump(json_dict, json_file, indent=4)


def delete_grib_files(dest_folder: str) -> None:
    """Deletes left over grib files

    Args:
        dest_folder (str): folder with grib files
    """
    path_with_grib_ending = os.path.join(dest_folder, "*.grib2")
    subprocess.run(["rm", path_with_grib_ending], check=True, )


def optimize_json(json_dict: dict) -> dict:
    """Optimize json format from DWD server

    Args:
        json_dict (dict): raw json from the grib_dump output in dict format

    Returns:
        dict: better and more readable json format
    """
    json_obj = json_dict["messages"][0]
    amount_of_messages = len(json_obj)
    return {json_obj[i]["key"]: json_obj[i]["value"] for i in range(amount_of_messages)}


def main():
    """For testing and debugging purposes"""
    path_to_model = os.path.join(os.path.expanduser("~"),
                                 "Dokumente", "_TU", "Bachelor", "DWD", "icon-d2")
    path_to_model = os.path.join(r"/media/sf_icon-d2")
    number_of_hours = 2
    provide_database(path_to_model, number_of_hours=number_of_hours)


if __name__ == "__main__":
    main()
