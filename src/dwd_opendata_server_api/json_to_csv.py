"""
Convert json to csv
"""


import json
import os.path
import sys
from itertools import product
from time import localtime

import numpy as np
import pandas as pd


def json_to_csv(path_to_json_file: str) -> None:
    """Overwrites json to csv

    Args:
        path_to_json_file (str): path to json file
    """
    with open(path_to_json_file, "r", encoding="utf8") as json_file:
        json_dict = json.load(json_file)
        new_json_dict = {key: val for key, val in json_dict.items() if key != "values"}
        values = np.array(json_dict["values"])
    number_longitude_points = new_json_dict["Ni"]
    number_latitude_points = new_json_dict["Nj"]
    start_longitude = new_json_dict["longitudeOfFirstGridPointInDegrees"] * 100
    end_longitude = new_json_dict["longitudeOfLastGridPointInDegrees"] * 100
    start_latitude = new_json_dict["latitudeOfFirstGridPointInDegrees"] * 100
    end_latitude = new_json_dict["latitudeOfLastGridPointInDegrees"] * 100
    step = new_json_dict["iDirectionIncrementInDegrees"] * 100
    df_idx = np.arange(start_latitude, end_latitude+step, step)
    df_idx = df_idx / 100

    if start_longitude > end_longitude:
        df_cols_1 = np.arange(start_longitude, 36000, step)
        df_cols_2 = np.arange(0, end_longitude+step, step)
        df_cols = np.concatenate((df_cols_1, df_cols_2))
    else:
        df_cols = np.arange(start_longitude, end_longitude, step)
    df_cols = df_cols / 100
    df = pd.DataFrame(values.reshape(number_latitude_points, number_longitude_points),
                      index=df_idx, columns=df_cols)

    with open(path_to_json_file, "w", encoding="utf8") as json_file:
        json.dump(new_json_dict, json_file, indent=4)

    with open(fr"{path_to_json_file[:-5]}.csv", "w", encoding="utf8") as csv_file:
        df.to_csv(csv_file, sep=";")


def convert_to_csv(dest_folder: str, *, number_of_hours: int = 48,
                                        number_of_flight_levels: int = 65) -> None:
    """Convert json into csv

    Args:
        dest_folder (str): destination of directory
        number_of_hours (int, optional): number of desired hours. Defaults to 48.
        number_of_flight_levels (int, optional): number of desired flight levels. Defaults to 65.
    """
    model = "regular-lat-lon_model-level"
    time_stamps = ("00", "03", "06", "09", "12", "15", "18", "21")
    fields = ("u", "v", "w")

    year, month, day, *useless = localtime()
    month = 11
    day = 25
    today = f"{year}{month}{day}"

    json_file_begin = fr"icon-d2_germany_{model}_"
    json_file_end = r".json"

    for time_stamp, field in product(time_stamps, fields):
        path_to_field_folder = os.path.join(dest_folder, time_stamp, field)
        file_begin_extended = fr"{json_file_begin}{today}{time_stamp}"
        for hour in range(number_of_hours + 1):
            for flight_level in range(1, number_of_flight_levels+1):
                json_file_end_extended = fr"{hour}_{flight_level}_{field}{json_file_end}"
                if hour < 10:
                    json_file = fr"{file_begin_extended}_00{json_file_end_extended}"
                else:
                    json_file = fr"{file_begin_extended}_0{json_file_end_extended}"
                path_to_file = os.path.join(path_to_field_folder, json_file)
                print(path_to_file)
                if time_stamp == "00" and field == "u" and hour == 0 and flight_level in {1, 2}:
                    continue
                json_to_csv(path_to_file)


def main():
    """For testing and debugging purposes"""
    path_to_model = r"/media/sf_icon-d2"
    convert_to_csv(path_to_model, number_of_hours=2, number_of_flight_levels=29)


if __name__ == "__main__":
    main()
