"""This module downloads the grib file from the opendata server and extracts the data"""

import argparse
import asyncio
import bz2
import json
import os
import subprocess
from pathlib import Path
from time import localtime
from typing import TypeAlias, Union

# import eccodes
import httpx
import numpy as np
import pandas as pd

PathLike: TypeAlias = Union[str, bytes, os.PathLike, Path]

BZ2 = r".bz2"
FIELDS = "u", "v", "w"
GRIB2 = r".grib2"
JSON = r".json"
MODEL = r"regular-lat-lon_model-level"
ICOND2_URL = r"https://opendata.dwd.de/weather/nwp/icon-d2/grib"
GRIB_FIELDS = (
    "values",
    "dataDate",
    "dataTime",
    "numberOfDataPoints",
    "Ni",
    "Nj",
    "latitudeOfFirstGridPointInDegrees",
    "longitudeOfFirstGridPointInDegrees",
    "latitudeOfLastGridPointInDegrees",
    "longitudeOfLastGridPointInDegrees",
    "iDirectionIncrementInDegrees",
    "jDirectionIncrementInDegrees",
    "gridType",
    "parameterUnits",
    "parameterName",
    "shortNameECMF",
    "shortName",
    "nameECMF",
    "name",
    "cfNameLegacyECMF",
    "cfNameECMF",
    "cfName",
    "cfVarNameLegacyECMF",
    "cfVarNameECMF",
    "cfVarName",
    "numberOfValues",
    "packingType",
    "maximum",
    "minimum",
    "average",
    "numberOfMissing",
    "standardDeviation",
    "skewness",
    "kurtosis",
    "getNumberOfValues"
)


async def download_single_file(
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
        url: str,
        dest_folder: PathLike) -> None:
    """Downloads single file sing the given httpx client and semaphore to limit connections.

    :param httpx.AsyncClient client: httpx.ASyncClient
    :param asyncio.Semaphore semaphore: asyncio.Semaphore
    :param str url: url with the file at the ending
    :param PathLike dest_folder: dir where file should be saved
    :raises FileNotFoundError: if ``dest_folder`` doesn't exist
    """
    if not dest_folder.exists():
        raise FileNotFoundError(f"dir \"{dest_folder}\" doesn't exist; not automatically created")

    async with semaphore:
        try:
            async with client.stream("GET", url) as response:
                filename = url.split('/')[-1].replace(" ", "_")  # be careful with file names
                file_path = dest_folder / filename
                with open(file_path, "wb") as stream:
                    async for chunk in response.aiter_bytes():
                        stream.write(chunk)
                        # stream.flush()
                        # fsync(stream.fileno())
        except httpx.HTTPError as exc:
            print(f"HTTP error occurred: {exc}")


async def download_url_list(url_list: list[str], dest_folder: PathLike, *, limit: int = 10) -> None:
    """Download files from a list of URLs with a limit on the number of concurrent connections.

    :param list[str] url_list: list of urls
    :param PathLike dest_folder: dir of destination
    :param int limit: limit of parallel connections, defaults to 10
    """
    async with httpx.AsyncClient() as client:
        semaphore = asyncio.Semaphore(limit)
        tasks = [asyncio.ensure_future(download_single_file(client, semaphore, url, dest_folder)) for url in url_list]
        await asyncio.gather(*tasks)


def extract_grib_file(path_to_grib_file: PathLike) -> None:
    """Grib data is downloaded in .bz2 files. Extraction is necessary

    :param PathLike path_to_grib_file: path to grib file
    """
    with open(path_to_grib_file, mode="rb") as compressed_stream:
        decompressed_data = bz2.decompress(compressed_stream.read())
    with open(path_to_grib_file.with_suffix(""), mode="wb") as decompressed_stream:
        decompressed_stream.write(decompressed_data)


def get_grib_data(path_to_grib_file: PathLike) -> None:
    """Dump grib data with eccodes functions

    :param PathLike path_to_grib_file: path to grib file
    """
    # with eccodes.FileReader(path_to_grib_file) as reader:
    #     message = next(reader)
    #     grib_data = {field_name: message.get(field_name) for field_name in GRIB_FIELDS if field_name != "values"}
    #     grib_data["values"] = list(message.get("values"))
    grib_stdout = subprocess.run(
        ["grib_dump", "-j", str(path_to_grib_file)],
        capture_output=True,
        text=True,
        check=True
    )
    grib_data = optimize_json(json.loads(grib_stdout.stdout))
    with open(Path(path_to_grib_file).with_suffix(".json"), "w", encoding="utf-8") as json_file:
        json.dump(grib_data, json_file, indent=4)


def delete_files(dest_folder: PathLike, *, suffix: str = ".grib2") -> None:
    """Deletes left over grib files

    :param PathLike dest_folder: folder with grib files
    :param str suffix: suffix of files to delete, defaults to ``".grib2"``
    """
    for file in dest_folder.glob(suffix):
        file.unlink(missing_ok=True)


def json_to_csv(path_to_json: Path) -> None:
    """Overwrites json to csv

    :param Path path_to_json: path to json file
    """
    with open(path_to_json, "r", encoding="utf8") as json_file:
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
    df_idx = np.arange(start_latitude, end_latitude+step, step) / 100

    if start_longitude > end_longitude:
        df_cols_1 = np.arange(start_longitude, 36000, step)
        df_cols_2 = np.arange(0, end_longitude+step, step)
        df_cols = np.concatenate((df_cols_1, df_cols_2)) / 100
    else:
        df_cols = np.arange(start_longitude, end_longitude, step) / 100

    frame = pd.DataFrame(
        values.reshape(number_latitude_points, number_longitude_points),
        index=df_idx,
        columns=df_cols
    )

    with open(path_to_json, "w", encoding="utf8") as json_file:
        json.dump(new_json_dict, json_file, indent=4)

    with open(path_to_json.with_suffix(".csv"), "w", encoding="utf8") as csv_file:
        frame.to_csv(csv_file, sep=";")


def optimize_json(json_dict: dict) -> dict:
    """Optimizes json output from DWD server

    :param dict json_dict: raw json from grib_dump output in dict format
    :return dict: better and more readable json format
    """
    json_obj = json_dict["messages"][0]
    amount_of_messages = len(json_obj)
    return {json_obj[i]["key"]: json_obj[i]["value"] for i in range(amount_of_messages)}


def provide_database(
        dest_folder: Path,
        *,
        number_of_hours: int = 48,
        flight_levels: tuple[int, int] = (1, 67),
        latest: bool = False
) -> None:
    """Downloads the specified time from the OpenData DWD Server

    :param Path dest_folder: dir of the destination
    :param int number_of_hours: number of desired hours, defaults to 48
    :param tuple[int, int] flight_levels: desired flight levels, defaults to (1, 67)
    :param bool latest: include latest data or not
    """
    year, month, day, hour, *_ = localtime()
    latest_hour = hour - hour % 3
    if not latest:
        latest_hour -= 3  # -3 because the latest hour might not be uploaded
    time_stamp = f"{year}{month:02d}{day:02d}{latest_hour:02d}"

    for field in FIELDS:
        Path.mkdir(dest_folder / time_stamp / field, parents=True, exist_ok=True)

    file_begin = fr"icon-d2_germany_{MODEL}_{time_stamp}"

    urls = {field: [] for field in FIELDS}

    for field in FIELDS:
        path_to_field_folder = dest_folder / time_stamp / field
        for hour in range(number_of_hours + 1):
            for flight_level in range(*flight_levels):
                bz2_file = fr"{file_begin}_0{hour:02d}_{flight_level}_{field}{GRIB2}{BZ2}"
                url_to_bz2_file = fr"{ICOND2_URL}/{latest_hour:02d}/{field}/{bz2_file}"
                urls[field].append(url_to_bz2_file)
        asyncio.run(download_url_list(urls[field], path_to_field_folder))
    # return
    for field in FIELDS:
        path_to_field_folder = dest_folder / time_stamp / field
        for hour in range(number_of_hours + 1):
            for flight_level in range(*flight_levels):
                bz2_file = fr"{file_begin}_0{hour:02d}_{flight_level}_{field}{GRIB2}{BZ2}"
                grib_file = fr"{file_begin}_0{hour:02d}_{flight_level}_{field}{GRIB2}"
                print(grib_file)
                extract_grib_file(path_to_field_folder / bz2_file)
                get_grib_data(path_to_field_folder / grib_file)
                json_file = fr"{file_begin}_0{hour:02d}_{flight_level}_{field}{JSON}"
                json_to_csv(path_to_field_folder / json_file)
        delete_files(path_to_field_folder)


def main() -> None:
    """Entry point for script"""
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output", required=True, help="Output directory of data")
    parser.add_argument("-n", "--hours", default=1, type=int, help="Number of hours that will be downloaded")
    parser.add_argument(
        "--level",
        nargs=2,
        default=(38, 66),
        help="Range of levels to include, left-side including, right-side excluding, refer to README to flight levels"
    )
    parser.add_argument(
        "--latest",
        action="store_true",
        default=False,
        help="Latest hour or 3 hours before that"
    )
    args = parser.parse_args()

    path_to_model = Path(args.output).resolve()
    number_of_hours = args.hours
    flight_levels = tuple(args.level)  # opendata.dwd.de uploads full levels, thus lowest flight level is 65

    if number_of_hours > 48:
        raise ValueError(f"Number of hours given exceeds 48: {number_of_hours}")
    if number_of_hours < 0:
        raise ValueError(f"Number of hours must be positive: {number_of_hours}")
    if flight_levels[0] > flight_levels[1]:
        raise ValueError(f"Starting flight level can't be greater than ending flight level: {flight_levels}")
    if flight_levels[0] > 66 or flight_levels[1] > 66:
        raise ValueError(f"Given flight levels exceed 66, but only 65 available: {flight_levels}")

    provide_database(
        path_to_model,
        number_of_hours=number_of_hours,
        flight_levels=flight_levels
    )


if __name__ == "__main__":
    main()
