"""This module downloads the grib file from the opendata server and extracts the data"""

import asyncio
import bz2
import json
import subprocess  # only for grib_dump solution until bundling with pyinstaller works
import sys
from argparse import ArgumentParser
from pathlib import Path
from time import localtime
from typing import Dict, Iterable, List, Mapping, Sequence, Tuple, Union

import httpx
import numpy as np
import pandas as pd
# from eccodes import codes_dump, codes_grib_new_from_file
# from gribapi import codes_dump, codes_new_from_file

BZ2_SUFFIX = r".bz2"
FIELDS = "u", "v", "w"
GRIB2_SUFFIX = r".grib2"
JSON_SUFFIX = r".json"
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
INDEX_47_DEG_LAT = 191
INDEX_54P98_DEG_LAT = 591
INDEX_5_DEG_LON = 447
INDEX_14P98_DEG_LON = 947


async def download_single_file(
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
        url: str,
        dest_folder: Path
) -> None:
    """Downloads single file sing the given httpx client and semaphore to limit connections.

    :param httpx.AsyncClient client: httpx.ASyncClient
    :param asyncio.Semaphore semaphore: asyncio.Semaphore
    :param str url: url with the file at the ending
    :param Path dest_folder: dir where file should be saved
    :raises FileNotFoundError: if ``dest_folder`` doesn't exist
    """
    if not dest_folder.exists():
        raise FileNotFoundError(f"dir \"{str(dest_folder)}\" doesn't exist; not automatically created")

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


async def download_url_list(url_list: Iterable[str], dest_folder: Path, *, limit: int = 10) -> None:
    """Download files from a list of URLs with a limit on the number of concurrent connections.

    :param Iterable[str] url_list: list of urls
    :param Path dest_folder: dir of destination
    :param int limit: limit of parallel connections, defaults to ``10``
    """
    async with httpx.AsyncClient() as client:
        semaphore = asyncio.Semaphore(limit)
        tasks = [asyncio.ensure_future(
            download_single_file(client, semaphore, url, dest_folder)
        ) for url in url_list]
        await asyncio.gather(*tasks)


def extract_grib_file(path_to_grib_file: Path) -> None:
    """Grib data is downloaded in .bz2 files. Extraction is necessary

    :param Path path_to_grib_file: path to grib file
    """
    with open(path_to_grib_file, mode="rb") as compressed_stream:
        decompressed_data = bz2.decompress(compressed_stream.read())
    with open(path_to_grib_file.with_suffix(""), mode="wb") as decompressed_stream:
        decompressed_stream.write(decompressed_data)


def get_grib_data(path_to_grib_file: Path) -> None:
    """Dump grib data with ecCodes functions

    :param Path path_to_grib_file: path to grib file
    """
    try:
        # with open(path_to_grib_file, "rb") as grib_stream:
            # grib_id = codes_new_from_file(grib_stream, 1)  # 1 stands for CODES_PRODUCT_GRIB
            # grib_id = codes_grib_new_from_file(grib_stream)
        grib_stdout = subprocess.run(
            ["grib_dump", "-j", str(path_to_grib_file)],
            capture_output=True,
            text=True,
            check=True
        )
    except FileNotFoundError as exc:
        print(exc.args)
        print(f"Missing file: {exc.filename}")
        sys.exit(
            "ecCodes is probably not installed. ecCodes isn't available on PyPI."
            " Refer here for installation: https://confluence.ecmwf.int/display/ECC/ecCodes+Installation"
        )
    grib_data = optimize_json(json.loads(grib_stdout.stdout)["messages"][0])
    with open(path_to_grib_file.with_suffix(".json"), "w", encoding="utf-8") as json_stream:
        # codes_dump(grib_id, json_stream, mode="json")
        json.dump(grib_data, json_stream, indent=4)


def delete_files(dest_folder: Path, *, suffix: str = ".grib2") -> None:
    """Deletes left over grib files

    :param Path dest_folder: folder with grib files
    :param str suffix: suffix of files to delete, defaults to ``.grib2``
    """
    for file in dest_folder.glob(suffix):
        file.unlink(missing_ok=True)


def json_to_csv(path_to_json: Path) -> np.ndarray:
    """Overwrites json to csv

    :param Path path_to_json: path to json file
    """
    with open(path_to_json, "r", encoding="utf-8") as json_stream:
        # json_key_val_seq: Sequence[Dict[str, Union[int, float, str]]] = json.load(json_stream)
        json_key_val_seq: Dict[str, Union[int, float, str]] = json.load(json_stream)
        new_json_dict = {}
        # for key_val_pair in json_key_val_seq:
        #     key = key_val_pair["key"]
        #     val = key_val_pair["value"]
        #     if key == "values":
        #         _values = val
        #     else:
        #         new_json_dict[key] = val
        for key, val in json_key_val_seq.items():
            if key == "values":
                _values = val
            else:
                new_json_dict[key] = val
        values = np.array(_values, dtype=np.float64)

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

    data_matrix = values.reshape(number_latitude_points, number_longitude_points)

    frame = pd.DataFrame(data_matrix, index=df_idx, columns=df_cols)

    with open(path_to_json, "w", encoding="utf-8") as json_stream:
        json.dump(new_json_dict, json_stream, indent=4)

    with open(path_to_json.with_suffix(".csv"), "w", encoding="utf-8") as csv_stream:
        frame.to_csv(csv_stream, sep=";", lineterminator="\n")

    only_germany = data_matrix[INDEX_47_DEG_LAT:INDEX_54P98_DEG_LAT, INDEX_5_DEG_LON:INDEX_14P98_DEG_LON]
    return only_germany
    # with open(path_to_json.with_suffix(".bin"), "wb") as binary_stream:
    #     for column in only_germany.T:
    #         for val in column:
    #             binary_stream.write(bytes(val))


def optimize_json(
        json_key_val_seq: Sequence[Mapping[str, Union[int, float, str]]]
) -> Dict[str, Union[int, float, str]]:
    """Optimizes json output from DWD server

    :param Sequence[Mapping[str, Union[int, float, str]]] json_key_val_seq: raw json from codes_dump output
    :return Dict[str, Union[int, float, str]]: better and more readable json format
    """
    amount_of_messages = len(json_key_val_seq)
    return {json_key_val_seq[i]["key"]: json_key_val_seq[i]["value"] for i in range(amount_of_messages)}
    # return {json_obj["key"]: json_obj["value"] for json_obj in json_key_val_seq}


def create_binary_file_over_all_flight_levels(data_matrices: Iterable[np.ndarray], path_to_json: Path) -> None:
    """Creates .bin files over all flight levels so reading into MATLab is easier.

    :param Iterable[np.ndarray] data_matrices: data matrices of germany
    :param Path path_to_json: path to json file without flight level specification
    """
    with open(path_to_json.with_suffix(".bin"), "wb") as binary_stream:
        for only_germany_height in data_matrices:
            for column in only_germany_height.T:
                for val in column:
                    binary_stream.write(bytes(val))


def get_wind_data(
        dest_folder: Path,
        *,
        range_of_hours: Tuple[int, int] = (0, 48),
        flight_levels: Tuple[int, int] = (1, 65),
        latest: bool = False
) -> None:
    """Downloads the specified time from the OpenData DWD Server

    :param Path dest_folder: dir of the destination
    :param Tuple[int, int] range_of_hours: range of desired hours, defaults to ``(0, 48)``
    :param Tuple[int, int] flight_levels: desired flight levels, defaults to ``(1, 65)``
    :param bool latest: include latest data or not
    """
    year, month, day, local_hour, *_ = localtime()
    latest_hour = local_hour - local_hour % 3
    if not latest:
        latest_hour -= 3  # -3 because the latest hour might not be uploaded
    if latest_hour < 0:
        day -= 1
        latest_hour = 24 + latest_hour
    time_stamp = f"{year}{month:02d}{day:02d}{latest_hour:02d}"

    for field in FIELDS:
        Path.mkdir(dest_folder / time_stamp / field, parents=True, exist_ok=True)

    file_begin = fr"icon-d2_germany_{MODEL}_{time_stamp}"

    urls: Dict[str, List[str]] = {field: [] for field in FIELDS}

    hour_start, hour_stop = range_of_hours
    flight_level_start, flight_level_stop = flight_levels
    for field in FIELDS:
        path_to_field_folder = dest_folder / time_stamp / field
        for hour in range(hour_start, hour_stop + 1):
            for flight_level in range(flight_level_start, flight_level_stop + 1):
                bz2_file: str = fr"{file_begin}_0{hour:02d}_{flight_level}_{field}{GRIB2_SUFFIX}{BZ2_SUFFIX}"
                url_to_bz2_file = fr"{ICOND2_URL}/{latest_hour:02d}/{field}/{bz2_file}"
                urls[field].append(url_to_bz2_file)
        asyncio.run(download_url_list(urls[field], path_to_field_folder))
    print("Download of data files finished")

    for field in FIELDS:
        path_to_field_folder = dest_folder / time_stamp / field
        for hour in range(hour_start, hour_stop + 1):
            data_matrices: List[np.ndarray] = []
            for flight_level in range(flight_level_start, flight_level_stop + 1):
                grib_file: str = fr"{file_begin}_0{hour:02d}_{flight_level}_{field}{GRIB2_SUFFIX}"
                bz2_file = fr"{grib_file}{BZ2_SUFFIX}"
                extract_grib_file(path_to_field_folder / bz2_file)
                print(f"{grib_file} decompressed")
                get_grib_data(path_to_field_folder / grib_file)
                print(f"{grib_file} data extracted")
                json_file: str = fr"{file_begin}_0{hour:02d}_{flight_level}_{field}{JSON_SUFFIX}"
                data_matrices.append(json_to_csv(path_to_field_folder / json_file))
                print(f"{grib_file} CSV created")
            create_binary_file_over_all_flight_levels(
                data_matrices,
                path_to_field_folder / fr"{file_begin}_0{hour:02d}_{field}{JSON_SUFFIX}"
            )
        delete_files(path_to_field_folder)


def main() -> None:
    """Entry point for script"""
    parser = ArgumentParser()
    parser.add_argument("-o", "--output", required=True, help="Output directory of data")
    parser.add_argument(
        "-n",
        "--hours",
        nargs=2,
        default=(0, 0),
        help="Range of hours that will be downloaded, including right side"
    )
    parser.add_argument(
        "-l",
        "--level",
        nargs=2,
        default=(38, 65),
        help="Range of levels to include, left-side and right-side including, refer to README to flight levels"
    )
    parser.add_argument(
        "--latest",
        action="store_true",
        default=False,
        help="Latest hour or 3 hours before that"
    )
    args = parser.parse_args()

    path_to_model = Path(args.output).resolve()
    range_of_hours = int(args.hours[0]), int(args.hours[1])
    flight_levels = int(args.level[0]), int(args.level[1])
    # opendata.dwd.de uploads full levels, thus lowest flight level is 65

    if range_of_hours[1] < range_of_hours[0]:
        raise ValueError(f"Range of hours must be in order: {range_of_hours}")
    if range_of_hours[1] > 48:
        raise ValueError(f"Range of hours given exceeds 48: {range_of_hours[1]}")
    if range_of_hours[0] < 0:
        raise ValueError(f"Range of hours must be positive: {range_of_hours[0]}")
    if flight_levels[0] > flight_levels[1]:
        raise ValueError(f"Starting flight level can't be greater than ending flight level: {flight_levels}")
    if flight_levels[0] > 65 or flight_levels[1] > 65:
        raise ValueError(f"Given flight levels exceed 65, but only 1-65 are available: {flight_levels}")

    get_wind_data(path_to_model, range_of_hours=range_of_hours, flight_levels=flight_levels)


if __name__ == "__main__":
    main()
