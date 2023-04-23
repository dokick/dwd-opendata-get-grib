"""
This module downloads the grib file from the opendata server and extracts the data

Pressure levels (hPa): 200, 250, 300, 400, 500, 600, 700, 850, 950, 975, 1000
"""

import asyncio
import json
import subprocess
from os import fsync
from pathlib import Path
from time import localtime

import httpx
import numpy as np
import pandas as pd
import requests


BZ2 = r".grib2.bz2"
FIELDS = "u", "v", "w"
GRIB2 = r".grib2"
JSON = r".json"
MODEL = r"regular-lat-lon_model-level"
ICOND2URL = r"http://opendata.dwd.de/weather/nwp/icon-d2/grib"


async def download_single_file(
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
        url: str,
        dest_folder: Path) -> None:
    """Downloads single file sing the given httpx client and semaphore to limit connections.

    :param httpx.AsyncClient client: httpx.ASyncClient
    :param asyncio.Semaphore semaphore: asyncio.Semaphore
    :param str url: url with the file at the ending
    :param Path dest_folder: dir where file should be saved
    :raises FileNotFoundError: if dest_folder doesn't exist
    """
    if not dest_folder.exists():
        raise FileNotFoundError(f"dir \"{dest_folder}\" doesn't exist; not automatically created")

    async with semaphore:
        try:
            async with client.stream("GET", url) as response:
                filename = url.split('/')[-1].replace(" ", "_")  # be careful with file names
                file_path = dest_folder / filename
                with open(file_path, "wb") as f:  # pylint: disable=invalid-name
                    async for chunk in response.aiter_bytes():
                        f.write(chunk)
                        # f.flush()
                        # fsync(f.fileno())
        except httpx.HTTPError as exc:
            print(f"HTTP error occured: {exc}")


async def download_url_list(url_list: list[str], dest_folder: Path, *, limit: int = 10) -> None:
    """Download files from a list of URLs with a limit on the number of concurrent connections.

    :param list[str] url_list: list of urls
    :param Path dest_folder: dir of destination
    :param int limit: limit of parallel connections, defaults to 10
    """
    async with httpx.AsyncClient() as client:
        semaphore = asyncio.Semaphore(limit)
        tasks = [asyncio.ensure_future(download_single_file(client, semaphore, url, dest_folder)) for url in url_list]  # pylint: disable=line-too-long
        await asyncio.gather(*tasks)


def download_legacy(url: str, dest_folder: Path) -> None:
    """Downloads the grib file

    :param str url: url with the file at the ending
    :param Path dest_folder: dir where file should be saved
    :raises FileNotFoundError: if dest_folder doesn't exist
    """
    if not dest_folder.exists():
        raise FileNotFoundError(f"dir \"{dest_folder}\" doesn't exist; not automatically created")

    filename = url.split('/')[-1].replace(" ", "_")  # be careful with file names
    file_path = dest_folder / filename

    req = requests.get(url, stream=True, timeout=30)
    if req.ok:
        print("saving to", Path.resolve(file_path))
        with open(file_path, 'wb') as f:  # pylint: disable=invalid-name
            for chunk in req.iter_content(chunk_size=1024 * 8):
                if chunk:
                    f.write(chunk)
                    f.flush()
                    fsync(f.fileno())
    else:  # HTTP status code 4XX/5XX
        print(f"Download failed: status code {req.status_code}\n{req.text}")


def extract_grib_file(path_to_grib_file: Path) -> None:
    """Grib data is downloaded in .bz2 files. Extractin is necessary

    :param Path path_to_grib_file: ~
    """
    if path_to_grib_file.exists():
        subprocess.run(["bzip2", "-d", path_to_grib_file], check=True)


def dump_grib_data(path_to_grib: Path) -> None:
    """Dump grib data with eccodes functions

    :param Path path_to_grib: ~
    """
    grib_stdout = subprocess.run(
        ["grib_dump", "-j", str(path_to_grib)],
        capture_output=True, text=True, check=True
    )
    json_dict = optimize_json(json.loads(grib_stdout.stdout))

    with open(path_to_grib.with_suffix(".json"), "w", encoding="utf8") as json_file:
        json.dump(json_dict, json_file, indent=4)


def delete_grib_files(dest_folder: Path) -> None:
    """Deletes left over grib files

    :param Path dest_folder: folder with grib files
    """
    subprocess.run(["rm", str(dest_folder / "*.grib2")], check=True)


def json_to_csv(path_to_json: Path) -> None:
    """Overwrites json to csv

    :param Path path_to_json_file: path to json file
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

    df = pd.DataFrame(
        values.reshape(number_latitude_points, number_longitude_points),
        index=df_idx,
        columns=df_cols
    )

    with open(path_to_json, "w", encoding="utf8") as json_file:
        json.dump(new_json_dict, json_file, indent=4)

    with open(path_to_json.with_suffix(".csv"), "w", encoding="utf8") as csv_file:
        df.to_csv(csv_file, sep=";")


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
        flight_levels: tuple[int, int] = (1, 67)) -> None:
    """Downloads the specified time from the OpenData DWD Server

    :param Path dest_folder: dir of the destination
    :param int number_of_hours: number of desired hours, defaults to 48
    :param tuple[int, int] flight_levels: desired flight levels, defaults to (1, 67)
    """
    year, month, day, hour, *_ = localtime()
    latest_hour = hour - hour % 3 - 3  # -3 because the latest hour might not be uploaded
    time_stamp = f"{year}{month:02d}{day:02d}{latest_hour:02d}"

    for field in FIELDS:
        Path.mkdir(dest_folder / time_stamp / field, parents=True, exist_ok=True)

    file_begin = fr"icon-d2_germany_{MODEL}_{time_stamp}"

    urls = {field: [] for field in FIELDS}

    for field in FIELDS:
        path_to_field_folder = dest_folder / time_stamp / field
        for hour in range(number_of_hours + 1):
            for flight_level in range(*flight_levels):
                bz2_file = fr"{file_begin}_0{hour:02d}_{flight_level}_{field}{BZ2}"
                url_to_bz2_file = fr"{ICOND2URL}/{latest_hour:02d}/{field}/{bz2_file}"
                urls[field].append(url_to_bz2_file)
        asyncio.run(download_url_list(urls[field], path_to_field_folder))
    # return
    for field in FIELDS:
        path_to_field_folder = dest_folder / time_stamp / field
        for hour in range(number_of_hours + 1):
            for flight_level in range(*flight_levels):
                bz2_file = fr"{file_begin}_0{hour:02d}_{flight_level}_{field}{BZ2}"
                grib_file = fr"{file_begin}_0{hour:02d}_{flight_level}_{field}{GRIB2}"
                print(grib_file)
                extract_grib_file(path_to_field_folder / bz2_file)
                dump_grib_data(path_to_field_folder / grib_file)
                json_file = fr"{file_begin}_0{hour:02d}_{flight_level}_{field}{JSON}"
                json_to_csv(path_to_field_folder / json_file)
        delete_grib_files(path_to_field_folder)


def main() -> None:
    """For testing and debugging purposes"""
    path_to_model = Path("/") / "media" / "sf_Ubuntu_18.04.6" / "sf_icon-d2"
    number_of_hours = 2
    flight_levels = 38, 66  # opendata.dwd.de uploads full levels, thus lowest flight level is 65
    provide_database(
        path_to_model,
        number_of_hours=number_of_hours,
        flight_levels=flight_levels
    )


if __name__ == "__main__":
    main()
