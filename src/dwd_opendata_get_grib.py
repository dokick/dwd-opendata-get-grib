"""This module downloads the grib file from the opendata server and extracts the data"""

import asyncio
import bz2
import json
import multiprocessing as mp
import subprocess  # only for grib_dump solution until bundling with pyinstaller works
import sys
from argparse import ArgumentParser
from pathlib import Path
from time import localtime
from typing import Dict, Final, Iterable, List, Mapping, Sequence, Tuple, Union

import httpx
import numpy as np
import pandas as pd


BZ2_SUFFIX: Final[str] = r".bz2"
FIELDS = "u", "v", "w"
GRIB2_SUFFIX: Final[str] = r".grib2"
JSON_SUFFIX: Final[str] = r".json"
MODEL: Final[str] = r"regular-lat-lon_model-level"
ICOND2_URL = r"https://opendata.dwd.de/weather/nwp/icon-d2/grib"
GRIB_META_FIELDS: Final = (
    "centre",
    "subCentre",
    "significanceOfReferenceTime",
    "dataDate",
    "dataTime",
    "grib2LocalSectionNumber",
    "numberOfDataPoints",
    "shapeOfTheEarth",
    # i and j values
    "Ni",
    "Nj",
    "iScansNegatively",
    "jScansPositively",
    "jPointsAreConsecutive",
    "alternativeRowScanning",
    "latitudeOfFirstGridPointInDegrees",
    "longitudeOfFirstGridPointInDegrees",
    "latitudeOfLastGridPointInDegrees",
    "longitudeOfLastGridPointInDegrees",
    "iDirectionIncrementInDegrees",
    "jDirectionIncrementInDegrees",
    "gridType",
    "NV",
    "productDefinitionTemplateNumber",
    "parameterCategory",
    "parameterNumber",
    "parameterUnits",
    "parameterName",
    "typeOfGeneratingProcess",
    "generatingProcessIdentifier",
    "indicatorOfUnitOfTimeRange",
    "stepUnits",
    "forecastTime",
    "stepRange",
    # first fixed surface
    "typeOfFirstFixedSurface",
    "unitsOfFirstFixedSurface",
    "nameOfFirstFixedSurface",
    "scaleFactorOfFirstFixedSurface",
    "scaledValueOfFirstFixedSurface",
    # second fixed surface
    "typeOfSecondFixedSurface",
    "unitsOfSecondFixedSurface",
    "nameOfSecondFixedSurface",
    "scaleFactorOfSecondFixedSurface",
    "scaledValueOfSecondFixedSurface",
    # levels
    # "topLevel",
    # "bottomLevel",
    # names of parameters
    "shortName",
    "name",
    "cfName",
    "cfVarName",
    "nlev",
    "numberOfVGridUsed",
    "numberOfValues",
    "packingType",
    "bitMapIndicator",
    "bitmapPresent",
    # statistical values
    "maximum",
    "minimum",
    "average",
    "numberOfMissing",
    "standardDeviation",
    "skewness",
    "kurtosis",
    "getNumberOfValues"
)
INDEX_47_DEG_LAT: Final[int] = 191
INDEX_54P98_DEG_LAT: Final[int] = 591
INDEX_5_DEG_LON: Final[int] = 447
INDEX_14P98_DEG_LON: Final[int] = 947


async def download_single_file(
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
        url: str,
        destination_folder: Path
) -> None:
    """Downloads single file sing the given httpx client and semaphore to limit connections.

    :param httpx.AsyncClient client: httpx.ASyncClient
    :param asyncio.Semaphore semaphore: asyncio.Semaphore
    :param str url: url with the file at the ending
    :param Path destination_folder: directory where file should be saved
    :raises FileNotFoundError: if ``destination_folder`` doesn't exist
    """
    if not destination_folder.exists():
        raise FileNotFoundError(f"dir \"{str(destination_folder)}\" doesn't exist; not automatically created")

    async with semaphore:
        try:
            async with client.stream("GET", url) as response:
                filename = url.split('/')[-1].replace(" ", "_")  # be careful with file names
                file_path = destination_folder / filename
                with open(file_path, "wb") as stream:
                    async for chunk in response.aiter_bytes():
                        stream.write(chunk)
                        # stream.flush()
                        # fsync(stream.fileno())
        except httpx.HTTPError as exc:
            print(f"HTTP error occurred: {exc}")


async def download_url_list(url_list: Iterable[str], destination_folder: Path, *, limit: int = 10) -> None:
    """Download files from a list of URLs with a limit on the number of concurrent connections.

    :param Iterable[str] url_list: list of urls
    :param Path destination_folder: directory for destination
    :param int limit: limit of parallel connections, defaults to ``10``
    """
    async with httpx.AsyncClient() as client:
        semaphore = asyncio.Semaphore(limit)
        tasks = [asyncio.ensure_future(
            download_single_file(client, semaphore, url, destination_folder)
        ) for url in url_list]
        await asyncio.gather(*tasks)


def extract_grib_file(bz2_file_path: Path) -> None:
    """Grib data is downloaded in .bz2 files. Extraction is necessary

    :param Path bz2_file_path: path to grib file
    """
    with open(bz2_file_path, mode="rb") as compressed_stream:
        decompressed_data = bz2.decompress(compressed_stream.read())
    with open(bz2_file_path.with_suffix(""), mode="wb") as decompressed_stream:
        decompressed_stream.write(decompressed_data)


def get_grib_data(grib_file_path: Path) -> Union[int, None]:
    """Dump grib data with ecCodes functions

    :param Path grib_file_path: path to grib file
    """
    try:  # first try eccodes python bindings
        from eccodes import codes_grib_new_from_file
        with open(grib_file_path, "rb") as grib_stream:
            grib_id = codes_grib_new_from_file(grib_stream)
            return grib_id
    except (ModuleNotFoundError, RuntimeError, FileNotFoundError):
        try:  # try directly calling the binaries
            grib_stdout = subprocess.run(
                ["grib_dump", "-j", str(grib_file_path)],
                capture_output=True,
                text=True,
                check=True
            )
            grib_data = optimize_json(json.loads(grib_stdout.stdout)["messages"][0])
            with open(grib_file_path.with_suffix(".json"), "w", encoding="utf-8") as json_stream:
                json.dump(grib_data, json_stream, indent=4)
        except FileNotFoundError as exc:
            print(exc.args)
            print(f"Missing file: {exc.filename}")
            sys.exit(
                "ecCodes is probably not installed. ecCodes isn't available on PyPI."
                " Refer here for installation: https://confluence.ecmwf.int/display/ECC/ecCodes+Installation"
            )


def delete_files(destination_folder: Path, *, pattern: str = "*.grib2") -> None:
    """Deletes left over grib files

    :param Path destination_folder: folder with grib files
    :param str pattern: suffix of files to delete, defaults to ``.grib2``
    """
    for file in destination_folder.glob(pattern):
        file.unlink(missing_ok=True)


def create_meta_json_and_csv_from_grib_id(grib_id: int, json_file_path: Path) -> np.ndarray:
    """Converts metadata to json file and weather data to csv file from grib id

    :param int grib_id: grib id
    :param Path json_file_path: path to json file
    :return: numpy array of only germany
    :rtype: numpy.ndarray
    """
    from eccodes import codes_get, codes_get_double_array
    metadata_json: Dict[str, Union[float, str]] = {
        grib_field: codes_get(grib_id, grib_field) for grib_field in GRIB_META_FIELDS
    }
    with open(json_file_path, "w", encoding="utf-8") as json_stream:
        json.dump(metadata_json, json_stream, indent=4)

    number_longitude_points = codes_get(grib_id, "Ni")
    number_latitude_points = codes_get(grib_id, "Nj")
    start_longitude = codes_get(grib_id, "longitudeOfFirstGridPointInDegrees") * 100
    end_longitude = codes_get(grib_id, "longitudeOfLastGridPointInDegrees") * 100
    start_latitude = codes_get(grib_id, "latitudeOfFirstGridPointInDegrees") * 100
    end_latitude = codes_get(grib_id, "latitudeOfLastGridPointInDegrees") * 100
    step = codes_get(grib_id, "iDirectionIncrementInDegrees") * 100
    df_idx = np.arange(start_latitude, end_latitude + step, step) / 100

    if start_longitude > end_longitude:
        df_cols_1 = np.arange(start_longitude, 36000, step)
        df_cols_2 = np.arange(0, end_longitude + step, step)
        df_cols = np.concatenate((df_cols_1, df_cols_2)) / 100
    else:
        df_cols = np.arange(start_longitude, end_longitude, step) / 100

    values = codes_get_double_array(grib_id, "values")  # codes_get_double_array uses float64

    data_matrix = values.reshape(number_latitude_points, number_longitude_points)
    # with debugging and numerical analysis this reshaping is correct. values from json output are written
    # in this order: all longitude values for one latitude value, for all latitude values (basically C order)
    # data_matrix = values.reshape(number_longitude_points, number_latitude_points) <-- this is wrong

    frame = pd.DataFrame(data_matrix, index=df_idx, columns=df_cols)
    frame.to_csv(json_file_path.with_suffix(".csv"), sep=";", lineterminator="\n")

    only_germany = data_matrix[INDEX_47_DEG_LAT:INDEX_54P98_DEG_LAT, INDEX_5_DEG_LON:INDEX_14P98_DEG_LON]
    return only_germany


def create_meta_json_and_csv_from_json(json_file_path: Path) -> np.ndarray:
    """Converts metadata to json file and weather data to csv file from already existing json file

    :param json_file_path: path to json file
    :return: numpy array of only germany
    :rtype: numpy.ndarray
    """
    with open(json_file_path, "r", encoding="utf-8") as json_stream:
        # json_key_val_seq: Sequence[Dict[str, Union[int, float, str]]] = json.load(json_stream)
        json_key_val_seq: Dict[str, Union[float, str]] = json.load(json_stream)

    meta_json = {grib_meta_field: json_key_val_seq[grib_meta_field] for grib_meta_field in GRIB_META_FIELDS}
    with open(json_file_path, "w", encoding="utf-8") as json_stream:
        json.dump(meta_json, json_stream, indent=4)

    number_longitude_points = meta_json["Ni"]
    number_latitude_points = meta_json["Nj"]
    start_longitude = meta_json["longitudeOfFirstGridPointInDegrees"] * 100
    end_longitude = meta_json["longitudeOfLastGridPointInDegrees"] * 100
    start_latitude = meta_json["latitudeOfFirstGridPointInDegrees"] * 100
    end_latitude = meta_json["latitudeOfLastGridPointInDegrees"] * 100
    step = meta_json["iDirectionIncrementInDegrees"] * 100
    df_idx = np.arange(start_latitude, end_latitude + step, step) / 100

    if start_longitude > end_longitude:
        df_cols_1 = np.arange(start_longitude, 36000, step)
        df_cols_2 = np.arange(0, end_longitude + step, step)
        df_cols = np.concatenate((df_cols_1, df_cols_2)) / 100
    else:
        df_cols = np.arange(start_longitude, end_longitude, step) / 100

    data_matrix = np.array(
        json_key_val_seq["values"],
        dtype=np.float64
    ).reshape(number_latitude_points, number_longitude_points)

    frame = pd.DataFrame(data_matrix, index=df_idx, columns=df_cols)
    frame.to_csv(json_file_path.with_suffix(".csv"), sep=";", lineterminator="\n")

    only_germany = data_matrix[INDEX_47_DEG_LAT:INDEX_54P98_DEG_LAT, INDEX_5_DEG_LON:INDEX_14P98_DEG_LON]
    return only_germany


def optimize_json(json_key_val_seq: Sequence[Mapping[str, Union[float, str]]]) -> Dict[str, Union[float, str]]:
    """Optimizes json output from DWD server

    :param Sequence[Mapping[str, Union[float, str]]] json_key_val_seq: raw json from codes_dump output
    :return Dict[str, Union[float, str]]: better and more readable json format
    """
    amount_of_messages = len(json_key_val_seq)
    return {json_key_val_seq[i]["key"]: json_key_val_seq[i]["value"] for i in range(amount_of_messages)}
    # return {json_obj["key"]: json_obj["value"] for json_obj in json_key_val_seq}


def create_binary_file_over_all_flight_levels(data_matrices: Iterable[np.ndarray], json_file_path: Path) -> None:
    """Creates .bin files over all flight levels so reading into MATLab is easier.

    :param Iterable[np.ndarray] data_matrices: data matrices of germany
    :param Path json_file_path: path to json file without flight level specification
    """
    with open(json_file_path.with_suffix(".bin"), "wb") as binary_stream:
        for only_germany_height in data_matrices:
            for val in np.nditer(only_germany_height, order="C"):
                binary_stream.write(bytes(val))


def from_bz2_to_csv_and_json(paths: Tuple[Path, Path, Path]) -> Tuple[Tuple[Path, Path, Path], np.ndarray]:
    """.grib2 to .csv

    :param Tuple[Path, Path, Path] paths:
    :return: ...
    :rtype: Tuple[Tuple[Path, Path, Path], np.ndarray]
    """
    grib_file_path, bz2_file_path, json_file_path = paths
    extract_grib_file(bz2_file_path)
    print(f"{bz2_file_path.name} decompressed")
    grib_id = get_grib_data(grib_file_path)
    print(f"{grib_file_path.name} data extracted")
    if grib_id is not None:
        data_matrix = create_meta_json_and_csv_from_grib_id(grib_id, json_file_path)
    else:
        data_matrix = create_meta_json_and_csv_from_json(json_file_path)
    print(f"{grib_file_path.name} .csv created")
    return (grib_file_path, bz2_file_path, json_file_path), data_matrix


def get_wind_data(
        destination_folder: Path,
        *,
        range_of_hours: Tuple[int, int] = (0, 48),
        flight_levels: Tuple[int, int] = (38, 65),
        latest: bool = False
) -> None:
    """Downloads the specified time from the OpenData DWD Server

    :param Path destination_folder: directory of the destination
    :param Tuple[int, int] range_of_hours: range of desired hours, defaults to ``(0, 48)``
    :param Tuple[int, int] flight_levels: desired flight levels, defaults to ``(38, 65)``
    :param bool latest: include latest data or not
    """
    # Next 8 code lines: get latest timestamp
    year, month, day, local_hour, *_ = localtime()
    latest_hour = local_hour - local_hour % 3
    if not latest:
        latest_hour -= 3  # -3 because the latest hour might not be uploaded
    if latest_hour < 0:
        day -= 1
        latest_hour += 24
    time_stamp = f"{year}{month:02d}{day:02d}{latest_hour:02d}"

    # create field folders
    for field in FIELDS:
        Path.mkdir(destination_folder / time_stamp / field, parents=True, exist_ok=True)

    file_begin = fr"icon-d2_germany_{MODEL}_{time_stamp}"

    hour_start, hour_stop = range_of_hours
    urls: Dict[str, List[str]] = {field: [] for field in FIELDS}
    file_info: Dict[str, Dict[str, List[Tuple[Path, Path, Path]]]] = {
        field: {str(hour): [] for hour in range(hour_start, hour_stop + 1)} for field in FIELDS
    }

    # download loop
    flight_level_start, flight_level_stop = flight_levels
    for field in FIELDS:
        field_folder_path = destination_folder / time_stamp / field
        for hour in range(hour_start, hour_stop + 1):
            for flight_level in range(flight_level_start, flight_level_stop + 1):
                grib_file: str = fr"{file_begin}_0{hour:02d}_{flight_level}_{field}{GRIB2_SUFFIX}"
                bz2_file: str = fr"{grib_file}{BZ2_SUFFIX}"
                url_to_bz2_file: str = fr"{ICOND2_URL}/{latest_hour:02d}/{field}/{bz2_file}"
                urls[field].append(url_to_bz2_file)
                json_file: str = fr"{file_begin}_0{hour:02d}_{flight_level}_{field}{JSON_SUFFIX}"
                file_info[field][str(hour)].append(
                    (
                        field_folder_path / grib_file,
                        field_folder_path / bz2_file,
                        field_folder_path / json_file
                    )
                )
        asyncio.run(download_url_list(urls[field], field_folder_path))
    print("Download of data files finished")

    # data transformation loop
    for field in FIELDS:
        field_folder_path = destination_folder / time_stamp / field
        for hour in range(hour_start, hour_stop + 1):
            with mp.Pool() as pool:
                results = pool.map(from_bz2_to_csv_and_json, file_info[field][str(hour)], chunksize=3)
                data_matrices: List[np.ndarray] = [data_matrix for _, data_matrix in results]
            create_binary_file_over_all_flight_levels(
                data_matrices,
                field_folder_path / fr"{file_begin}_0{hour:02d}_{field}{JSON_SUFFIX}"
            )
        delete_files(field_folder_path)


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
    mp.freeze_support()
    main()
