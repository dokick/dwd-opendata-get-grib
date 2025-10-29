# dwd-opendata-get-grib

[![testing: pytest](https://img.shields.io/badge/testing-pytest-blue)](https://github.com/pytest-dev/pytest)
[![linting: pylint](https://img.shields.io/badge/linting-pylint-yellowgreen)](https://github.com/pylint-dev/pylint)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)

## Introduction

Functions to get grib data from the DWD OpenData Server and
convert it to a format that is more readable and
easier to access and perform Data Science on it.

## Requirements

- ecCodes (Installation instructions [here](https://confluence.ecmwf.int/display/ECC/ecCodes+installation))

## Installation

As a package with pip:

``pip install dwd-opendata-get-grib``

or with conda/mamba to get the eccodes library as a direct binary

``conda install -c conda-forge dwd-opendata-get-grib``

``mamba install -c conda-forge dwd-opendata-get-grib``

As a standalone:

Download .zip archive from GitHub Releases. Built with pyinstaller.

## Table of full height levels

| level idx. | height [m] | level idx. | height [m] | level idx. | height [m] | level idx. | height [m] |
|------------|------------|------------|------------|------------|------------|------------|------------|
| 1          | 20700.926  | 18         | 8446.611   | 35         | 3639.535   | 52         | 924.048    |
| 2          | 18707.630  | 19         | 8073.640   | 36         | 3434.414   | 53         | 855.630    |
| 3          | 17459.836  | 20         | 7715.350   | 37         | 3235.976   | 54         | 752.427    |
| 4          | 16432.216  | 21         | 7370.106   | 38         | 3044.029   | 55         | 654.479    |
| 5          | 15538.089  | 22         | 7039.106   | 39         | 2858.399   | 56         | 561.856    |
| 6          | 14738.074  | 23         | 6719.557   | 40         | 2678.926   | 57         | 474.652    |
| 7          | 14009.789  | 24         | 6411.462   | 41         | 2505.461   | 58         | 393.002    |
| 8          | 13338.901  | 25         | 6114.219   | 42         | 2337.870   | 59         | 317.092    |
| 9          | 12715.508  | 26         | 5827.280   | 43         | 2176.032   | 60         | 247.172    |
| 10         | 12132.398  | 27         | 5550.148   | 44         | 2019.836   | 61         | 183.592    |
| 11         | 11584.105  | 28         | 5282.374   | 45         | 1869.185   | 62         | 126.857    |
| 12         | 11066.360  | 29         | 5023.548   | 46         | 1723.991   | 63         | 77.745     |
| 13         | 10575.747  | 30         | 4773.294   | 47         | 1584.179   | 64         | 37.606     |
| 14         | 10109.477  | 31         | 4531.269   | 48         | 1449.686   | 65         | 10.000     |
| 15         | 9665.235   | 32         | 4297.157   | 49         | 1320.458   |            |            |
| 16         | 9241.077   | 33         | 4070.672   | 50         | 1196.457   |            |            |
| 17         | 8835.344   | 34         | 3851.546   | 51         | 1077.658   |            |            |

## Table of full pressure levels

| pressure level [hPa] |
|----------------------|
| 200                  |
| 250                  |
| 300                  |
| 400                  |
| 500                  |
| 600                  |
| 700                  |
| 850                  |
| 950                  |
| 975                  |
| 1000                 |

## Build

With pyinstaller:

``pyinstaller --clean --add-binary "~/path/to/eccodes/installation/Library/bin/grib_dump.exe;."
.\src\dwd_opendata_get_grib.py``
