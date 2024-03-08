# synop2bufr

The synop2bufr Python module contains both a command line interface and API to convert data stored in SYNOP or TAC text file to the WMO BUFR data format. More information on the BUFR format can be found in the WMO Manual on Codes, Volume I.2.

## Installation

### Requirements
- Python 3 and above
- [ecCodes](https://confluence.ecmwf.int/display/ECC)
- [csv2bufr](https://github.com/wmo-im/csv2bufr)

### Dependencies

Dependencies are listed in [requirements.txt](https://github.com/wmo-im/synop2bufr/blob/main/requirements.txt). Dependencies are automatically installed during synop2bufr installation.

### Setting Environment Variables

Before using synop2bufr, we highly encourage you to set the `BUFR_ORIGINATING_CENTRE` and `BUFR_ORIGINATING_SUBCENTRE` environment variables. These variables are used to specify the originating centre and subcentre of the SYNOP messages. **Without these set, they will default to missing (255).**

You can set these environment variables in your shell if you want to run synop2bufr on your local machine. Here's how you can do it in a Bash shell:

```bash
export BUFR_ORIGINATING_CENTRE=<centre_value>
export BUFR_ORIGINATING_SUBCENTRE=<subcentre_value>
```

## Running

To run synop2bufr from a Docker container:

```console
docker run -it -v /$(pwd):/local wmoim/dim_eccodes_baseimage:2.34.0 bash
apt-get update && apt-get install -y git
cd /local
python3 setup.py install
synop2bufr --help
```

Example data can be found in `data` directory, with the corresponding reference BUFR4 in `data/bufr`.

To transform SYNOP data file into BUFR:

```console
mkdir output-data
synop2bufr data transform --metadata data/station_list.csv --year 2023 --month 03 --output-dir output-data data/A_SMRO01YRBK211200_C_EDZW_20220321120500_12524785.txt
```

To run synop2bufr inside a Lambda function on Amazon Web Services, please refer to [aws-lambda/README.md](aws-lambda/README.md) and use this [Dockerfile](aws-lambda/Dockerfile) to build the container image for the Lambda function.

## API Usage Guide

Here we detail how the synop2bufr API can be used in Python.

To begin, suppose we have some SYNOP data.

> Note: It does not matter whether this SYNOP data is a text file in the local directory or a string, provided the message(s) follow the SYNOP regulations.

In a Python file, we can import the modules of synop2bufr by:

```
from synop2bufr import method_name
```

where `method_name` is a placeholder for the following methods provided in this module:

| Method         | Description                                                                                                        |
| ----------------- | ------------------------------------------------------------------------------------------------------------------ |
| `transform`         | Conversion of all SYNOP data to multiple BUFR4 files.                                                              |
| `parse_synop` | Conversion of a single SYNOP tac string to a Python dictionary object.                                             |
| `extract_individual_synop` | Extracts and reformats the individual SYNOP messages from a single string.                                         |
| `file_extract`    | Extracts and reformats the individual SYNOP messages from a single text file.                                      |

___

### Conversion to BUFR

The `to_bufr` method can be used in the following way:

```python
to_bufr(synop_message)
```

where, as mentioned before, the input can either be the tac string itself or the directory to the text file containing the SYNOP data.

This method generates BUFR4 file(s) in a folder called _output-bufr_. The number of BUFR4 files generated is equivalent to the number of SYNOP messages input.

### Example

Suppose we have a text file named `A_SMRO01YRBK211200_C_EDZW_20220321120500_12524785.txt` containing 23 SYNOP reports from January 2023, with corresponding station metadata `"station_list.csv"`. We can convert these to 23 BUFR4 files with the following code:

```python
from synop2bufr import transform

transform(data = "A_SMRO01YRBK211200_C_EDZW_20220321120500_12524785.txt", metadata = "station_list.csv", year = 2023, month = 1)
```

> Note: the Python file must be run in the Docker container, not on your physical machine!

___

### Conversion to a Python dictionary

synop2bufr offers two methods to obtain the Python dictionary of SYNOP message(s) prior to conversion to BUFR.

The most simple of which is `parse_synop`. This can be used in the following way:

```python
parse_synop(single_synop_message, year, month)
```

where the SYNOP message must be a string, and the year/month must be an integer. This returns an array containing a single Python dictionary for the decoded message, as well as the number of section 3 and section 4 cloud groups detected[^1].

[^1]: These are the replicated cloud groups of section 3 and section 4 of a SYNOP message. See the [WMO manual on FM-12](https://library.wmo.int/doc_num.php?explnum_id=10235) for more details.

> Note: For this method, the terminating character `=` of the SYNOP message must be omitted.

### Example

Suppose we have the following SYNOP messages:

```
AAXX 21121

15015 02999 02501 10103 21090 39765 42952 57020 60001=

15020 02997 23104 10130 21075 30177 40377 58020 60001 81041=
```

We can decode one of the messages, e.g. the former, using `parse_synop` as follows:

```python
from synop2bufr import parse_synop

message = "AAXX 21121 15001 05515 32931 10103 21090 39765 42250 57020 60001"

parse_synop(data = message, year = 2023, month = 1)
```

which returns (pretty printed):

```json
[
  {
    "report_type": "AAXX",
    "year": 2023,
    "month": 1,
    "day": 21,
    "hour": 12,
    "minute": 0,
    "wind_indicator": 8,
    "block_no": "15",
    "station_no": "015",
    "station_id": "15015",
    "region": null,
    "WMO_station_type": 1,
    "lowest_cloud_base": null,
    "visibility": 50000,
    "cloud_cover": 0,
    "time_significance": 2,
    "wind_time_period": -10,
    "wind_direction": 250,
    "wind_speed": 1,
    "air_temperature": 283.45,
    "dewpoint_temperature": 264.15,
    "relative_humidity": 24.799534703795413,
    "station_pressure": null,
    "isobaric_surface": null,
    "geopotential_height": null,
    "sea_level_pressure": null,
    "3hr_pressure_change": null,
    "pressure_tendency_characteristic": 15,
    "precipitation_s1": null,
    "ps1_time_period": null,
    "present_weather": 511,
    "past_weather_1": 31,
    "past_weather_2": 31,
    "past_weather_time_period": -6,
    "cloud_vs_s1": 62,
    "cloud_amount_s1": 0,
    "low_cloud_type": 30,
    "middle_cloud_type": 20,
    "high_cloud_type": 10,
    "maximum_temperature": null,
    "minimum_temperature": null,
    "ground_state": null,
    "ground_temperature": null,
    "snow_depth": null,
    "evapotranspiration": null,
    "evaporation_instrument": null,
    "temperature_change": null,
    "tc_time_period": null,
    "sunshine_amount_1hr": null,
    "sunshine_amount_24hr": null,
    "low_cloud_drift_direction": null,
    "low_cloud_drift_vs": null,
    "middle_cloud_drift_direction": null,
    "middle_cloud_drift_vs": null,
    "high_cloud_drift_direction": null,
    "high_cloud_drift_vs": null,
    "e_cloud_genus": null,
    "e_cloud_direction": null,
    "e_cloud_elevation": null,
    "24hr_pressure_change": null,
    "net_radiation_1hr": null,
    "net_radiation_24hr": null,
    "global_solar_radiation_1hr": null,
    "global_solar_radiation_24hr": null,
    "diffuse_solar_radiation_1hr": null,
    "diffuse_solar_radiation_24hr": null,
    "long_wave_radiation_1hr": null,
    "long_wave_radiation_24hr": null,
    "short_wave_radiation_1hr": null,
    "short_wave_radiation_24hr": null,
    "net_short_wave_radiation_1hr": null,
    "net_short_wave_radiation_24hr": null,
    "direct_solar_radiation_1hr": null,
    "direct_solar_radiation_24hr": null,
    "precipitation_s3": null,
    "ps3_time_period": null,
    "precipitation_24h": null,
    "highest_gust_1": null,
    "highest_gust_2": null,
    "hg2_time_period": -360
  },
  0,
  0
]
```

> Note: The dictionary returned always has the same keys, meaning that often there are many null items as these groups aren't present in the SYNOP message.

> Note 2: As the example messages do not contain section 3 nor section 4 groups, the number of such cloud groups detected is 0.

___

### Message extraction

The remaining two methods provided by synop2bufr are relatively basic and unlikely to be used. These are `extract_individual_synop` and `file_extract`, which as mentioned above are used to extract strings ready for conversion into a Python dictionary and subsequently BUFR4 files.

One can use `extract_individual_synop` in the following way:

```python
extract_individual_synop(SYNOP message string)
```

which returns an array of strings, where each string is an individual SYNOP message (ready for `convert_to_dict` for example).

One can use `file_extract` in the following way:

```python
file_extract(SYNOP message text file directory)
```

which returns the same array as `extract_individual_synop` would if provided the contents of the file, as well as the year and month determined by the file name.

---

## Releasing

```console
# create release (x.y.z is the release version)
vi synop2bufr/__init__.py  # update __version__
git commit -am 'update release version vx.y.z'
git push origin main
git tag -a vx.y.z -m 'tagging release version vx.y.z'
git push --tags

# upload to PyPI
rm -fr build dist *.egg-info
python setup.py sdist bdist_wheel --universal
twine upload dist/*

# publish release on GitHub (https://github.com/wmo-im/synop2bufr/releases/new)

# bump version back to dev
vi synop2bufr/__init__.py  # update __version__
git commit -am 'back to dev'
git push origin main
```
## Documentation

The full documentation for synop2bufr can be found at [https://synop2bufr.readthedocs.io](https://synop2bufr.readthedocs.io), including sample files.

### Code Conventions

* [PEP8](https://www.python.org/dev/peps/pep-0008)

### Bugs and Issues

All bugs, enhancements and issues are managed on [GitHub](https://github.com/wmo-im/synop2bufr/issues).

## Contact

* [David Berry](https://github.com/david-i-berry)
* [Rory Burke](https://github.com/RoryPTB)
