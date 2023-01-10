# SYNOP2BUFR

This module converts SYNOP files or tac messages into Python dictionaries which can be accessed directly or converted to output BUFR4 files (in accordance with the B/C1 regulations).

---

## Installation Guide

Everything necessary for use can be installed by building a Docker image and running the code in a Docker container:

1. Build the image using `docker build -t synop2bufr .`

2. Run the container using `docker run -it -v ${pwd}:/local synop2bufr`

3. Once in the Bash terminal, navigate to the local directory using `cd ./local`

Now SYNOP2BUFR has been successfully installed and is ready for use within the Docker container.

### Remarks

Example SYNOP data can be found in the _data_ folder, with the corresponding reference BUFR4 in the _reference-bufr_ folder.

To see a demonstration of this module in action, see the `demo.py` file.

To get a feel for how this module behaves, see the unit tests in `./tests/test_synop2bufr.py`

Finally, there is a `bufr_compare.sh` script available that allows you to compare the output BUFR4 files from this module to the corresponding reference BUFR4 files. This script can be ran using `./bufr_compare.sh`

---

## Usage Guide

Here we detail how SYNOP2BUFR can be used.

To begin, suppose we have some SYNOP data.

> Note: It does not matter whether this SYNOP data is a text file in the local directory or a string, provided the message(s) follow the SYNOP regulations.

In a Python file, we can import the modules of SYNOP2BUFR by:

```
from synop2bufr import method_name
```

where `method_name` is a placeholder for the following methods provided in this module:

| Method         | Description                                                                                                        |
| ----------------- | ------------------------------------------------------------------------------------------------------------------ |
| `to_bufr`         | Conversion of all SYNOP data to multiple BUFR4 files.                                                              |
| `to_json`         | Conversion of all SYNOP data to a nested Python dictionary object, keyed by TSI (traditional station idenfitifer). |
| `convert_to_dict` | Conversion of a single SYNOP tac string to a Python dictionary object.                                             |
| `message_extract` | Extracts and reformats the individual SYNOP messages from a single string.                                         |
| `file_extract`    | Extracts and reformats the individual SYNOP messages from a single text file.                                      |

___

### Conversion to BUFR

The `to_bufr` method can be used in the following way:

```
to_bufr(SYNOP message)
```

where, as mentioned before, the input can either be the tac string itself or the directory to the text file containing the SYNOP data.

This method generates BUFR4 file(s) in a folder called _output-bufr_. The number of BUFR4 files generated is equivalent to the number of SYNOP messages input.

### Example

Suppose we have a text file named `A_SMRO01YRBK211200_C_EDZW_20220321120500_12524785.txt` containing 23 SYNOP messages. We can convert these to 23 BUFR4 files with the following code:

```
from synop2bufr import to_json

to_json("./A_SMRO01YRBK211200_C_EDZW_20220321120500_12524785.txt")
```

> Note: the Python file must be run in the Docker container, not on your physical machine!

___

### Conversion to a Python dictionary

As mentioned before, this module offers *two* *methods* to obtain the Python dictionary of SYNOP message(s) prior to conversion to BUFR.

The most simple of which is `convert_to_dict`. This can be used in the following way:
```
convert_to_dict(one SYNOP message, year of message, month of message)
```
where the SYNOP message must be a string, and the year/month must be an integer. This returns an array containing a single Python dictionary for the decoded message, as well as the number of section 3 and section 4 cloud groups detected[^1].

[^1]: These are the replicated cloud groups of section 3 and section 4 of a SYNOP message. See the [WMO manual on FM-12](https://library.wmo.int/doc_num.php?explnum_id=10235) for more details.

> Note: For this method, the terminating character '=' of the SYNOP message must be omitted.

Another and more complex method is `to_json`. This can be used in the following way:
```
to_json(SYNOP message)
```
where the input is the same as the `to_bufr` method. Just like `convert_to_dict`, this returns an array containing a nested Python dictionary for all of the decoded messages, as well as the number of section 3 and section 4 cloud groups detected.

The main advantage of `to_json` is that it is able to process several SYNOP messages simultaneously to return one nested dictionary for all stations.

Additionally, when a text file name is provided as input, it automatically determines the year/month of the message (and otherwise automatically assigns the current year/month).

> Note: For this method, the terminating character '=' of each SYNOP message must remain in the string.

### Example
Suppose we have the following SYNOP messages:
```
AAXX 21121

15015 02999 02501 10103 21090 39765 42952 57020 60001=

15020 02997 23104 10130 21075 30177 40377 58020 60001 81041=
```

We can decode one of the messages, e.g. the former, using `convert_to_dict` as follows:
```
from synop2bufr import convert_to_dict

message = "AAXX 21121 
15001 05515 32931 10103 21090 39765 42250 57020 60001"

convert_to_dict(message, 2023, 1)
```
which returns (when pretty printed):
```
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

We can decode all of these messages at once using `to_json` as follows:

```
from synop2bufr import to_json

messages = """AAXX 21121

15015 02999 02501 10103 21090 39765 42952 57020=

15020 02997 23104 10130 21075 30177 40377 58020 60001 81041="""

to_json(messages)
```
which returns (when pretty printed):
```
{
  "15015": [
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
      "station_pressure": 97650.0,
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
  ],
  "15020": [
    {
      "report_type": "AAXX",
      "year": 2023,
      "month": 1,
      "day": 21,
      "hour": 12,
      "minute": 0,
      "wind_indicator": 8,
      "block_no": "15",
      "station_no": "020",
      "station_id": "15020",
      "region": null,
      "WMO_station_type": 1,
      "lowest_cloud_base": 2500,
      "visibility": 10000,
      "cloud_cover": 25,
      "time_significance": 2,
      "wind_time_period": -10,
      "wind_direction": 310,
      "wind_speed": 4,
      "air_temperature": 286.15,
      "dewpoint_temperature": 265.65,
      "relative_humidity": 23.314606896338145,
      "station_pressure": 101770.0,
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
      "cloud_vs_s1": 63,
      "cloud_amount_s1": null,
      "low_cloud_type": 63,
      "middle_cloud_type": 63,
      "high_cloud_type": 63,
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
}
```

> Note: As the example messages do not contain section 3 nor section 4 groups, the number of such cloud groups detected is 0 in both outputs.

___

### Message extraction
The remaining two methods provided by SYNOP2BUFR are relatively basic and unlikely to be used. These are `message_extract` and `file_extract`, which as mentioned above are used to extract strings ready for conversion into a Python dictionary and subsequently BUFR4 files.

One can use `message_extract` in the following way:
```
message_extract(SYNOP message string)
```
which returns an array of strings, where each string is an individual SYNOP message (ready for `convert_to_dict` for example).

One can use `file_extract` in the following way:
```
file_extract(SYNOP message text file directory)
```
which returns the same array as `message_extract` would if provided the contents of the file, as well as the year and month determined by the file name.

---
## License
Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.