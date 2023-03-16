.. _quickstart:

Quick start
===========

The synop2bufr Python module contains both a command line interface and an API to convert data
stored in the WMO FM-12 SYNOP data format to the WMO BUFR format.
For example, the command line interface reads in data from a text file, converts it to BUFR and writes out the data to the specified directory. e.g.:


.. code-block:: shell

   synop2bufr transform \
       --metadata <station-metadata.csv> \
       --output-dir <output-directory-path> \
       --year <year-of-observation> \
       --month <month-of-observation> \
       <input-fm12.txt>

This command is explained in more detail below.

Command line interface (CLI)
****************************

.. code-block:: shell

   synop2bufr transform \
       --metadata <station-list.csv> \
       --output-dir <output-directory-path> \
       --year <year-of-observation> \
       --month <month-of-observation> \
       <input-fm12.txt>

Input FM-12 file (input-fm12.txt)
---------------------------------
The FM-12 input data format is described in the `WMO Manual on Codes, Volume I.1 <https://library.wmo.int/doc_num.php?explnum_id=10235>`__.
Each message in the input data file must be terminated with a equals (=) symbol as per the example data below:

.. code-block::

    AAXX 21121

    15015 02999 02501 10103 21090 39765 42952 57020 60001=

    15020 02997 23104 10130 21075 30177 40377 58020 60001 81041=

This input file is specified after the options in the CLI.

Input metadata file (station-metadata.csv)
------------------------------------------
Due to the limitations of the FM-12 SYNOP format additional metadata needs to be passed to
synop2bufr for inclusion in the BUFR data. The formatting of this file is the same as for the wis2box (see
`wis2box documentation <https://docs.wis2box.wis.wmo.int/en/latest/reference/running/station-metadata.html>`__ for further details).

The required columns are
    - `station_name`: The name of the observing station.
    - `wigos_station_identifier`: The WIGOS station identifier for the station.
    - `traditional_station_identifier`: The non-WIGOS station identifier for the station.




Output directory (output-directory-path)
----------------------------------------
This specifies the directory to write the output BUFR files to. One BUFR file per weather report / observation
is created.

Year of observation (year-of-observation)
-----------------------------------------
Due to the limitations of the FM-12 SYNOP format it is not possible to encode the year of observation as part of the
message. Instead this is required to be passed as an option to `synop2bufr`


Month of observation (month-of-observation)
-------------------------------------------
Due to the limitations of the FM-12 SYNOP format it is not possible to encode the month of observation as part of the
message. Instead this is required to be passed as an option to `synop2bufr`

API
===

Here we detail how the synop2bufr API can be used in Python. To begin, suppose we have some SYNOP data.

*Note*: It does not matter whether this SYNOP data is a text file in the local directory or a string, provided the message(s) follow the SYNOP regulations.

In a Python file, we can import the modules of SYNOP2BUFR by:

.. code-block:: python

    from synop2bufr import method_name

where `method_name` is a placeholder for the following methods provided in this module:


.. list-table::
   :widths: 25 50
   :header-rows: 1

   * - Method
     - Description
   * - ``transform``
     - Conversion of all SYNOP data to BUFR files.
   * - ``convert_to_dict``
     - Conversion of a single SYNOP tac string to a Python dictionary object.
   * - ``message_extract``
     - Extracts and reformats the individual SYNOP messages from a single string.
   * - ``file_extract``
     - Extracts and reformats the individual SYNOP messages from a single text file.


Transform
*********

The SYNOP to BUFR transform method can be used in the following way: 

.. code-block:: python
    
    transform(SYNOP message, metadata, year, month)

where, as mentioned before, the SYNOP message input can either be the tac string itself or the directory to the text file containing the SYNOP data.

This method generates BUFR4 file(s) in the local directory. The number of BUFR4 files generated is equivalent to the number of SYNOP messages input.

Example
-------

Suppose we have a text file named ``A_SMRO01YRBK211200_C_EDZW_20220321120500_12524785.txt`` containing 23 SYNOP messages from January 2023, with corresponding station metadata ``metadata.csv`` in our local directory. We can convert these to 23 BUFR files with the following code:

.. code-block:: python

    from synop2bufr import transform

    file = "A_SMRO01YRBK211200_C_EDZW_20220321120500_12524785.txt"

    metadata = "metadata.csv"

    transform(file, metadata, 2023, 1)

Conversion to a Python Dictionary
*********************************
A single SYNOP message can be converted to a Python dictionary in the following way:

.. code-block:: python

    convert_to_dict(SYNOP message, year, month)

where the SYNOP message **must** be a string *without* the terminating equals (=) sign, and the year/month must be an integer. This returns an array containing a single Python dictionary for the decoded message, as well as the number of section 3 and section 4 cloud groups detected [1]_.

.. [1] These are the replicated cloud groups of section 3 and section 4 of a SYNOP message. See the `WMO manual on FM-12 <https://library.wmo.int/doc_num.php?explnum_id=10235>`_ for more details.

Example
-------

Suppose we have the following SYNOP messages from January 2023:


.. code-block::

  AAXX 21121

  15015 02999 02501 10103 21090 39765 42952 57020 60001=

  15020 02997 23104 10130 21075 30177 40377 58020 60001 81041=

We can extract the 2nd SYNOP message by joining the section 0 part of the message (``AAXX 21121``) to the rest of the message, excluding the equals (=) sign:

.. code-block:: python

  from synop2bufr import convert_to_dict

  second_msg = """AAXX 21121
                  15020 02997 23104 10130 21075 30177 40377 58020 60001 81041"""

  convert_to_dict(second_msg, 2023, 1)

which will return (when pretty printed):

.. code-block::
    
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

*Note:* The dictionary returned always has the same keys, meaning that often there are many null items as these groups aren't present in the SYNOP message.

Notice that the example message does not contain section 3 nor section 4 groups, thus the number of such cloud groups detected is 0 in both outputs.

Message Extraction
******************

The remaining two methods provided by synop2bufr provide relatively basic functionality. These are ``message_extract`` and ``file_extract``, which as mentioned above are used to extract strings ready for conversion into a Python dictionary and subsequently BUFR files.

One can use ``message_extract`` in the following way:

.. code-block:: python

  message_extract(SYNOP message string)

which returns an array of strings, where each string is an individual SYNOP message (ready for the ``convert_to_dict`` method for example).

One can use ``file_extract`` in the following way:

.. code-block:: python

  file_extract(SYNOP message text file directory)

which returns the same array as ``message_extract`` would if provided the contents of the file, as well as the year and month determined by the file name.