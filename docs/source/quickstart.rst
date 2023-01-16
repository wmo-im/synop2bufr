.. _quickstart:

Quick start
===========

The synop2bufr Python module contains both a command line interface and an API to convert data
stored in the WMO FM-12 SYNOP data format to the WMO BUFR format.
For example, the command line interface reads in data from a text file, converts it to BUFR and writes out the data to the specified directory. e.g.:


.. code-block:: shell

   synop2bufr transform \
       --input <input-fm12.txt> \
       --metadata <station-metadata.csv> \
       --output <output-directory-path> \
       --year <year-of-observation> \
       --month <month-of-observation>

This command is explained in more detail below.

Command line interface
**********************

.. code-block:: shell

   synop2bufr transform \
       --input <input-fm12.txt> \
       --metadata <station-metadata.csv> \
       --output <output-directory-path> \
       --year <year-of-observation> \
       --month <month-of-observation>

Input FM-12 file (input-fm12.txt)
---------------------------------
The FM-12 input data format is described in the `WMO Manual on Codes, Volume I.1 <https://library.wmo.int/doc_num.php?explnum_id=10235>`__.
Each message in the input data file must be terminated with a equals (=) symbol as per the example data below:

.. code-block::

    AAXX 21121

    15015 02999 02501 10103 21090 39765 42952 57020 60001=

    15020 02997 23104 10130 21075 30177 40377 58020 60001 81041=

Input metadata file (station-metadata.csv)
------------------------------------------
Due to the limitations of the FM-12 SYNOP format additional metadata needs to be passed to
synop2bufr for inclusion in the BUFR data. The formatting of this file is the same as for the wis2box (see
`wis2box documentation <https://docs.wis2box.wis.wmo.int/en/latest/reference/running/station-metadata.html>`__ for further details).

The required columns are
    - `station_name`: name of the obsevring station
    - `wigos_station_identifier`: WIGOS station identifier for the station
    - `traditional_station_identifier`:




Output directory (output-directory-path)
----------------------------------------
This specifies the dircetory to write the output BUFR files to. One BUFR file per weather report / observation
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