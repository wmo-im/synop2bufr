# SYNOP2BUFR Training
___
# Learning outcomes

- Understand the structure and need for a station metadata csv file.
- Ability to convert a SYNOP message into BUFR.
- Understand the relationship between the number of SYNOP reports and the number of BUFR files produced.
- Ability to inspect the contents of the BUFR files created.


# Essentials

Before attempting the questions below, here are some essential commands that will be helpful:

## Transform
The `transform` function is what will convert a given SYNOP message to BUFR, and can be used in the following way in the command line:

```
synop2bufr transform --metadata my_file.csv --output-dir ./my_folder --year message_year --month message_month my_SYNOP.txt
```

Note that if the metadata, output direction, year and month options are not specified, they will assume their default values:

| Option      | Default |
| ----------- | ----------- |
| --metadata | metadata.csv |
| --output-dir | The current working directory. |
| --year | The current year. |
| --month | The current month. |

In the examples, the year and month are not given, so feel free to specify a date yourself or use the default values.

## BUFR Dump
The `bufr_dump` function will allow you to inspect the BUFR files created from the conversion. It has many options, but the following will be the most applicable to the exercises:

```
bufr_dump -p my_bufr.bufr4
```
This will enumerate the content of your BUFR on screen. If you are interested in the values taken by a variable in particular, you can use the `grep` command. For example:

```
bufr_dump -p my_bufr.bufr4 |grep -i 'temperature'
```
This will enumerate the variables related to temperature in your BUFR file. If you want to do this for multiple types of variables, you can use the `\|` command. For example:
```
bufr_dump -p my_bufr.bufr4 |grep -i 'temperature\|wind'
```

# Exercises

## Ex. 1

1. Open the SYNOP file `ex_1_one_report.txt`. How many SYNOP reports are in this file?
1. Open the station metadata file `ex_1_station_list.csv`. How many stations are listed in this file?
1. Convert `ex_1_one_report.txt` to BUFR.
1. Use BUFR Dump to check the latitude and longitude value stored in the output BUFR file. Verify these values using the station metadata file.

## Ex. 2

1. Open the SYNOP file `ex_2_many_reports.txt`. How many SYNOP reports are in this file?
1. Open the station metadata file `ex_2_station_list.csv`. How many stations are listed in this file?
1. Convert `ex_2_many_reports.txt` to BUFR.
1. Based on the results of this and the previous exercise, how could you predict the number of output BUFR files based upon the number of SYNOP reports and stations listed in the metadata file?
1. Use BUFR Dump to check each of the output BUFR files have different WIGOS metadata.

## Ex. 3

1. Open the SYNOP file `ex_3_long_report.txt`. You should notice this file only contains 1, longer SYNOP report with more sections. Now open `ex_3_station_list.csv`. Is it a problem that this file contains more stations than there are SYNOP reports?\
   (_Hint: The station list file is just a metadata file for SYNOP2BUFR to find out more information than SYNOP alone can encode._)

1. Convert `ex_3_long_msg.txt` to BUFR.
1. Use BUFR Dump to find the:
   * Air temperature (K) of the report.
   * Total cloud cover (%) of the report.
   * Total period of sunshine (mins) of the report.
   * Wind speed (m/s) of the report.

## Ex. 4

1. Open the SYNOP file `ex_4_incorrect.txt`. What is incorrect about this SYNOP file?
1. Attempt to convert `ex_4_incorrect.txt` using the station list file `ex_4_station_list.csv`.

## Ex. 5

1. Open the SYNOP file `ex_5_many_reports.txt` and station list file `ex_5_station_list_missing.csv`. What is missing in the station list?
1. Attempt to convert `ex_5_many_reports.txt` to BUFR. What error is presented?
1. Considering the error presented, justify the number of BUFR files produced.

