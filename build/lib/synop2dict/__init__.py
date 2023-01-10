###############################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
###############################################################################
from copy import deepcopy
from datetime import (date, datetime)
import logging
import math
import os
from pymetdecoder import synop
import re

__version__ = '0.1.dev0'

LOGGER = logging.getLogger(__name__)

# Enumerate the keys
_keys = ['station_type', 'year', 'month', 'day', 'hour', 'minute',
         'wind_indicator', 'station_id', 'region',
         'precipitation_indicator', 'weather_indicator',
         'lowest_cloud_base', 'visibility', 'cloud_cover',
         'wind_direction', 'wind_speed', 'air_temperature',
         'dewpoint_temperature', 'station_pressure', 'sea_level_pressure',
         '3_hour_pressure_change', 'precipitation_s1', 'present_weather',
         'past_weather', 'low_cloud_type', 'low_cloud_amount',
         'middle_cloud_type', 'high_cloud_type']

# Build the dictionary template
synop_template = dict.fromkeys(_keys)


def convert_to_dict(message, year, month):
    """This function uses Pymetdecoder to convert the SYNOP message, then strips values
        from this and converts units to output a dictionary ready to be
        converted to BUFR.

        Args:
            message (str): The message to be decoded.
            year (int): The assigned year of the message.
            month (int): The assigned month of the message.
    """

    # Get the full output decoded message from the Pymetdecoder package
    decode = synop.SYNOP().decode(message)

    # Get the template dictionary to be filled
    output = synop_template

    # !Convert and assign to the template dictionary

    # *The following do not need to be converted

    output['station_type'] = message[0:5]
    output['year'] = year
    output['month'] = month

    if 'obs_time' in decode.keys():
        output['day'] = decode['obs_time']['day']['value']
        output['hour'] = decode['obs_time']['hour']['value']

    # *The minute will be 00 unless specified by exact observation time
    if 'exact_obs_time' in decode.keys():
        output['minute'] = decode['exact_obs_time']['minute']['value']
    else:
        output['minute'] = 0

    if 'wind_indicator' in decode.keys():
        output['wind_indicator'] = decode['wind_indicator']['value']

    if 'station_id' in decode.keys():
        output['station_id'] = decode['station_id']['value']

    if 'region' in decode.keys():
        output['region'] = decode['region']['value']

    if 'precipitation_indicator' in decode.keys():
        output['precipitation_indicator'] = decode['precipitation_indicator']['value']

    if 'weather_indicator' in decode.keys():
        output['weather_indicator'] = decode['weather_indicator']['value']

    # *Lowest cloud base is already given in metres, but we specifically select the minimum value
    if 'lowest_cloud_base' in decode.keys():
        output['lowest_cloud_base'] = decode['lowest_cloud_base']['min']

    # *Visibility is already given in metres
    if 'visibility' in decode.keys():
        output['visibility'] = decode['visibility']['value']

    # *Cloud cover is given in oktas, which we convert to a percentage rounded up (see page 1139 of BUFR manual)
    if 'cloud_cover' in decode.keys():
        N_oktas = decode['cloud_cover']['value']
        # If the cloud cover is 9 oktas, this means the sky was obscured and we keep the value as None
        if N_oktas == 9:
            N_percentage = 113
        else:
            N_percentage = math.ceil((N_oktas / 8) * 100)
            output['cloud_cover'] = N_percentage

    # *Wind direction is already in degrees
    if 'wind_direction' in decode.keys():
        output['wind_direction'] = decode['wind_direction']['value']

    # *Wind speed is given in the units specified by 'wind_indicator', which we use to convert to m/s
    if 'wind_speed' in decode.keys():
        ff = decode['wind_speed']['value']

        # Find the units
        ff_unit = decode['wind_indicator']['unit']

        # If units are knots instead of m/s, convert it to knots
        if ff_unit == 'KT':
            ff *= 0.51444

        output['wind_speed'] = ff

    # *All temperatures are given in celcius, which we convert to kelvin and then round to 2dp
    if 'air_temperature' in decode.keys():
        output['air_temperature'] = round(
            decode['air_temperature']['value'] + 273.15, 2)
    if 'dewpoint_temperature' in decode.keys():
        output['dewpoint_temperature'] = round(
            decode['dewpoint_temperature']['value'] + 273.15, 2)

    # *Pressure is given in hPa, which we convert to Pa
    if 'station_pressure' in decode.keys():
        output['station_pressure'] = decode['station_pressure']['value'] * 100
    if 'sea_level_pressure' in decode.keys():
        output['sea_level_pressure'] = decode['sea_level_pressure']['value'] * 100
    if 'pressure_tendency' in decode.keys():
        output['3_hour_pressure_change'] = decode['pressure_tendency']['change']['value'] * 100

    # *Precipitation is given in mm, which we convert to m
    if 'precipitation_s1' in decode.keys():
        output['precipitation_s1'] = decode['precipitation_s1']['amount'] * 0.001

    # *What to do here?
    if 'present_weather' in decode.keys():
        output['present_weather'] = decode['present_weather']['value']
    if 'past_weather' in decode.keys():
        output['past_weather_1'] = decode['past_weather'][0]['value']
        output['past_weather_2'] = decode['past_weather'][1]['value']

    # *Cloud types are untouched
    if 'cloud_types' in decode.keys():
        output['low_cloud_type'] = decode['cloud_types']['low_cloud_type']['value']
        output['middle_cloud_type'] = decode['cloud_types']['middle_cloud_type']['value']
        output['high_cloud_type'] = decode['cloud_types']['high_cloud_type']['value']

        # *Low cloud amount is given in oktas, which we convert to a percentage rounded up (see page 1139 of BUFR manual)
        N_oktas = decode['cloud_types']['low_cloud_amount']['value']
        # If the cloud cover is 9 oktas, this means the sky was obscured and we keep the value as None
        if N_oktas == 9:
            N_percentage = 113
        else:
            N_percentage = math.ceil((N_oktas / 8) * 100)
            output['cloud_cover'] = N_percentage

    # !Return the new dictionary
    return output


def file_extract(file):
    """This function extracts the contents of the file and the date of the file

    Args:
        file (str): The file directory or file name of the SYNOP message.
    """

    # Open and read the file, stripping any new lines
    try:
        with open(file, "r") as fp:
            data = fp.read()
    except:
        return "Error: The file path is incorrect."

    # Obtain the year and month of the data from the file name
    file_name = os.path.basename(file)
    file_year, file_month = get_date_from_filename(file_name)

    # Obtain the individual SYNOP messages from the file contents
    messages = message_extract(data)

    # Return the list of messages and the date of the file
    return messages, file_year, file_month


def message_extract(data):
    """This function separates the SYNOP tac and returns the individual SYNOP messages, ready for conversion
    Args:
        data (str): The SYNOP tac.
    """

    # check for abbreviated header line TTAAii etc.

    # now split based as section 0 of synop, beginning AAXX YYGGi_w
    start_position = data.find("AAXX")
    headers = data[0:start_position]
    data = re.split('(AAXX [0-9]{5})', data[start_position:])
    data = data[1:]  # drop first null element
    # iterate over messages processing
    messages = []
    for d in data:
        s1 = None
        if "AAXX" in d:
            s0 = d
        else:
            d = re.sub(r"\n+", " ", d)
            _messages = d.split("=")
            num_msg = len(_messages)
            for idx in range(num_msg):
                if len(_messages[idx]) > 0:
                    _messages[idx] = re.sub(
                        r"\s+", " ", f"{s0} {_messages[idx]}")
                else:
                    _messages[idx] = None
            messages.extend(_messages)
    # Return the messages
    return messages


def get_date_from_filename(name):
    """This function checks whether the input file name conforms to
        the standards, and if so returns the datetime of the file, otherwise defaults
        to returning the current year and month.

    Args:
        name (str): The file path basename.

    Returns:
        datetime: The datetime of the file.
    """

    # File format is: pflag_productidentifier_oflag_originator_yyyyMMddhhmmss.extension
    try:
        # Returns the part of the string that should be the datetime of the file
        # (begins with an underscore, but doesn't end with one)
        # Note: \d represents the decimal part, {8} means it checks for 8 digits
        # the condition is failed.
        match = re.search(r"_(\d{8})", name)
        # Strip the datetime from the part of the string
        d = datetime.strptime(match.group(1), '%Y%m%d')
        year = d.year
        month = d.month
        return year, month

    except ValueError:
        LOGGER.error(
            f"File {name} is in wrong file format. The current year and month will be used for the conversion.")
        year = date.today().year
        month = date.today().month
        return year, month


def to_json(input):
    """This function determines whether the input is raw SYNOP tac data or a file. If the former,
    it converts the SYNOP messages into a dictionary directly with the current year and month. If the latter, it extracts
    each SYNOP message as well as the year and month from the file name, and then converts the messages.

    Args:
        input (str): This string is either the raw SYNOP data or the path directory to the file.
    """

    # *Begin by checking if the input is the SYNOP messages or the file directory
    if os.path.isdir(input) or os.path.isfile(input):
        messages, year, month = file_extract(input)
    else:
        # Extract the message and default to the current year and month
        messages = message_extract(input)
        year = date.today().year
        month = date.today().month

    # *Now convert each message into a dictionary and append each dictionary to a total dictionary named result
    result = {}
    for msg in messages:
        LOGGER.debug(msg)
        if msg is not None:
            new_dict = convert_to_dict(msg, year, month)
            key = new_dict['station_id']
            if key in result:
                LOGGER.error(f"Key {key} already found in output dictionary")
                LOGGER.error(result[key])
                assert False
            result[key] = deepcopy(new_dict)

    return result
