
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

import pytest
from synop2bufr import message_extract, convert_to_dict, to_json

# * Happy path tests


def test_message_separation():

    multiple_messages = """AAXX 21121

15015 02999 02501 10103 21090 39765 42952 57020 60001=

15020 02997 23104 10130 21075 30177 40377 58020 60001 81041=

15090 02997 53102 10139 21075 30271 40364 58031 60001 82046="""

    # Extract each message
    msg_list = message_extract(multiple_messages)

    # Assert each message has been extracted as intended

    assert msg_list[0] == "AAXX 21121 15015 02999 02501 10103 21090 39765 42952 57020 60001"

    assert msg_list[1] == "AAXX 21121 15020 02997 23104 10130 21075 30177 40377 58020 60001 81041"

    assert msg_list[2] == "AAXX 21121 15090 02997 53102 10139 21075 30271 40364 58031 60001 82046"


def test_conversion():

    single_message = """AAXX 21121 
15001 05515 32931 10103 21090 39765 42250 57020 60071 72006 82110 91155 
333 10178 21073 34101 55055 00010 20003 30002 50001 60004 60035 70500 83145 81533 91008 91111 
444 18031 22053"""

    # Get the returned dictionary from the message, using a random year and month
    d, num_s3_clouds, num_s4_clouds = convert_to_dict(single_message, 2000, 1)

    # We now need to check that most the dictionary items are what we expect
    assert d['station_id'] == "15001"
    assert d['day'] == 21
    assert d['wind_indicator'] == 8
    assert d['WMO_station_type'] == 0
    assert d['lowest_cloud_base'] == 600
    assert d['visibility'] == 1500
    assert d['cloud_cover'] == 38
    assert d['wind_direction'] == 290
    assert d['wind_speed'] == 31
    assert d['air_temperature'] == 283.45
    assert d['dewpoint_temperature'] == 264.15
    assert d['station_pressure'] == 97650
    assert d['isobaric_surface'] == 92500
    assert d['geopotential_height'] == 1250
    assert d['pressure_tendency_characteristic'] == 7
    assert d['3hr_pressure_change'] == -200
    assert d['precipitation_s1'] == 7
    assert d['ps1_time_period'] == -6
    assert d['present_weather'] == 20
    assert d['past_weather_1'] == 0
    assert d['past_weather_2'] == 6
    assert d['cloud_amount_s1'] == 2
    assert d['low_cloud_type'] == 31
    assert d['middle_cloud_type'] == 21
    assert d['high_cloud_type'] == 10
    assert d['hour'] == 11
    assert d['minute'] == 55
    assert d['maximum_temperature'] == 290.95
    assert d['minimum_temperature'] == 265.85
    assert d['ground_state'] == 4
    assert d['ground_temperature'] == 272.15
    assert d['sunshine_amount_24hr'] == 330
    assert d['net_radiation_24hr'] == 100000
    assert d['global_solar_radiation_24hr'] == 30000
    assert d['diffuse_solar_radiation_24hr'] == 20000
    assert d['long_wave_radiation_24hr'] == -10000
    assert d['short_wave_radiation_24hr'] == 40000
    assert d['precipitation_s3'] == 3
    assert d['ps3_time_period'] == -1
    assert d['precipitation_24h'] == 50
    assert d['cloud_amount_s3_0'] == 3
    assert d['cloud_genus_s3_0'] == 1
    assert d['cloud_height_s3_0'] == 1350
    assert d['cloud_amount_s3_1'] == 1
    assert d['cloud_genus_s3_1'] == 5
    assert d['cloud_height_s3_1'] == 990
    assert d['highest_gust_1'] == 8
    assert d['highest_gust_2'] == 11
    assert d['cloud_amount_s4_0'] == 1
    assert d['cloud_genus_s4_0'] == 8
    assert d['cloud_height_s4_0'] == 300
    assert d['cloud_top_s4_0'] == 1
    assert d['cloud_amount_s4_1'] == 2
    assert d['cloud_genus_s4_1'] == 2
    assert d['cloud_height_s4_1'] == 500
    assert d['cloud_top_s4_1'] == 3
    assert num_s3_clouds == 2
    assert num_s4_clouds == 2

# * Sad path tests


def test_invalid_separation():

    missing_delimiter = """AAXX 21121

15015 02999 02501 10103 21090 39765 42952 57020 60001

15020 02997 23104 10130 21075 30177 40377 58020 60001 81041

15090 02997 53102 10139 21075 30271 40364 58031 60001 82046"""

    with pytest.raises(Exception) as e:
        # Attempt to extract each message
        msg_list = message_extract(missing_delimiter)
    assert str(
        e.value) == "Delimiters (=) are not present in the string, thus unable to identify separate SYNOP messages."


def test_no_type():

    missing_station_type = """21121 
        15001 05515 32931 10103 21090 39765 42250 57020 60071 72006 82110 91155="""

    with pytest.raises(Exception) as e:
        # Attempt to decode the message
        result = to_json(missing_station_type)
        assert str(
            e.value) == "Invalid SYNOP message: AAXX could not be found."


def test_no_time():

    missing_time = """AAXX
        15001 05515 32931 10103 21090 39765 42250 57020 60071 72006 82110 91155="""

    with pytest.raises(Exception) as e:
        # Attempt to decode the message
        result = to_json(missing_time)
        assert str(
            e.value) == "Unexpected precipitation group found in section 1, thus unable to decode. Section 0 groups may be missing."


def test_no_tsi():

    missing_tsi = """AAXX 21121
        05515 32931 10103 21090 39765 42250 57020 60071 72006 82110 91155="""

    with pytest.raises(Exception) as e:
        # Attempt to decode the message
        result = to_json(missing_tsi)
        assert str(
            e.value) == "Unexpected precipitation group found in section 1, thus unable to decode. Section 0 groups may be missing."
