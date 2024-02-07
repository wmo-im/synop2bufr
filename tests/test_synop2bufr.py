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
import logging
from synop2bufr import extract_individual_synop, parse_synop, transform

LOGGER = logging.getLogger(__name__)


@pytest.fixture
def multiple_reports_307080():
    return """AAXX 21120
15015 02999 02501 10103 21090 39765 42952 57020 60001=
15020 02997 23104 10130 21075 30177 40377 58020 60001 81041=
15090 02997 53102 10139 21075 30271 40364 58031 60001 82046=
    """


@pytest.fixture
def multiple_reports_307096():
    return """AAXX 21121
15015 02999 02501 10103 21090 39765 42952 57020 60001=
15020 02997 23104 10130 21075 30177 40377 58020 60001 81041=
15090 02997 53102 10139 21075 30271 40364 58031 60001 82046=
    """


@pytest.fixture
def single_report():
    return """AAXX 21121
15001 05515 32931 10103 21090 39765 42250 57020 60071 72006 82110 91155
 333 10178 21073 34101 55055 00010 20003 30002 50001 60004
 60035 70500 83145 81533 91008 91111
 444 18031 22053
    """


@pytest.fixture
def metadata_string():
    md = "station_name,wigos_station_identifier,traditional_station_identifier,facility_type,latitude,longitude,elevation,barometer_height,territory_name,wmo_region\n" + \
            "OCNA SUGATAG,0-20000-0-15015,15015,Land (fixed),47.77706163,23.94046026,503,504.43,Romania,6\n" + \
            "BOTOSANI,0-20000-0-15020,15020,Land (fixed),47.73565324,26.64555017,161,162.2,Romania,6\n" + \
            "IASI,0-20000-0-15090,15090,Land (fixed),47.16333333,27.62722222,74.29,75.69,Romania,6"  # noqa
    return md


def test_report_separation(multiple_reports_307080):
    # Extract each report
    msg_list = extract_individual_synop(multiple_reports_307080)
    assert len(msg_list) == 3
    # Assert each report has been extracted as intended
    assert msg_list[0] == "AAXX 21120 15015 02999 02501 10103 21090 39765 42952 57020 60001"  # noqa
    assert msg_list[1] == "AAXX 21120 15020 02997 23104 10130 21075 30177 40377 58020 60001 81041"  # noqa
    assert msg_list[2] == "AAXX 21120 15090 02997 53102 10139 21075 30271 40364 58031 60001 82046"  # noqa


def test_conversion(single_report):
    # Get the returned dictionary from the report, using a random
    # year and month
    d, num_s3_clouds, num_s4_clouds = parse_synop(single_report, 2000, 1)
    # We now need to check that most the dictionary items are what we expect
    assert d['station_id'] == "15001"
    assert d['day'] == 21
    assert d['hour'] == 11
    assert d['minute'] == 55
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
    assert d['maximum_temperature'] == 290.95
    assert d['minimum_temperature'] == 265.85
    assert d['maximum_temperature_period_start'] == -12
    assert d['maximum_temperature_period_end'] == 0
    assert d['minimum_temperature_period_start'] == -12
    assert d['minimum_temperature_period_end'] == 0
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
    assert d['cloud_amount_s3_1'] == 3
    assert d['cloud_genus_s3_1'] == 1
    assert d['cloud_height_s3_1'] == 1350
    assert d['cloud_amount_s3_2'] == 1
    assert d['cloud_genus_s3_2'] == 5
    assert d['cloud_height_s3_2'] == 990
    assert d['highest_gust_1'] == 8
    assert d['highest_gust_2'] == 11
    assert d['cloud_amount_s4_1'] == 1
    assert d['cloud_genus_s4_1'] == 8
    assert d['cloud_height_s4_1'] == 300
    assert d['cloud_top_s4_1'] == 1
    assert d['cloud_amount_s4_2'] == 2
    assert d['cloud_genus_s4_2'] == 2
    assert d['cloud_height_s4_2'] == 500
    assert d['cloud_top_s4_2'] == 3
    assert num_s3_clouds == 2
    assert num_s4_clouds == 2


def test_bufr_307080(multiple_reports_307080, metadata_string):
    result = transform(
        multiple_reports_307080, metadata_string, 2022, 3
    )
    msgs = {}
    for item in result:
        msgs[item['_meta']['id']] = item
    # Test the md5 keys
    assert msgs['WIGOS_0-20000-0-15015_20220321T120000']['_meta']['properties']['md5'] == '1e564e1ec2d679bbc120141ba031ab7a'  # noqa
    assert msgs['WIGOS_0-20000-0-15020_20220321T120000']['_meta']['properties']['md5'] == 'db62277233118df3f1cf7b6a073f1cbe'  # noqa
    assert msgs['WIGOS_0-20000-0-15090_20220321T120000']['_meta']['properties']['md5'] == '538db43645fb4b2459edfcb467048b7a'  # noqa

    # Test the bufr template used for all the reports
    # (they should be the same for every report)
    assert msgs['WIGOS_0-20000-0-15015_20220321T120000']['_meta']['template'] == 307080  # noqa
    assert msgs['WIGOS_0-20000-0-15020_20220321T120000']['_meta']['template'] == 307080  # noqa
    assert msgs['WIGOS_0-20000-0-15090_20220321T120000']['_meta']['template'] == 307080  # noqa


def test_bufr_307096(multiple_reports_307096, metadata_string):
    result = transform(
        multiple_reports_307096, metadata_string, 2022, 3
    )
    msgs = {}
    for item in result:
        msgs[item['_meta']['id']] = item
    # Test the md5 keys
    assert msgs['WIGOS_0-20000-0-15015_20220321T120000']['_meta']['properties']['md5'] == '5f1744ec26875630efca0e1583cddca9'  # noqa
    assert msgs['WIGOS_0-20000-0-15020_20220321T120000']['_meta']['properties']['md5'] == 'e2dc1199d4e38fae25d26ded815597da'  # noqa
    assert msgs['WIGOS_0-20000-0-15090_20220321T120000']['_meta']['properties']['md5'] == '7c352acb43530946f2445a95eb349e68'  # noqa

    # Test the bufr template used for all the reports
    # (they should be the same for every report)
    assert msgs['WIGOS_0-20000-0-15015_20220321T120000']['_meta']['template'] == 307096  # noqa
    assert msgs['WIGOS_0-20000-0-15020_20220321T120000']['_meta']['template'] == 307096  # noqa
    assert msgs['WIGOS_0-20000-0-15090_20220321T120000']['_meta']['template'] == 307096  # noqa


def test_invalid_separation():

    missing_delimiter = """AAXX 21121

15015 02999 02501 10103 21090 39765 42952 57020 60001

15020 02997 23104 10130 21075 30177 40377 58020 60001 81041

15090 02997 53102 10139 21075 30271 40364 58031 60001 82046"""

    with pytest.raises(Exception) as e:
        # Attempt to extract each report
        extract_individual_synop(missing_delimiter)
        assert str(
            e.value) == (
                        "Delimiters (=) are not present in the string,"
                        " thus unable to identify separate SYNOP reports."
                        )  # noqa


def test_no_type():

    missing_station_type = """21121
           15001 05515 32931 10103 21090
    39765 42250 57020 60071 72006 82110 91155="""

    with pytest.raises(Exception) as e:
        # Attempt to decode the report
        extract_individual_synop(
            missing_station_type)
        assert str(
            e.value) == "Invalid SYNOP message: AAXX could not be found."


def test_no_time():

    missing_time = """AAXX
           15001 05515 32931 10103 21090
    39765 42250 57020 60071 72006 82110 91155"""

    with pytest.raises(Exception) as e:
        # Attempt to decode the message
        parse_synop(missing_time, 2000, 1)
        assert str(
            e.value) == ("No SYNOP reports were extracted."
                         " Perhaps the date group YYGGiw"
                         " is missing.")


def test_no_tsi():

    missing_tsi = """AAXX 21121
           05515 32931 10103 21090
    39765 42250 57020 60071 72006 82110 91155="""

    with pytest.raises(Exception) as e:
        # Attempt to decode the message
        parse_synop(missing_tsi, 2000, 1)
        assert str(
            e.value) == ("Unexpected precipitation group"
                         " found in section 1, thus unable to"
                         " decode. Section 0 groups may be"
                         " missing.")


def test_dewpoint_qc(caplog):

    invalid_dewpoint = """AAXX 21121
    15015 05515 32931 10103 20111 39765 42250 57020 60071"""

    parse_synop(invalid_dewpoint, 2000, 1)

    # Check that the warning message is correct
    assert "Reported dewpoint temperature 284.25 is greater than the reported air temperature 283.45. Elements set to missing" in caplog.text  # noqa


def test_range_qc(metadata_string):

    out_of_range = """AAXX 21121
    15015 05515 32980 10610 21810 34765 42250 57020 66001="""

    result = transform(out_of_range, metadata_string, 2000, 1)

    for item in result:
        warning_msgs = item["_meta"]["result"]["warnings"]

    assert "#1#nonCoordinatePressure: Value (47650.0) out of valid range (50000 - 108000).; Element set to missing" in warning_msgs  # noqa
    assert "#1#airTemperature: Value (334.15) out of valid range (193.15 - 333.15).; Element set to missing" in warning_msgs  # noqa
    assert "#1#dewpointTemperature: Value (192.15) out of valid range (193.15 - 308.15).; Element set to missing" in warning_msgs  # noqa
    assert "#1#windSpeed: Value (80.0) out of valid range (0.0 - 75).; Element set to missing" in warning_msgs  # noqa
    assert "#1#totalPrecipitationOrTotalWaterEquivalent: Value (600.0) out of valid range (0.0 - 500).; Element set to missing" in warning_msgs  # noqa
