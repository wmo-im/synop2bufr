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
import csv
from copy import deepcopy
from datetime import (date, datetime, timezone)
from io import StringIO
import json
import logging
import math
import os
import re

from pymetdecoder import synop
from csv2bufr import BUFRMessage

__version__ = '0.1.dev1'

LOGGER = logging.getLogger(__name__)

# Enumerate the keys
_keys = ['report_type', 'year', 'month', 'day', 'hour', 'minute',
         'wind_indicator', 'block_no', 'station_no', 'station_id', 'region',
         'WMO_station_type',
         'lowest_cloud_base', 'visibility', 'cloud_cover',
         'time_significance', 'wind_time_period',
         'wind_direction', 'wind_speed', 'air_temperature',
         'dewpoint_temperature', 'relative_humidity', 'station_pressure',
         'isobaric_surface', 'geopotential_height', 'sea_level_pressure',
         '3hr_pressure_change', 'pressure_tendency_characteristic',
         'precipitation_s1', 'ps1_time_period', 'present_weather',
         'past_weather_1', 'past_weather_2', 'past_weather_time_period',
         'cloud_vs_s1', 'cloud_amount_s1', 'low_cloud_type',
         'middle_cloud_type', 'high_cloud_type',
         'maximum_temperature', 'minimum_temperature',
         'ground_state', 'ground_temperature', 'snow_depth',
         'evapotranspiration', 'evaporation_instrument',
         'temperature_change', 'tc_time_period',
         'sunshine_amount_1hr', 'sunshine_amount_24hr',
         'low_cloud_drift_direction', 'low_cloud_drift_vs',
         'middle_cloud_drift_direction', 'middle_cloud_drift_vs',
         'high_cloud_drift_direction', 'high_cloud_drift_vs',
         'e_cloud_genus', 'e_cloud_direction',
         'e_cloud_elevation', '24hr_pressure_change',
         'net_radiation_1hr', 'net_radiation_24hr',
         'global_solar_radiation_1hr', 'global_solar_radiation_24hr',
         'diffuse_solar_radiation_1hr', 'diffuse_solar_radiation_24hr',
         'long_wave_radiation_1hr', 'long_wave_radiation_24hr',
         'short_wave_radiation_1hr', 'short_wave_radiation_24hr',
         'net_short_wave_radiation_1hr', 'net_short_wave_radiation_24hr',
         'direct_solar_radiation_1hr', 'direct_solar_radiation_24hr',
         'precipitation_s3', 'ps3_time_period', 'precipitation_24h',
         'highest_gust_1', 'highest_gust_2', 'hg2_time_period']

# Build the dictionary template
synop_template = dict.fromkeys(_keys)

THISDIR = os.path.dirname(os.path.realpath(__file__))
MAPPINGS = f"{THISDIR}{os.sep}resources{os.sep}synop-mappings.json"

# Load template mappings file, this will be updated for each message.
with open(MAPPINGS) as fh:
    _mapping = json.load(fh)


def convert_to_dict(message: str, year: int, month: int) -> dict:
    """
    This function parses a SYNOP message, storing and returning the
    example_data as a python dictionary.

    :param message: String containing the SYNOP message to be decoded
    :param year: Int value of the corresponding year for the SYNOP messsage
    :param month: Int value of the corresponding month for the SYNOP messsage

    """

    # Get the full output decoded message from the Pymetdecoder package
    try:
        decode = synop.SYNOP().decode(message)
    except Exception as e:
        LOGGER.error("Unable to decode the SYNOP message.")
        raise e

    # Get the template dictionary to be filled
    output = deepcopy(synop_template)

    # SECTIONs 0 AND 1

    # The following do not need to be converted
    output['report_type'] = message[0:4]
    output['year'] = year
    output['month'] = month

    if 'obs_time' in decode.keys():
        output['day'] = decode['obs_time']['day']['value']
        output['hour'] = decode['obs_time']['hour']['value']

    # The minute will be 00 unless specified by exact observation time
    if 'exact_obs_time' in decode.keys():
        output['minute'] = decode['exact_obs_time']['minute']['value']
        # Overwrite the hour, because the actual observation may be from
        # the hour before but has been rounded in the YYGGiw group
        output['hour'] = decode['exact_obs_time']['hour']['value']
    else:
        output['minute'] = 0

    # Translate wind instrument flag from the SYNOP code to the BUFR code
    if 'wind_indicator' in decode.keys():
        iw = decode['wind_indicator']['value']

        # In this conversion, we convert bit number to a value (see code table
        # 0 02 002)  # noq
        # Not bit 3 should never be set for synop, units of km/h not reportable
        if iw == 0:
            iw_translated = 0b0000  # Wind in m/s, default, no bits set
        elif iw == 1:
            iw_translated = 0b1000 # Wind in m/s with anemometer bit 1 (left most) set  # noqa
        elif iw == 3:
            iw_translated = 0b0100  # Wind in knots, bit 2 set
        elif iw == 4:
            iw_translated = 0b1100  # Wind in knots with anemometer, bits
            # 1 and 2 set # noq
        else:
            iw_translated = 0b1111  # Missing value
    else:
        iw_translated = 0b1111  # Missing value

    output['wind_indicator'] = iw_translated

    if 'station_id' in decode.keys():
        tsi = decode['station_id']['value']
        output['station_id'] = tsi
        output['block_no'] = tsi[0:2]
        output['station_no'] = tsi[2:5]

    # ! Removed region and precipitation indicator as they are redundant

    # We translate this station type flag from the SYNOP code to the BUFR code
    if 'weather_indicator' in decode.keys():
        ix = decode['weather_indicator']['value']
        if ix <= 3:
            ix_translated = 1  # Manned station
        elif ix == 4:
            ix_translated = 2  # Hybrid station
        elif ix > 4 and ix <= 7:
            ix_translated = 0  # Automatic station
        else:
            ix_translated = 3  # Missing value
    else:
        ix_translated = 3  # Missing value

    output['WMO_station_type'] = ix_translated

    # Lowest cloud base is already given in metres, but we specifically select
    # the minimum value  # noq
    # NOTE: By B/C1.4.4.4 the precision of this value is in tens of metres
    if 'lowest_cloud_base' in decode.keys() and \
            decode['lowest_cloud_base'] is not None:
        output['lowest_cloud_base'] = \
            round(decode['lowest_cloud_base']['min'], -1)

    # Visibility is already given in metres
    if 'visibility' in decode.keys():
        output['visibility'] = decode['visibility']['value']

    # Cloud cover is given in oktas, which we convert to a percentage
    #  NOTE: By B/C10.4.4.1 this percentage is always rounded up
    if 'cloud_cover' in decode.keys():
        N_oktas = decode['cloud_cover']['_code']
        # If the cloud cover is 9 oktas, this means the sky was obscured and
        # we keep the value as None
        if N_oktas == 9:
            N_percentage = 113
        else:
            N_percentage = math.ceil((N_oktas / 8) * 100)
            output['cloud_cover'] = N_percentage

    # Wind direction is already in degrees
    if 'surface_wind' in decode.keys():
        # See B/C1.10.5.3
        # NOTE: Every time period in the following code shall be a negative number,  # noqa
        # to indicate measurements have been taken up until the present.
        output['time_significance'] = 2
        output['wind_time_period'] = -10

        if decode['surface_wind']['direction'] is not None:
            output['wind_direction'] = \
                decode['surface_wind']['direction']['value']

        # Wind speed in units specified by 'wind_indicator', convert to m/s
        if decode['surface_wind']['speed'] is not None:
            ff = decode['surface_wind']['speed']['value']

            # Find the units
            ff_unit = decode['wind_indicator']['unit']

            # If units are knots instead of m/s, convert it to knots
            if ff_unit == 'KT':
                ff *= 0.51444

            output['wind_speed'] = ff

    # Temperatures are given in Celsius, convert to kelvin and round to 2 dp
    if 'air_temperature' in decode.keys():
        output['air_temperature'] = round(
            decode['air_temperature']['value'] + 273.15, 2)
    if 'dewpoint_temperature' in decode.keys():
        output['dewpoint_temperature'] = round(
            decode['dewpoint_temperature']['value'] + 273.15, 2)

    # RH is already given in %
    if 'relative_humidity' in decode.keys():
        output['relative_humidity'] = decode['relative_humidity']
    else:
        # if RH is missing estimate from air temperature and dew point
        # temperature
        #
        # Reference to equation / method required

        A = output.get('air_temperature')
        D = output.get('dewpoint_temperature')

        if None in (A, D):
            output['relative_humidity'] = None
        else:
            A -= 273.15
            D -= 273.15

            beta = 17.625
            lam = 243.04

            U = 100 * math.exp(((beta*D)/(lam+D)) - ((beta*A)/(lam+A)))

            output['relative_humidity'] = U

    # Pressure is given in hPa, which we convert to Pa. By B/C 1.3.1,
    # pressure has precision in tens of Pa
    if 'station_pressure' in decode.keys():
        output['station_pressure'] = round(
            decode['station_pressure']['value'] * 100, -1)

    #  Similar to above. By B/C1.3.2, pressure has precision in tens of Pa
    if 'sea_level_pressure' in decode.keys():
        output['sea_level_pressure'] = round(
            decode['sea_level_pressure']['value'] * 100, -1)

    if 'geopotential' in decode.keys():
        output['isobaric_surface'] = round(
            decode['geopotential']['surface']['value'] * 100, 1)
        output['geopotential_height'] = \
            decode['geopotential']['height']['value']

    if 'pressure_tendency' in decode.keys():
        #  By B/C1.3.3, pressure has precision in tens of Pa
        output['3hr_pressure_change'] = round(
            decode['pressure_tendency']['change']['value'] * 100, -1)
        output['pressure_tendency_characteristic'] = \
            decode['pressure_tendency']['tendency']['value']
    else:
        output['pressure_tendency_characteristic'] = 15  # Missing value

    # Precipitation is given in mm, which is equal to kg/m^2 of rain
    if 'precipitation_s1' in decode.keys():
        # NOTE: When the precipitation measurement RRR has code 990, this
        # represents a trace amount of rain
        # (<0.01 inches), which pymetdecoder records as 0. I agree with
        # this choice, and so no change has been made.
        output['precipitation_s1'] = \
            decode['precipitation_s1']['amount']['value']
        output['ps1_time_period'] = -1 * \
            decode['precipitation_s1']['time_before_obs']['value']

    # The present and past weather SYNOP codes align with that of BUFR apart
    # from missing values
    if 'present_weather' in decode.keys():
        output['present_weather'] = decode['present_weather']['value']
    else:
        output['present_weather'] = 511  # Missing value

    if 'past_weather' in decode.keys():

        if decode['past_weather']['past_weather_1'] is not None:
            output['past_weather_1'] = \
                decode['past_weather']['past_weather_1']['value']
        else:
            output['past_weather_1'] = 31  # Missing value

        if decode['past_weather']['past_weather_2'] is not None:
            output['past_weather_2'] = \
                decode['past_weather']['past_weather_2']['value']

        else:
            output['past_weather_2'] = 31  # Missing value

    else:  # Missing values
        output['past_weather_1'] = 31
        output['past_weather_2'] = 31

    #  The past weather time period is determined by the hour of observation,
    #  as per B/C1.10.1.8.1
    hr = output['hour']

    # NOTE: All time periods must be negative
    if hr % 6 == 0:
        output['past_weather_time_period'] = -6
    elif hr % 3 == 0:
        output['past_weather_time_period'] = -3
    elif hr % 2 == 0:
        output['past_weather_time_period'] = -2
    else:
        output['past_weather_time_period'] = -1

    # We translate these cloud type flags from the SYNOP codes to the
    # BUFR codes
    if 'cloud_types' in decode.keys():

        Cl = decode['cloud_types']['low_cloud_type']['value']
        Cl_translated = Cl + 30
        output['low_cloud_type'] = Cl_translated

        Cm = decode['cloud_types']['middle_cloud_type']['value']
        Cm_translated = Cm + 20
        output['middle_cloud_type'] = Cm_translated

        Ch = decode['cloud_types']['high_cloud_type']['value']
        Ch_translated = Ch + 10
        output['high_cloud_type'] = Ch_translated

        if 'low_cloud_amount' in decode['cloud_types'].keys():
            # Low cloud amount is given in oktas, and by B/C1.4.4.3.1 it
            # stays that way for BUFR
            N_oktas = decode['cloud_types']['low_cloud_amount']['value']
            # If the cloud cover is 9 oktas, this means the sky was obscured
            # and we keep the value as None
            if N_oktas == 9:
                # By B/C1.4.4.2, if sky obscured, use significance code 5
                output['cloud_vs_s1'] = 5
            else:
                # By B/C1.4.4.2, if low clouds present, use significance code 7
                output['cloud_vs_s1'] = 7
                output['cloud_amount_s1'] = N_oktas

        elif 'middle_cloud_amount' in decode['cloud_types'].keys():
            # Middle cloud amount is given in oktas, and by B/C1.4.4.3.1 it
            # stays that way for BUFR
            N_oktas = decode['cloud_types']['middle_cloud_amount']['value']
            # If the cloud cover is 9 oktas, this means the sky was obscured
            # and we keep the value as None
            if N_oktas == 9:
                # By B/C1.4.4.2, if sky obscured, use significance code 5
                output['cloud_vs_s1'] = 5
            else:
                # By B/C1.4.4.2, only middle clouds present, use significance
                # code 8
                output['cloud_vs_s1'] = 8
                output['cloud_amount_s1'] = N_oktas

        # According to B/C1.4.4.3.1, if only high clouds present, cloud amount
        # and significance code will be set to 0
        elif decode['cloud_types']['high_cloud_type'] is not None:
            output['cloud_vs_s1'] = 0
            output['cloud_amount_s1'] = 0

        # According to B/C1.4.4.3.1, if no clouds present, use significance
        # code  62
        else:
            output['cloud_vs_s1'] = 62
            output['cloud_amount_s1'] = 0

    else:  # Missing values
        output['cloud_vs_s1'] = 63
        output['low_cloud_type'] = 63
        output['middle_cloud_type'] = 63
        output['high_cloud_type'] = 63

    #  Now, some of the above cloud information may be different if the
    # overall cloud cover
    #  (N in group Nddff) is recorded as 0. This is because if it is confirmed
    # that no clouds are present, then the remaining cloud
    # information is automatic

    if output['cloud_cover'] == 0:
        # Overwrite the above in the case of no clouds
        output['cloud_vs_s1'] = 62
        output['cloud_amount_s1'] = 0
        output['lowest_cloud_base'] = None
        output['low_cloud_type'] = 30
        output['middle_cloud_type'] = 20
        output['high_cloud_type'] = 10

    # ! SECTION 3

    #  Group 1 1snTxTxTx - gives maximum temperature over a time period
    # decided by the region
    if ('maximum_temperature' in decode.keys() and
            decode['maximum_temperature'] is not None):
        #  Convert to Kelvin
        output['maximum_temperature'] = round(
            decode['maximum_temperature']['value'] + 273.15, 2)

    #  Group 2 2snTnTnTn - gives minimum temperature over a time period
    # decided by the region
    if ('minimum_temperature' in decode.keys() and
            decode['minimum_temperature'] is not None):
        #  Convert to Kelvin
        output['minimum_temperature'] = round(
            decode['minimum_temperature']['value'] + 273.15, 2)
    #  Group 3 3Ejjj
    # NOTE: According to SYNOP manual 12.4.5, the group is
    # developed regionally.
    # This regional difference is as follows:
    # It is either omitted, or it takes form 3EsnTgTg, where
    # Tg is the ground temperature
    if 'ground_state' in decode.keys():

        if decode['ground_state']['state'] is not None:
            output['ground_state'] = decode['ground_state']['state']['value']
            print(decode['ground_state'])
        else:
            # By B/C1 a missing value has code 31
            output['ground_state'] = 31

        if decode['ground_state']['temperature'] is not None:
            #  Convert to Kelvin
            output['ground_temperature'] = round(
                decode['ground_state']['temperature']['value'] + 273.15, 2)

    #  Group 4 4E'sss - gives state of the ground with snow, and the snow
    # depth (not regional like group 3 is)
    if 'ground_state_snow' in decode.keys():

        if decode['ground_state_snow']['state'] is not None:
            # We translate the snow depth flags from the SYNOP codes to the
            # BUFR codes
            E = decode['ground_state_snow']['state']['value']
            E_translated = E + 10
            output['ground_state'] = E_translated

        else:  # Missing value
            output['ground_state'] = 31

        # Snow depth is given in cm but should be recorded in m
        snow_depth = decode['ground_state_snow']['depth']['depth']
        output['snow_depth'] = snow_depth * 0.01

    #  We now look at group 5, 5j1j2j3j4, which can take many different forms
    # and also have
    #  supplementary groups for radiation measurements

    # Evaporation 5EEEiE
    if 'evapotranspiration' in decode.keys():

        # Evapotranspiration is given in mm, which is equal to kg/m^2 for rain
        output['evapotranspiration'] = \
            decode['evapotranspiration']['amount']['value']

        if decode['evapotranspiration']['type'] is not None:
            output['evaporation_instrument'] = \
                decode['evapotranspiration']['type']['_code']

        else:
            # Missing value
            output['evaporation_instrument'] = 15

    # Temperature change 54g0sndT
    if 'temperature_change' in decode.keys():

        if decode['temperature_change']['time_before_obs'] is not None:
            output['tc_time_period'] = -1 * \
                decode['temperature_change']['time_before_obs']['value']

        if decode['temperature_change']['change'] is not None:
            # We do not correct this measurement, as the temperature change is
            # the same in both degC and K
            output['temperature_change'] = \
                decode['temperature_change']['change']['value']

    # Sunshine amount 55SSS (24hrs) and 553SS (1hr)
    if ('sunshine' in decode.keys() and
            decode['sunshine']['amount'] is not None):

        # The time period remains in hours
        sun_time = decode['sunshine']['duration']['value']

        if sun_time == 1:
            # Sunshine amount should be given in minutes
            output['sunshine_amount_1hr'] = \
                decode['sunshine']['amount']['value'] * 60

        elif sun_time == 24:
            # Sunshine amount should be
            # given in minutes
            output['sunshine_amount_24hr'] = \
                decode['sunshine']['amount']['value'] * 60

    # Cloud drift example_data 56DLDMDH
    #  By B/C1.6.2 we must convert the direction to a degree bearing
    def to_bearing(direction):
        # Between NE and NW
        if direction < 8:
            return direction * 45
        # N
        if direction == 8:
            return 0

    if 'cloud_drift_direction' in decode.keys():
        if decode['cloud_drift_direction']['low'] is not None:
            low_dir = decode['cloud_drift_direction']['low']['_code']

            # NOTE: If direction code is 0, the clouds are stationary or
            # there are no clouds.
            # If direction code is 0, the direction is unknown or the clouds
            # or invisible.
            # In both cases, I believe no BUFR entry should be made.
            if low_dir > 0 and low_dir < 9:
                output['low_cloud_drift_direction'] = to_bearing(low_dir)

        if decode['cloud_drift_direction']['middle'] is not None:
            middle_dir = decode['cloud_drift_direction']['middle']['_code']
            if middle_dir > 0 and middle_dir < 9:
                output['middle_cloud_drift_direction'] = to_bearing(middle_dir)

        if decode['cloud_drift_direction']['high'] is not None:
            high_dir = decode['cloud_drift_direction']['high']['_code']
            if high_dir > 0 and high_dir < 9:
                output['high_cloud_drift_direction'] = to_bearing(high_dir)

    # Direction and elevation angle of the clouds 57CDaeC
    if 'cloud_elevation' in decode.keys():
        if decode['cloud_elevation']['genus'] is not None:
            output['e_cloud_genus'] = \
                decode['cloud_elevation']['genus']['_code']
        else:
            # Missing value
            output['e_cloud_genus'] = 63

        if decode['cloud_elevation']['direction'] is not None:
            e_dir = decode['cloud_elevation']['direction']['_code']

            # NOTE: If direction code is 0, the clouds are stationary or there
            # are no clouds.
            # If direction code is 0, the direction is unknown or the clouds
            # or invisible.
            # In both cases, I believe no BUFR entry should be made.
            if e_dir > 0 and e_dir < 9:
                # We reuse the to_bearing function from above
                output['e_cloud_direction'] = to_bearing(e_dir)

        if decode['cloud_elevation']['elevation'] is not None:
            if decode['cloud_elevation']['elevation']['value'] is not None:
                # The elevation is already given in degrees
                output['e_cloud_elevation'] = \
                    decode['cloud_elevation']['elevation']['value']

    # Positive 58p24p24p24 or negative 59p24p24p24 changes in surface pressure
    # over 24hrs
    if 'pressure_change' in decode.keys():
        if decode['pressure_change'] is not None:
            #  SYNOP has units hPa. By B/C1.3.4, pressure change is given in
            # tens of Pa
            output['24hr_pressure_change'] = round(
                decode['pressure_change']['value']*100, -1)

    # Radiation supplementary information - the following radiation types are:
    # 1) Positive net radiation
    # 2) Negative net radiation
    # 3) Global solar radiation
    # 4) Diffused solar radiation
    # 5) Direct solar radiation
    # 6) Downward long-wave radiation
    # 7) Upward long-wave radiation
    # 8) Short wave radiation
    # NOTE: If the radiation is over the past hour, it is given in kJ/m^2.
    # If it is over the past 24 hours, it is given in J/cm^2.
    # In either case, B/C1.12.2 requires that all radiation measurements
    # are given in J/m^2. We convert this here.

    if 'radiation' in decode.keys():

        rad_dict = decode['radiation']

        # Create a function to do the appropriate conversion depending
        # on time period
        def rad_convert(rad, time):
            if time == 1:
                # 1 kJ/m^2 = 1000 J/m^2
                return 1000 * rad
            elif time == 24:
                # 1 J/cm^2 = 10000 J/m^2
                return 10000 * rad

        if 'positive_net' in rad_dict.keys():
            if rad_dict['positive_net']['value'] is not None:
                rad = rad_dict['positive_net']['value']
                time = rad_dict['positive_net']['time_before_obs']['value']

                if time == 1:
                    #  Set positive and convert to J/m^2, rounding to 1000s
                    # of J/m^2 (B/C1.12.2)
                    output['net_radiation_1hr'] = \
                        round(rad_convert(rad, time), -3)
                elif time == 24:
                    #  Set positive and convert to J/m^2, rounding to 1000s
                    # of J/m^2 (B/C1.12.2)
                    output['net_radiation_24hr'] = \
                        round(rad_convert(rad, time), -3)

        if 'negative_net' in rad_dict.keys():
            if rad_dict['negative_net']['value'] is not None:
                rad = rad_dict['negative_net']['value']
                time = rad_dict['negative_net']['time_before_obs']['value']

                if time == 1:
                    #  Set negative and convert to J/m^2,rounding to 1000s
                    # of J/m^2 (B/C1.12.2)
                    output['net_radiation_1hr'] = \
                        -1*round(rad_convert(rad, time), -3)
                if time == 24:
                    #  Set negative and convert to J/m^2,rounding to 1000s
                    # of J/m^2 (B/C1.12.2)
                    output['net_radiation_24hr'] = \
                        -1*round(rad_convert(rad, time), -3)

        if 'global_solar' in rad_dict.keys():
            if rad_dict['global_solar']['value'] is not None:
                rad = rad_dict['global_solar']['value']
                time = rad_dict['global_solar']['time_before_obs']['value']

                if time == 1:
                    #  Convert to J/m^2,rounding to 100s
                    # of J/m^2 (B/C1.12.2)
                    output['global_solar_radiation_1hr'] = \
                        round(rad_convert(rad, time), -2)
                elif time == 24:
                    #  Convert to J/m^2,rounding to 100s of J/m^2 (B/C1.12.2)
                    output['global_solar_radiation_24hr'] = \
                        round(rad_convert(rad, time), -2)

        if 'diffused_solar' in rad_dict.keys():
            if rad_dict['diffused_solar']['value'] is not None:
                rad = rad_dict['diffused_solar']['value']
                time = rad_dict['diffused_solar']['time_before_obs']['value']

                if time == 1:
                    #  Set Convert to J/m^2,rounding to 100s
                    # of J/m^2 (B/C1.12.2)
                    output['diffuse_solar_radiation_1hr'] = \
                        round(rad_convert(rad, time), -2)

                if time == 24:
                    #  Set Convert to J/m^2,rounding to 100s
                    # of J/m^2 (B/C1.12.2)
                    output['diffuse_solar_radiation_24hr'] = \
                        round(rad_convert(rad, time), -2)

        if 'downward_long_wave' in rad_dict.keys():
            if rad_dict['downward_long_wave']['value'] is not None:
                rad = rad_dict['downward_long_wave']['value']
                time = \
                    rad_dict['downward_long_wave']['time_before_obs']['value']

                if time == 1:
                    #  Set positive and convert to J/m^2,rounding to 10000s
                    # of J/m^2 (B/C1.12.2)
                    output['long_wave_radiation_1hr'] = \
                        round(rad_convert(rad, time), -4)
                if time == 24:
                    #  Set positive and convert to J/m^2,rounding to 10000s
                    # of J/m^2 (B/C1.12.2)
                    output['long_wave_radiation_24hr'] = \
                        round(rad_convert(rad, time), -4)

        if 'upward_long_wave' in rad_dict.keys():
            if rad_dict['upward_long_wave']['value'] is not None:
                rad = rad_dict['upward_long_wave']['value']
                time = rad_dict['upward_long_wave']['time_before_obs']['value']

                if time == 1:
                    #  Set negative and convert to J/m^2,rounding to 10000s
                    # of J/m^2 (B/C1.12.2)
                    output['long_wave_radiation_1hr'] = \
                        -1*round(rad_convert(rad, time), -4)
                if time == 24:
                    #  Set negative and convert to J/m^2,rounding to 10000s
                    # of J/m^2 (B/C1.12.2)
                    output['long_wave_radiation_24hr'] = \
                        -1*round(rad_convert(rad, time), -4)

        if 'short_wave' in rad_dict.keys():
            if rad_dict['short_wave']['value'] is not None:
                rad = rad_dict['short_wave']['value']
                time = rad_dict['short_wave']['time_before_obs']['value']

                if time == 1:
                    #  Convert to J/m^2,rounding to 1000s
                    # of J/m^2 (B/C1.12.2)
                    output['short_wave_radiation_1hr'] = \
                        round(rad_convert(rad, time), -3)
                if time == 24:
                    #  Convert to J/m^2,rounding to 1000s
                    # of J/m^2 (B/C1.12.2)
                    output['short_wave_radiation_24hr'] = \
                        round(rad_convert(rad, time), -3)

        if 'direct_solar' in rad_dict.keys():
            if rad_dict['direct_solar']['value'] is not None:
                rad = rad_dict['direct_solar']['value']
                time = rad_dict['direct_solar']['time_before_obs']['value']

                if time == 1:
                    #  Convert to J/m^2,rounding to 100s of J/m^2 (B/C1.12.2)
                    output['direct_solar_radiation_1hr'] = \
                        round(rad_convert(rad, time), -2)

                elif time == 24:
                    #  Convert to J/m^2,rounding to 100s of J/m^2 (B/C1.12.2)
                    output['direct_solar_radiation_24hr'] = \
                        round(rad_convert(rad, time), -2)
    #  Group 6 6RRRtR - this is the same group as that in section 1, but over
    # a different time period tR
    #  (which is not a multiple of 6 hours as it is in section 1)
    if 'precipitation_s3' in decode.keys():
        # In SYNOP it is given in mm, and in BUFR it is required to be
        # in kg/m^2 (1mm = 1kg/m^2 for water)
        output['precipitation_s3'] = \
            decode['precipitation_s3']['amount']['value']
        # The time period is expected to be in hours
        output['ps3_time_period'] = -1 * \
            decode['precipitation_s3']['time_before_obs']['value']

    #  Group 7 7R24R24R24R24 - this group is the same as group 6, but
    # over a 24 hour time period
    if 'precipitation_24h' in decode.keys():
        # In SYNOP it is given in mm, and in BUFR it is required to be
        # in kg/m^2 (1mm = 1kg/m^2 for water)
        output['precipitation_24h'] = \
            decode['precipitation_24h']['amount']['value']

    #  Group 8 8NsChshs - information about a layer or mass of cloud.
    #  This group can be repeated for up to 4 cloud genuses that are witnessed,
    # by B/C1.4.5.1.1.

    # Create number of s3 group 8 clouds variable, in case there is no group 8
    num_s3_clouds = 0

    if 'cloud_layer' in decode.keys():

        # Name the array of 8NsChshs groups
        genus_array = decode['cloud_layer']

        # Get the number of 8NsChshs groups in the SYNOP message
        num_s3_clouds = len(genus_array)

        # For each cloud genus...
        for i in range(num_s3_clouds):

            # The vertical significance is determined by the number of clouds
            # given and whether it is a
            # Cumulonimbus cloud, by B/C1.4.5.2.1. Moreover, it also depends
            # on whether the station is automatic
            # (WMO_station_type = 0). We implement this below:

            # We create a boolean variable, which yields True if the station
            # is automatic
            automatic_state = bool(output['WMO_station_type'] == 0)

            if genus_array[i]['cloud_genus'] is not None:
                C_code = genus_array[i]['cloud_genus']['_code']
                output[f'cloud_genus_s3_{i}'] = C_code

                if C_code == 9:  # Cumulonimbus
                    if automatic_state:
                        output[f'vs_s3_{i}'] = 24
                    else:
                        output[f'vs_s3_{i}'] = 4
                else:  # Non-Cumulonimbus
                    if automatic_state:
                        output[f'vs_s3_{i}'] = i+21
                    else:
                        output[f'vs_s3_{i}'] = i+1

            else:
                # Missing value
                output[f'cloud_genus_s3_{i}'] = 63

                if automatic_state:
                    output[f'vs_s3_{i}'] = 20
                else:
                    output[f'vs_s3_{i}'] = 63

            if genus_array[i]['cloud_cover'] is not None:
                # This is left in oktas just like group 8 in section 1
                N_oktas = genus_array[i]['cloud_cover']['value']

                # If the cloud cover is 9 oktas, this means the sky was
                # obscured and we keep the value as None
                if N_oktas == 9:
                    # Replace vertical significance code in this case
                    output[f'vs_s3_{i}'] = 5

                else:
                    output[f'cloud_amount_s3_{i}'] = N_oktas

            else:
                # Missing value
                output[f'cloud_amount_s3_{i}'] = 15

            if genus_array[i]['cloud_height'] is not None:
                # In SYNOP the code table values correspond to heights in m,
                # which BUFR requires
                output[f'cloud_height_s3_{i}'] = \
                    genus_array[i]['cloud_height']['value']

    #  Group 9 9SpSpspsp is regional supplementary information and is
    #   mostly not present in the B/C1 regulations.
    #  The only part present in the B/C1 regulations are the maximum
    #  wind gust speed for region VI (groups 910fmfm and 911fxfx).
    #  These are given and required to be in m/s.

    if 'highest_gust' in decode.keys():
        if decode['highest_gust']['gust_1']['speed'] is not None:
            output['highest_gust_1'] = \
                decode['highest_gust']['gust_1']['speed']['value']
        if decode['highest_gust']['gust_2']['speed'] is not None:
            output['highest_gust_2'] = \
                decode['highest_gust']['gust_2']['speed']['value']

    #  Regulation 6/12.12.2 in the WMO regional guide tells us that the
    #  1st max gust speed has fixed time period 10 minutes, and the 2nd has
    #  time period equal to the length of the observation time in minutes.
    # NOTE: All time periods must be negative
    if hr % 6 == 0:
        output['hg2_time_period'] = -6*60
    elif hr % 3 == 0:
        output['hg2_time_period'] = -3*60
    elif hr % 2 == 0:
        output['hg2_time_period'] = -2*60
    else:
        output['hg2_time_period'] = -60

    # ! SECTION 4

    #  Only one group N'C'H'H'Ct - information about cloud layers whose base
    # is below station level
    # NOTE: Section 4 has not been properly implemented in pymetdecoder, so we
    # finish the implementation here.

    #  This group can be repeated for as many such cloud genuses that are
    # witnessed, by B/C1.5.1.5.

    # Create number of s4 clouds variable, in case there are no s4 groups
    num_s4_clouds = 0

    if 'section4' in decode.keys():

        # Name the array of section 4 items
        genus_array = decode['section4']

        # Get the number of section 4 groups in the SYNOP message
        num_s4_clouds = len(genus_array)

        # For each cloud genus with base below station level...
        for i in range(num_s4_clouds):

            # Get cloud information codes
            cloud_amount = genus_array[i][0]
            cloud_genus = genus_array[i][1]
            cloud_height = genus_array[i][2:4]
            cloud_top = genus_array[i][4]

            #  We now take a different approach, by updating the template
            # dictionary keys where necessary

            # Now we convert the code string to an integer, and check that
            # there aren't missing values
            if cloud_amount != '/':
                output[f'cloud_amount_s4_{i}'] = int(cloud_amount)
            else:
                # Missing value
                output[f'cloud_amount_s4_{i}'] = 15

            if cloud_genus != '/':
                output[f'cloud_genus_s4_{i}'] = int(cloud_genus)
            else:
                # Missing value
                output[f'cloud_genus_s4_{i}'] = 63

            if cloud_height != '//':
                # Multiply by 100 to get metres (B/C1.5.2.4)
                output[f'cloud_height_s4_{i}'] = int(cloud_height) * 100

            if cloud_top != '/':
                output[f'cloud_top_s4_{i}'] = int(cloud_top)
            else:
                # Missing value
                output[f'cloud_top_s4_{i}'] = 15

    # ! Return the new dictionary and the number of groups in section 4
    return output, num_s3_clouds, num_s4_clouds


def file_extract(file):
    """This function extracts the contents of the file and the date of the file

    Args:
        file (str): The file directory or file name of the SYNOP message.
    """
    # Open and read the file, stripping any new lines
    try:
        with open(file, "r") as fp:
            data = fp.read()
    except Exception:
        return "Error: The file path is incorrect."

    # Obtain the year and month of the example_data from the file name
    file_name = os.path.basename(file)
    file_year, file_month = get_date_from_filename(file_name)

    # Obtain the individual SYNOP messages from the file contents
    messages = message_extract(data)

    # Return the list of messages and the date of the file
    return messages, file_year, file_month


def message_extract(data):
    """This function separates the SYNOP tac and returns the individual SYNOP
    messages, ready for conversion
    Args:
        data (str): The SYNOP tac.
    """

    # Check for abbreviated header line TTAAii etc.

    # Now split based as section 0 of synop, beginning AAXX YYGGi_w
    start_position = data.find("AAXX")

    # Start position is -1 if AAXX is not present in the message
    if start_position == -1:
        raise Exception(
            "Invalid SYNOP message: AAXX could not be found."
        )

    data = re.split('(AAXX [0-9]{5})', data[start_position:])
    data = data[1:]  # Drop first null element
    # Iterate over messages processing
    messages = []
    for d in data:
        if "AAXX" in d:
            s0 = d
        else:
            if not d.__contains__("="):
                raise Exception("Delimiters (=) are not present in the string, thus unable to identify separate SYNOP messages.")  # noqa
            d = re.sub(r"\n+", " ", d)
            _messages = d.split("=")
            num_msg = len(_messages)
            for idx in range(num_msg):
                # if len(_messages[idx]) > 0:
                if len(re.sub(r"\s+", "", f"{_messages[idx]}")) > 0:
                    _messages[idx] = \
                        re.sub(r"\s+", " ", f"{s0} {_messages[idx]}")
                    # messages.extend(
                    # re.sub(r"\s+", " ", f"{s0} {_messages[idx]}")
                    # )
                else:
                    _messages[idx] = None
            messages.extend(list(filter(None, _messages)))
    # Return the messages
    return messages


def get_date_from_filename(name):
    """This function checks whether the input file name conforms to
        the standards, and if so returns the datetime of the file, otherwise
        defaults to returning the current year and month.

    Args:
        name (str): The file path basename.

    Returns:
        datetime: The datetime of the file.
    """

    # File format is:
    # pflag_productidentifier_oflag_originator_yyyyMMddhhmmss.extension
    try:
        # Returns the part of the string that should be the datetime of the
        # file (begins with an underscore, but doesn't end with one)
        # Note: \d represents the decimal part, {8} means it
        # checks for 8 digits
        match = re.search(r"_(\d{8})", name)
        # Strip the datetime from the part of the string
        d = datetime.strptime(match.group(1), '%Y%m%d')
        year = d.year
        month = d.month
        return year, month

    except ValueError:
        LOGGER.error(
            f"""File {name} is in wrong file format. The current year and month
            will be used for the conversion."""
            )
        year = date.today().year
        month = date.today().month
        return year, month


def extract_individual_synop(data):
    return message_extract(data)


def parse_synop(data, year, month):
    return convert_to_dict(data, year, month)


def transform(data: str, metadata: str, year: int, month: int):
    """
    Function to convert SYNOP encoded observations to BUFR.

    :param data: String containing the example_data to encode
    :param metadata: String containing CSV encoded metadata
    """

    # ===================
    # First parse metadata file
    # ===================
    if isinstance(metadata, str):
        fh = StringIO(metadata)
        reader = csv.reader(fh, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        col_names = next(reader)
        metadata_dict = {}
        tsi_mapping = {}
        for row in reader:
            single_row = dict(zip(col_names, row))
            wsi = single_row['wigos_station_identifier']
            tsi = single_row['traditional_station_identifier']
            metadata_dict[wsi] = deepcopy(single_row)
            if tsi in tsi_mapping:
                LOGGER.error(f"""Duplicate entries found for station {tsi} in
                             station list file""")
                raise ValueError(f"""Duplicate entries found for station {tsi}
                                 in station list file""")
            tsi_mapping[tsi] = wsi
        fh.close()
        # metadata = metadata_dict[wsi]
    else:
        LOGGER.error("Invalid metadata")
        raise ValueError

    # Now extract individual synop reports from string
    messages = extract_individual_synop(data)

    # Now we need to iterate over the reports, parsing and converting to BUFR
    for message in messages:
        # check we have date
        if message is None:
            continue
        # create dictionary to store / return result in
        result = dict()

        # get mapping template, this needs to be reloaded everytime as each
        # SYNOP can have a different number of
        # replications
        mapping = deepcopy(_mapping)

        # parse example_data to dictionary and get number of section 3 and 4
        # clouds
        msg, num_s3_clouds, num_s4_clouds = parse_synop(message, year, month)
        # get TSI
        tsi = msg['station_id']
        # set WSI
        wsi = tsi_mapping[tsi]
        # parse WSI to get sections
        try:
            wsi_series, wsi_issuer, wsi_issue_number, wsi_local = wsi.split("-")   # noqa
        except Exception:
            raise ValueError(f"""Invalid WSI ({wsi}) found in station file,
                             unable to parse""")

        # get other required metadata
        latitude = metadata_dict[wsi]["latitude"]
        longitude = metadata_dict[wsi]["longitude"]
        station_height = metadata_dict[wsi]["elevation"]

        # add these values to the example_data dictionary
        msg['_wsi_series'] = wsi_series
        msg['_wsi_issuer'] = wsi_issuer
        msg['_wsi_issue_number'] = wsi_issue_number
        msg['_wsi_local'] = wsi_local
        msg['_latitude'] = latitude
        msg['_longitude'] = longitude

        # update mappings for number of clouds
        s3_mappings = {}
        for idx in range(num_s3_clouds):
            # Build the dictionary of mappings for section 3 group 8NsChshs

            # NOTE: The following keys have been used before so the replicator
            # has to be increased:
            # - cloudAmount: used 2 times (Nh, Ns)
            # - cloudType: used 4 times (CL, CM, CH, C)
            # - heightOfBaseOfCloud: used
            # 1 time (h)
            # - verticalSignificance: used 7 times (for N, low-high cloud
            # amount, low-high cloud drift)
            s3_mappings = {
                {"eccodes_key": f"""#{idx+8}
                 #verticalSignificanceSurfaceObservations""",
                 "value": f"example_data:vs_s3_{idx+1}"},
                {"eccodes_key": f"#{idx+3}#cloudAmount",
                 "value": f"example_data:cloud_amount_s3_{idx+1}"},
                {"eccodes_key": f"#{idx+5}#cloudType",
                 "value": f"example_data:cloud_genus_s3_{idx+1}"},
                {"eccodes_key": f"#{idx+2}#heightOfBaseOfCloud",
                 "value": f"example_data:cloud_height_s3_{idx+1}"},
            }
        mapping.update(s3_mappings)

        s4_mappings = {}
        for idx in range(num_s4_clouds):
            # Based upon the station height metadata, the value of vertical
            # significance for section 4 groups can be determined.
            # Specifically, by B/C1.5.2.1, clouds with bases below but tops
            # above station level have vertical significance code 10.
            # Clouds with bases and tops below station level have vertical
            # significance code 11.
            cloud_top_height = msg[f'cloud_height_s4_{idx+1}']

            if cloud_top_height > station_height:
                vs_s4 = 10
            else:
                vs_s4 = 11

            # NOTE: Some of the ecCodes keys are used in the above, so we must
            # add 'num_s3_clouds'
            s4_mappings = {
                {"eccodes_key": f"""#{idx+num_s3_clouds+8}
                 #verticalSignificanceSurfaceObservations""",
                 "value": f"const:{vs_s4}"},
                {"eccodes_key": f"#{idx+num_s3_clouds+3}#cloudAmount",
                 "value": f"example_data:cloud_amount_s4_{idx+1}"},
                {"eccodes_key": f"#{idx+num_s3_clouds+5}#cloudType",
                 "value": f"example_data:cloud_genus_s4_{idx+1}"},
                {"eccodes_key": f"#{idx+1}#heightOfTopOfCloud",
                 "value": f"example_data:cloud_height_s4_{idx+1}"},
                {"eccodes_key": f"#{idx+1}#cloudTopDescription",
                 "value": f"example_data:cloud_top_s4_{idx+1}"}
            }
        mapping.update(s4_mappings)

        # At this point we have a dictionary for the example_data, a
        # dictionary of the mappings and the metadata
        # The last step is to convert to BUFR.
        unexpanded_descriptors = [301150, 307080]
        short_delayed_replications = []
        # update replications
        delayed_replications = [max(1, num_s3_clouds), max(1, num_s4_clouds)]
        extended_delayed_replications = []
        table_version = 37
        try:
            # create new BUFR msg
            message = BUFRMessage(
                unexpanded_descriptors,
                short_delayed_replications,
                delayed_replications,
                extended_delayed_replications,
                table_version)
        except Exception as e:
            LOGGER.error("Error creating BUFRMessage")
            raise e

        # parse
        message.parse(msg, mapping)

        try:
            result["bufr4"] = message.as_bufr()  # encode to BUFR
        except Exception as e:
            LOGGER.error("Error encoding BUFR, null returned")
            LOGGER.error(e)
            result["bufr4"] = None

        # now identifier based on WSI and observation date as identifier
        isodate = message.get_datetime().strftime('%Y%m%dT%H%M%S')
        rmk = f"WIGOS_{wsi}_{isodate}"

        # now additional metadata elements
        result["_meta"] = {
            "id": rmk,
            "geometry": {
                "type": "Point",
                "coordinates": [
                    message.get_element('#1#longitude'),
                    message.get_element('#1#latitude')
                ]
            },
            "properties": {
                "md5": message.md5(),
                "wigos_station_identifier": wsi,
                "datetime": message.get_datetime(),
                "originating_centre": message.get_element("bufrHeaderCentre"),
                "data_category": message.get_element("dataCategory")
            }
        }

        time_ = datetime.now(timezone.utc).isoformat()
        LOGGER.info(f"{time_}|{result['_meta']}")


        # now yield result back to caller
        yield result
