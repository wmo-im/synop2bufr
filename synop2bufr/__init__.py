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
from io import StringIO
import json
import logging
import math
import os
import re
from typing import Iterator

# Now import pymetdecoder and csv2bufr
from pymetdecoder import synop
from csv2bufr import BUFRMessage

__version__ = '0.6.2'

LOGGER = logging.getLogger(__name__)

# Global arrays to store warnings and errors
warning_msgs = []
error_msgs = []

# ! Configure the pymetdecoder/csv2bufr loggers to append warnings to the array


class ArrayHandler(logging.Handler):

    # The emit method will be called every time there is a log
    def emit(self, record):
        # If log level is warning, append to the warnings messages array
        if record.levelname == "WARNING":
            warning_msgs.append(self.format(record))


# Create instance of array handler
array_handler = ArrayHandler()
# Set format to be just the pure warning message with no metadata
formatter = logging.Formatter('%(message)s')
array_handler.setFormatter(formatter)
# Set level to ensure warnings are captured
array_handler.setLevel(logging.WARNING)

# Grab pymetdecoder logger and configure
PYMETDECODER_LOGGER = logging.getLogger('pymetdecoder')
PYMETDECODER_LOGGER.setLevel(logging.WARNING)
# Use this array handler in the pymetdecoder logger
PYMETDECODER_LOGGER.addHandler(array_handler)

# Grab csv2bufr logger and configure
CSV2BUFR_LOGGER = logging.getLogger('csv2bufr')
CSV2BUFR_LOGGER.setLevel(logging.WARNING)
# Use this array handler in the csv2bufr logger
CSV2BUFR_LOGGER.addHandler(array_handler)

# status codes
FAILED = 0
PASSED = 1

# ! Initialise the template dictionary and mappings

# Enumerate the keys
_keys = ['report_type', 'year', 'month', 'day',
         'hour', 'minute',
         'block_no', 'station_no', 'station_id', 'region',
         'WMO_station_type',
         'lowest_cloud_base', 'visibility', 'cloud_cover',
         'wind_indicator', 'template',
         'wind_time_period', 'wind_direction', 'wind_speed',
         'air_temperature', 'dewpoint_temperature',
         'relative_humidity', 'station_pressure',
         'isobaric_surface', 'geopotential_height', 'sea_level_pressure',
         '3hr_pressure_change', 'pressure_tendency_characteristic',
         'precipitation_s1', 'ps1_time_period', 'present_weather',
         'past_weather_1', 'past_weather_2', 'past_weather_time_period',
         'cloud_vs_s1', 'cloud_amount_s1', 'low_cloud_type',
         'middle_cloud_type', 'high_cloud_type',
         'maximum_temperature', 'minimum_temperature',
         'maximum_temperature_period_start',
         'maximum_temperature_period_end',
         'minimum_temperature_period_start',
         'minimum_temperature_period_end',
         'ground_state', 'ground_temperature', 'snow_depth',
         'evapotranspiration', 'evaporation_instrument',
         'temperature_change',
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
         'highest_gust_1', 'highest_gust_2']

# Build the dictionary template
synop_template = dict.fromkeys(_keys)

THISDIR = os.path.dirname(os.path.realpath(__file__))
MAPPINGS_307080 = f"{THISDIR}{os.sep}resources{os.sep}synop-mappings-307080.json"  # noqa
MAPPINGS_307096 = f"{THISDIR}{os.sep}resources{os.sep}synop-mappings-307096.json"  # noqa


# Load template mappings files, this will be updated for each message.
with open(MAPPINGS_307080) as fh:
    _mapping_307080 = json.load(fh)
with open(MAPPINGS_307096) as fh:
    _mapping_307096 = json.load(fh)


def parse_synop(message: str, year: int, month: int) -> dict:
    """
    This function parses a SYNOP message, storing and returning the
    data as a Python dictionary.

    :param message: String containing the SYNOP message to be decoded
    :param year: Int value of the corresponding year for the SYNOP messsage
    :param month: Int value of the corresponding month for the SYNOP messsage

    :returns: `dict` of parsed SYNOP message
    """
    # Make warning messages array global
    global warning_msgs

    # Get the full output decoded message from the pymetdecoder package
    try:
        decoded = synop.SYNOP().decode(message)
    except Exception as e:
        LOGGER.error("Unable to decode the SYNOP message.")
        raise e

    # Get the template dictionary to be filled
    output = deepcopy(synop_template)

    # SECTIONS 0 AND 1

    # The following do not need to be converted
    output['report_type'] = message[0:4]
    output['year'] = year
    output['month'] = month

    if decoded.get('obs_time') is not None:
        try:
            output['day'] = decoded['obs_time']['day']['value']
        except Exception:
            output['day'] = None
        try:
            output['hour'] = decoded['obs_time']['hour']['value']
        except Exception:
            output['hour'] = None

            # The minute will be 00 unless specified by exact observation time
    if decoded.get('exact_obs_time') is not None:
        try:
            output['minute'] = decoded['exact_obs_time']['minute']['value']
        except Exception:
            output['minute'] = None
        # Overwrite the hour, because the actual observation may be from
        # the hour before but has been rounded in the YYGGiw group
        try:
            output['hour'] = decoded['exact_obs_time']['hour']['value']
        except Exception:
            output['hour'] = None
    else:
        output['minute'] = 0

    # Translate wind instrument flag from the SYNOP code to the BUFR code
    if decoded.get('wind_indicator') is not None:
        try:
            iw = decoded['wind_indicator']['value']

            # Note bit 3 should never be set for synop, units
            # of km/h not reportable
            if iw == 0:
                iw_translated = 0b0000  # Wind in m/s, default, no bits set
                output['template'] = 307080
            elif iw == 1:
                iw_translated = 0b1000  # Wind in m/s with anemometer bit 1 (left most) set  # noqa
                output['template'] = 307096
            elif iw == 3:
                iw_translated = 0b0100  # Wind in knots, bit 2 set
                output['template'] = 307080
            elif iw == 4:
                iw_translated = 0b1100  # Wind in knots with anemometer, bits
                # 1 and 2 set # noq
                output['template'] = 307096
            else:
                iw_translated = None  # 0b1111  # Missing value
                output['template'] = 307080

            output['wind_indicator'] = iw_translated
        except Exception:
            output['wind_indicator'] = None
            output['template'] = 307080
    else:
        output['template'] = 307080

    if decoded.get('station_id') is not None:
        try:
            tsi = decoded['station_id']['value']
            output['station_id'] = tsi
            output['block_no'] = tsi[0:2]
            output['station_no'] = tsi[2:5]
        except Exception:
            tsi = None
            output['station_id'] = None
            output['block_no'] = None
            output['station_no'] = None

    # Get region of report
    if decoded.get('region') is not None:
        try:
            output['region'] = decoded['region']['value']
        except Exception:
            output['region'] = None

    # We translate this station type flag from the SYNOP code to the BUFR code
    if decoded.get('weather_indicator') is not None:
        try:
            ix = decoded['weather_indicator']['value']
            if ix <= 3:
                ix_translated = 1  # Manned station
            elif ix == 4:
                ix_translated = 2  # Hybrid station
            elif ix > 4 and ix <= 7:
                ix_translated = 0  # Automatic station
            else:
                ix_translated = None  # Missing value
        except Exception:
            ix_translated = None
    else:
        ix_translated = None  # Missing value

    output['WMO_station_type'] = ix_translated

    # Lowest cloud base is already given in metres, but we specifically select
    # the minimum value  # noq
    # NOTE: By B/C1.4.4.4 the precision of this value is in tens of metres
    if decoded.get('lowest_cloud_base') is not None:
        try:
            output['lowest_cloud_base'] = round(decoded['lowest_cloud_base']['min'], -1)  # noqa
        except Exception:
            output['lowest_cloud_base'] = None

    # Visibility is already given in metres
    if decoded.get('visibility') is not None:
        try:
            output['visibility'] = decoded['visibility']['value']
        except Exception:
            output['visibility'] = None

    # Cloud cover is given in oktas, which we convert to a percentage
    #  NOTE: By B/C10.4.4.1 this percentage is always rounded up
    if decoded.get('cloud_cover') is not None:
        try:
            N_oktas = decoded['cloud_cover']['_code']
            # If the cloud cover is 9 oktas, this means the sky was obscured
            # and we keep the value as None
            if N_oktas == 9:
                N_percentage = 113
            else:
                N_percentage = math.ceil((N_oktas / 8) * 100)
                output['cloud_cover'] = N_percentage
        except Exception:
            output['cloud_cover'] = None

    # Wind direction is already in degrees
    if decoded.get('surface_wind') is not None:
        # See B/C1.10.5.3
        # NOTE: Every time period in the following code shall be a negative number,  # noqa
        # to indicate measurements have been taken up until the present.
        output['wind_time_period'] = -10

        try:

            if decoded['surface_wind']['direction'] is not None:
                try:
                    output['wind_direction'] = decoded['surface_wind']['direction']['value']  # noqa
                except Exception:
                    output['wind_direction'] = None

            # Wind speed in units specified by 'wind_indicator', convert to m/s
            if decoded['surface_wind']['speed'] is not None:
                try:
                    ff = decoded['surface_wind']['speed']['value']
                    # Find the units
                    ff_unit = decoded['wind_indicator']['unit']

                    # If units are knots instead of m/s, convert it to knots
                    if ff_unit == 'KT':
                        ff *= 0.51444
                    output['wind_speed'] = ff
                except Exception:
                    output['wind_speed'] = None

        except Exception:
            output['wind_direction'] = None
            output['wind_speed'] = None

    # Temperatures are given in Celsius, convert to kelvin and round to 2 dp
    if decoded.get('air_temperature') is not None:
        try:
            output['air_temperature'] = round(decoded['air_temperature']['value'] + 273.15, 2)  # noqa
        except Exception:
            output['air_temperature'] = None

    if decoded.get('dewpoint_temperature') is not None:
        try:
            output['dewpoint_temperature'] = round(decoded['dewpoint_temperature']['value'] + 273.15, 2)  # noqa
        except Exception:
            output['dewpoint_temperature'] = None

    # Verify that the dewpoint temperature is less than or equal to
    # the air temperature
    if ((output.get('air_temperature') is not None) and
            (output.get('dewpoint_temperature') is not None)):

        A = output['air_temperature']
        D = output['dewpoint_temperature']

        # If the dewpoint temperature is higher than the air temperature,
        # log a warning and set both values to None
        if A < D:
            LOGGER.warning(f"Reported dewpoint temperature {D} is greater than the reported air temperature {A}. Elements set to missing")  # noqa
            warning_msgs.append(f"Reported dewpoint temperature {D} is greater than the reported air temperature {A}. Elements set to missing")  # noqa

            output['air_temperature'] = None
            output['dewpoint_temperature'] = None

    # RH is already given in %
    if decoded.get('relative_humidity') is not None:
        try:
            output['relative_humidity'] = decoded['relative_humidity']['value']
        except Exception:
            output['relative_humidity'] = None

    else:
        # if RH is missing estimate from air temperature and dew point
        # temperature
        #
        # Reference to equation / method required
        try:
            A = output['air_temperature']
            D = output['dewpoint_temperature']
        except Exception:
            A = None
            D = None

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
    if decoded.get('station_pressure') is not None:
        try:
            output['station_pressure'] = round(decoded['station_pressure']['value'] * 100, -1)  # noqa
        except Exception:
            output['station_pressure'] = None

    #  Similar to above. By B/C1.3.2, pressure has precision in tens of Pa
    if decoded.get('sea_level_pressure') is not None:
        try:
            output['sea_level_pressure'] = round(decoded['sea_level_pressure']['value'] * 100, -1)  # noqa
        except Exception:
            output['sea_level_pressure'] = None

    if decoded.get('geopotential') is not None:
        try:
            output['isobaric_surface'] = round(decoded['geopotential']['surface']['value'] * 100, 1)  # noqa
        except Exception:
            output['isobaric_surface'] = None
        try:
            output['geopotential_height'] = decoded['geopotential']['height']['value']  # noqa
        except Exception:
            output['geopotential_height'] = None

    if decoded.get('pressure_tendency') is not None:
        #  By B/C1.3.3, pressure has precision in tens of Pa
        try:
            output['3hr_pressure_change'] = round(decoded['pressure_tendency']['change']['value'] * 100, -1)  # noqa
        except Exception:
            output['3hr_pressure_change'] = None

        try:
            output['pressure_tendency_characteristic'] = decoded['pressure_tendency']['tendency']['value']  # noqa
        except Exception:
            output['pressure_tendency_characteristic'] = None

    # Precipitation is given in mm, which is equal to kg/m^2 of rain
    if decoded.get('precipitation_s1') is not None:
        # NOTE: When the precipitation measurement RRR has code 990, this
        # represents a trace amount of rain
        # (<0.01 inches), which pymetdecoder records as 0. I (RTB) agree with
        # this choice, and so no change has been made.
        try:
            output['precipitation_s1'] = decoded['precipitation_s1']['amount']['value']  # noqa
        except Exception:
            output['precipitation_s1'] = None

        try:
            output['ps1_time_period'] = -1 * decoded['precipitation_s1']['time_before_obs']['value']  # noqa
        except Exception:
            output['ps1_time_period'] = None

    # The present and past weather SYNOP codes align with that of BUFR apart
    # from missing values
    if decoded.get('present_weather') is not None:
        try:
            output['present_weather'] = decoded['present_weather']['value']
        except Exception:
            output['present_weather'] = None

    if decoded.get('past_weather') is not None:
        try:
            output['past_weather_1'] = decoded['past_weather']['past_weather_1']['value']  # noqa
        except Exception:
            output['past_weather_1'] = None
        try:
            output['past_weather_2'] = decoded['past_weather']['past_weather_2']['value']  # noqa
        except Exception:
            output['past_weather_2'] = None
    else:  # Missing values
        output['past_weather_1'] = None
        output['past_weather_2'] = None

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
    if decoded.get('cloud_types') is not None:
        try:
            Cl = decoded['cloud_types']['low_cloud_type']['value'] + 30
        except Exception:
            Cl = None
        output['low_cloud_type'] = Cl

        try:
            Cm = decoded['cloud_types']['middle_cloud_type']['value'] + 20
        except Exception:
            Cm = None

        output['middle_cloud_type'] = Cm

        try:
            Ch = decoded['cloud_types']['high_cloud_type']['value'] + 10
        except Exception:
            Ch = None

        output['high_cloud_type'] = Ch

        if decoded['cloud_types'].get('low_cloud_amount') is not None:
            # Low cloud amount is given in oktas, and by B/C1.4.4.3.1 it
            # stays that way for BUFR
            try:
                N_oktas = decoded['cloud_types']['low_cloud_amount']['value']
            except Exception:
                N_oktas = None

            # If the cloud cover is 9 oktas, this means the sky was obscured
            # and we keep the value as None
            if N_oktas == 9:
                # By B/C1.4.4.2, if sky obscured, use significance code 5
                output['cloud_vs_s1'] = 5
            else:
                # By B/C1.4.4.2, if low clouds present, use significance code 7
                output['cloud_vs_s1'] = 7
                output['cloud_amount_s1'] = N_oktas

        elif decoded['cloud_types'].get('middle_cloud_amount') is not None:
            # Middle cloud amount is given in oktas, and by B/C1.4.4.3.1 it
            # stays that way for BUFR
            try:
                N_oktas = decoded['cloud_types']['middle_cloud_amount']['value']  # noqa
            except Exception:
                N_oktas = None

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
        elif decoded['cloud_types']['high_cloud_type'] is not None:
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
    if decoded.get('maximum_temperature') is not None:
        #  Convert to Kelvin and round to required precision
        try:
            output['maximum_temperature'] = decoded['maximum_temperature']['value']  # noqa
            if output['maximum_temperature'] is not None:
                output['maximum_temperature'] = round(output['maximum_temperature'] + 273.15, 2)  # noqa

        except Exception:
            output['maximum_temperature'] = None

    #  Group 2 2snTnTnTn - gives minimum temperature over a time period
    # decided by the region
    if decoded.get('minimum_temperature') is not None:
        #  Convert to Kelvin and round to required precision
        try:
            output['minimum_temperature'] = decoded['minimum_temperature']['value']  # noqa
            if output['minimum_temperature'] is not None:
                output['minimum_temperature'] = round(output['minimum_temperature'] + 273.15, 2)  # noqa
        except Exception:
            output['minimum_temperature'] = None

    # Now calculate the associated time periods for the max and min temps
    try:
        if output['region'] in ['Antarctic', 'I', 'II', 'III', 'VI']:
            # Extremes recorded over past 12 hours
            output['maximum_temperature_period_start'] = -12
            output['minimum_temperature_period_start'] = -12

        elif output['region'] == 'V':
            # Extremes recorded over past 24 hours
            output['maximum_temperature_period_start'] = -24
            output['minimum_temperature_period_start'] = -24

        elif output['region'] == 'IV':
            # If time is 0000 UTC, extremes recorded over 12 and 18
            # hours respectively
            if output['hour'] == 0:
                output['maximum_temperature_period_start'] = -12
                output['minimum_temperature_period_start'] = -18

            # If time is 0600 UTC, extremes recorded over 24 hours
            elif output['hour'] == 6:
                output['maximum_temperature_period_start'] = -24
                output['minimum_temperature_period_start'] = -24

            # If time is 1200 UTC, maximum is recorded over the previous
            # day and the minimum is recorded over the previous 12 hours
            elif output['hour'] == 12:
                output['maximum_temperature_period_start'] = -36
                output['minimum_temperature_period_start'] = -12

            # If time is 1800 UTC, extremes recorded over 12 and 24
            # hours respectively
            elif output['hour'] == 18:
                output['maximum_temperature_period_start'] = -12
                output['minimum_temperature_period_start'] = -24

        # We now set the end of the time periods to be the time of the
        # observation (0), unless it is the maximum temperature of
        # the previous calendar day (when the time period started
        # 36 hours before the observation)
        if output['maximum_temperature_period_start'] == -36:
            output['maximum_temperature_period_end'] = -12
        else:
            output['maximum_temperature_period_end'] = 0

        # NOTE: I believe the minimum temperature time period always ends
        # at the time of the observation, even for region III
        # (see pg. 97 of the regional manual on codes). However, this
        # is contradicted the BUFR manual (note 2 on pg. 1069) thus
        # we define this as a variable rather than a constant in the mapping
        # file in case changes need to be made in the future.
        output['minimum_temperature_period_end'] = 0

    except Exception:
        output['maximum_temperature_period_start'] = None
        output['minimum_temperature_period_start'] = None
        output['maximum_temperature_period_end'] = None
        output['minimum_temperature_period_end'] = None

    #  Group 3 3Ejjj
    # NOTE: According to SYNOP manual 12.4.5, the group is
    # developed regionally.
    # This regional difference is as follows:
    # It is either omitted, or it takes form 3EsnTgTg, where
    # Tg is the ground temperature
    if decoded.get('ground_state') is not None:
        # get value
        if decoded['ground_state']['state'] is not None:
            try:
                output['ground_state'] = decoded['ground_state']['state']['value']  # noqa
            except Exception:
                output['ground_state'] = None
        else:
            output['ground_state'] = None

        if decoded['ground_state']['temperature'] is not None:
            try:
                #  Convert to Kelvin
                output['ground_temperature'] = round(decoded['ground_state']['temperature']['value'] + 273.15, 2)  # noqa
            except Exception:
                output['ground_temperature'] = None

    #  Group 4 4E'sss - gives state of the ground with snow, and the snow
    # depth (not regional like group 3 is)
    if decoded.get('ground_state_snow') is not None:
        if decoded['ground_state_snow']['state'] is not None:
            # We translate the snow depth flags from the SYNOP codes to the
            # BUFR codes
            try:
                E = decoded['ground_state_snow']['state']['value']
                if E is not None:
                    output['ground_state'] = E + 10
                else:
                    output['ground_state'] = None
            except Exception:
                output['ground_state'] = None
        else:  # Missing value
            output['ground_state'] = None

        # Snow depth is given in cm but should be encoded in m
        try:
            snow_depth = decoded['ground_state_snow']['depth']['depth']  # noqa
        except Exception:
            snow_depth = None

        if snow_depth is not None:
            output['snow_depth'] = snow_depth * 0.01
        else:
            output['snow_depth'] = None

    #  We now look at group 5, 5j1j2j3j4, which can take many different forms
    # and also have
    #  supplementary groups for radiation measurements

    # Evaporation 5EEEiE
    if decoded.get('evapotranspiration') is not None:

        # Evapotranspiration is given in mm, which is equal to kg/m^2 for rain
        try:
            output['evapotranspiration'] = decoded['evapotranspiration']['amount']['value']  # noqa
        except Exception:
            output['evapotranspiration'] = None

        try:
            if decoded['evapotranspiration']['type'] is not None:
                output['evaporation_instrument'] = decoded['evapotranspiration']['type']['_code']  # noqa
            else:
                # Missing value
                output['evaporation_instrument'] = None
        except Exception:
            output['evaporation_instrument'] = None

    # Temperature change 54g0sndT
    if decoded.get('temperature_change') is not None:

        if decoded['temperature_change']['change'] is not None:
            try:
                output['temperature_change'] = decoded['temperature_change']['change']['value']  # noqa
            except Exception:
                output['temperature_change'] = None

    # Sunshine amount 55SSS (24hrs) and 553SS (1hr)
    if (decoded.get('sunshine') is not None):
        if (decoded['sunshine'].get('amount') is not None):

            # The time period remains in hours
            try:
                sun_time = decoded['sunshine']['duration']['value']
            except Exception:
                sun_time = None

            try:
                # Sunshine amount should be given in minutes
                sun_amount = decoded['sunshine']['amount']['value'] * 60
            except Exception:
                sun_amount = None

            if sun_time == 1:
                output['sunshine_amount_1hr'] = sun_amount
            elif sun_time == 24:
                output['sunshine_amount_24hr'] = sun_amount

    # Cloud drift data 56DLDMDH
    #  By B/C1.6.2 we must convert the direction to a degree bearing
    def to_bearing(direction):
        # Between NE and NW
        if direction < 8:
            return direction * 45
        # N
        if direction == 8:
            return 0

    if decoded.get('cloud_drift_direction') is not None:
        if decoded['cloud_drift_direction']['low'] is not None:
            try:
                low_dir = decoded['cloud_drift_direction']['low']['_code']
                # NOTE: If direction code is 0, the clouds are stationary or
                # there are no clouds.
                # If direction code is 0, the direction is unknown or the
                # clouds or invisible.
                # In both cases, I believe no BUFR entry should be made.
                if low_dir > 0 and low_dir < 9:
                    output['low_cloud_drift_direction'] = to_bearing(low_dir)
                else:
                    output['low_cloud_drift_direction'] = None
            except Exception:
                output['low_cloud_drift_direction'] = None

        if decoded['cloud_drift_direction']['middle'] is not None:
            try:
                middle_dir = decoded['cloud_drift_direction']['middle']['_code']  # noqa
                if middle_dir > 0 and middle_dir < 9:
                    output['middle_cloud_drift_direction'] = to_bearing(middle_dir)  # noqa
                else:
                    output['middle_cloud_drift_direction'] = None
            except Exception:
                output['middle_cloud_drift_direction'] = None

        if decoded['cloud_drift_direction']['high'] is not None:
            try:
                high_dir = decoded['cloud_drift_direction']['high']['_code']
                if high_dir > 0 and high_dir < 9:
                    output['high_cloud_drift_direction'] = to_bearing(high_dir)
                else:
                    output['high_cloud_drift_direction'] = None
            except Exception:
                output['high_cloud_drift_direction'] = None

    # Direction and elevation angle of the clouds 57CDaeC
    if decoded.get('cloud_elevation') is not None:
        if decoded['cloud_elevation']['genus'] is not None:
            try:
                output['e_cloud_genus'] = decoded['cloud_elevation']['genus']['_code']  # noqa
            except Exception:
                output['e_cloud_genus'] = None
        else:
            # Missing value
            output['e_cloud_genus'] = None

        if decoded['cloud_elevation']['direction'] is not None:
            try:
                e_dir = decoded['cloud_elevation']['direction']['_code']
            except Exception:
                e_dir = None

            # NOTE: If direction code is 0, the clouds are stationary or there
            # are no clouds.
            # If direction code is 0, the direction is unknown or the clouds
            # or invisible.
            # In both cases, I believe no BUFR entry should be made.
            if e_dir > 0 and e_dir < 9:
                # We reuse the to_bearing function from above
                output['e_cloud_direction'] = to_bearing(e_dir)
            else:
                output['e_cloud_direction'] = None
        try:
            output['e_cloud_elevation'] = decoded['cloud_elevation']['elevation']['value']  # noqa
        except Exception:
            output['e_cloud_elevation'] = None

    # Positive 58p24p24p24 or negative 59p24p24p24 changes in surface pressure
    # over 24hrs
    if decoded.get('pressure_change') is not None:
        try:
            output['24hr_pressure_change'] = round(decoded['pressure_change']['value']*100, -1)  # noqa
        except Exception:
            output['24hr_pressure_change'] = None

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

    if decoded.get('radiation') is not None:
        rad_dict = decoded['radiation']
        # Create a function to do the appropriate conversion depending
        # on time period

        def rad_convert(rad, time):
            if time == 1:
                # 1 kJ/m^2 = 1000 J/m^2
                return 1000 * rad
            elif time == 24:
                # 1 J/cm^2 = 10000 J/m^2
                return 10000 * rad

        if 'positive_net' in rad_dict:
            try:
                rad = rad_dict['positive_net']['value']
                time = rad_dict['positive_net']['time_before_obs']['value']  # noqa
            except Exception:
                rad = None
                time = None
            if None not in (rad, time):
                if time == 1:
                    #  Convert to J/m^2,rounding to 1000s of J/m^2 (B/C1.12.2)
                    output['net_radiation_1hr'] = round(rad_convert(rad, time), -3)  # noqa
                elif time == 24:
                    #  Convert to J/m^2,rounding to 1000s of J/m^2 (B/C1.12.2)
                    output['net_radiation_24hr'] = round(rad_convert(rad, time), -3)  # noqa

        if 'negative_net' in rad_dict:
            try:
                rad = rad_dict['negative_net']['value']
                time = rad_dict['negative_net']['time_before_obs']['value']  # noqa
            except Exception:
                rad = None
                time = None

            if None not in (rad, time):
                if time == 1:
                    #  Set negative and convert to J/m^2,rounding to 1000s
                    # of J/m^2 (B/C1.12.2)
                    output['net_radiation_1hr'] = -1 * round(rad_convert(rad, time), -3)  # noqa
                elif time == 24:
                    #  Set negative and convert to J/m^2,rounding to 1000s
                    # of J/m^2 (B/C1.12.2)
                    output['net_radiation_24hr'] = -1 * round(rad_convert(rad, time), -3)  # noqa

        if 'global_solar' in rad_dict:
            try:
                rad = rad_dict['global_solar']['value']
                time = rad_dict['global_solar']['time_before_obs']['value']
            except Exception:
                rad = None
                time = None

            if None not in (rad, time):
                if time == 1:
                    #  Convert to J/m^2,rounding to 100s of J/m^2 (B/C1.12.2)
                    output['global_solar_radiation_1hr'] = round(rad_convert(rad, time), -2)  # noqa
                elif time == 24:
                    #  Convert to J/m^2,rounding to 100s of J/m^2 (B/C1.12.2)
                    output['global_solar_radiation_24hr'] = round(rad_convert(rad, time), -2)  # noqa

        if 'diffused_solar' in rad_dict:
            try:
                rad = rad_dict['diffused_solar']['value']
                time = rad_dict['diffused_solar']['time_before_obs']['value']
            except Exception:
                rad = None
                time = None

            if None not in (rad, time):
                if time == 1:
                    #  Convert to J/m^2,rounding to 100s of J/m^2 (B/C1.12.2)
                    output['diffuse_solar_radiation_1hr'] = round(rad_convert(rad, time), -2)  # noqa
                elif time == 24:
                    #  Convert to J/m^2,rounding to 100s of J/m^2 (B/C1.12.2)
                    output['diffuse_solar_radiation_24hr'] = round(rad_convert(rad, time), -2)  # noqa

        if 'downward_long_wave' in rad_dict:
            try:
                rad = rad_dict['downward_long_wave']['value']
                time = rad_dict['downward_long_wave']['time_before_obs']['value']  # noqa
            except Exception:
                rad = None
                time = None

            if None not in (rad, time):
                if time == 1:
                    #  Set positive and convert to J/m^2,rounding to 10000s
                    # of J/m^2 (B/C1.12.2)
                    output['long_wave_radiation_1hr'] = round(rad_convert(rad, time), -4)  # noqa
                elif time == 24:
                    #  Set positive and convert to J/m^2,rounding to 10000s
                    # of J/m^2 (B/C1.12.2)
                    output['long_wave_radiation_24hr'] = round(rad_convert(rad, time), -4)  # noqa

        if 'upward_long_wave' in rad_dict:
            try:
                rad = rad_dict['upward_long_wave']['value']
                time = rad_dict['upward_long_wave']['time_before_obs']['value']  # noqa
            except Exception:
                rad = None
                time = None

            if None not in (rad, time):
                if time == 1:
                    #  Set negative and convert to J/m^2,rounding to 10000s
                    # of J/m^2 (B/C1.12.2)
                    output['long_wave_radiation_1hr'] = -1 * round(rad_convert(rad, time), -4)  # noqa
                elif time == 24:
                    #  Set negative and convert to J/m^2,rounding to 10000s
                    # of J/m^2 (B/C1.12.2)
                    output['long_wave_radiation_24hr'] = -1 * round(rad_convert(rad, time), -4)  # noqa

        if 'short_wave' in rad_dict:
            try:
                rad = rad_dict['short_wave']['value']
                time = rad_dict['short_wave']['time_before_obs']['value']  # noqa
            except Exception:
                rad = None
                time = None

            if None not in (rad, time):
                if time == 1:
                    #  Convert to J/m^2,rounding to 1000s of J/m^2 (B/C1.12.2)
                    output['short_wave_radiation_1hr'] = round(rad_convert(rad, time), -3)  # noqa
                if time == 24:
                    #  Convert to J/m^2,rounding to 1000s of J/m^2 (B/C1.12.2)
                    output['short_wave_radiation_24hr'] = round(rad_convert(rad, time), -3)  # noqa

        if 'direct_solar' in rad_dict:
            try:
                rad = rad_dict['direct_solar']['value']
                time = rad_dict['direct_solar']['time_before_obs']['value']  # noqa
            except Exception:
                rad = None
                time = None

            if None not in (rad, time):
                if time == 1:
                    #  Convert to J/m^2,rounding to 100s of J/m^2 (B/C1.12.2)
                    output['direct_solar_radiation_1hr'] = round(rad_convert(rad, time), -2)  # noqa
                elif time == 24:
                    #  Convert to J/m^2,rounding to 100s of J/m^2 (B/C1.12.2)
                    output['direct_solar_radiation_24hr'] = round(rad_convert(rad, time), -2)  # noqa

    #  Group 6 6RRRtR - this is the same group as that in section 1, but over
    # a different time period tR
    #  (which is not a multiple of 6 hours as it is in section 1)
    if decoded.get('precipitation_s3') is not None:
        # In SYNOP it is given in mm, and in BUFR it is required to be
        # in kg/m^2 (1mm = 1kg/m^2 for water)
        try:
            output['precipitation_s3'] = decoded['precipitation_s3']['amount']['value']  # noqa
        except Exception:
            output['precipitation_s3'] = None

        try:
            # The time period is expected to be in hours
            output['ps3_time_period'] = -1 * decoded['precipitation_s3']['time_before_obs']['value']  # noqa
        except Exception:
            output['ps3_time_period'] = None

    # Precipitation indicator iR is needed to determine whether the
    # section 1 and section 3 precipitation groups are missing because there
    # is no data, or because there has been 0 precipitation observed
    if decoded.get('precipitation_indicator') is not None:
        if decoded['precipitation_indicator'].get('value') is not None:

            iR = decoded['precipitation_indicator']['value']

            # iR = 3 means 0 precipitation observed
            if iR == 3:
                output['precipitation_s1'] = 0
                output['precipitation_s3'] = 0

    #  Group 7 7R24R24R24R24 - this group is the same as group 6, but
    # over a 24 hour time period
    if decoded.get('precipitation_24h') is not None:
        # In SYNOP it is given in mm, and in BUFR it is required to be
        # in kg/m^2 (1mm = 1kg/m^2 for water)
        try:
            output['precipitation_24h'] = decoded['precipitation_24h']['amount']['value']  # noqa
        except Exception:
            output['precipitation_24h'] = None

    # Group 8 8NsChshs - information about a layer or mass of cloud.
    # This group can be repeated for up to 4 cloud genuses that are witnessed,
    # by B/C1.4.5.1.1.

    # Create number of s3 group 8 clouds variable, in case there is no group 8
    num_s3_clouds = 0

    if decoded.get('cloud_layer') is not None:

        # Name the array of 8NsChshs groups
        genus_array = decoded['cloud_layer']

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
                try:
                    C_code = genus_array[i]['cloud_genus']['_code']
                    output[f'cloud_genus_s3_{i+1}'] = C_code

                    if C_code == 9:  # Cumulonimbus
                        if automatic_state:
                            output[f'vs_s3_{i+1}'] = 24
                        else:
                            output[f'vs_s3_{i+1}'] = 4

                    else:  # Non-Cumulonimbus
                        if automatic_state:
                            output[f'vs_s3_{i+1}'] = i+21
                        else:
                            output[f'vs_s3_{i+1}'] = i+1
                except Exception:
                    output[f'vs_s3_{i + 1}'] = None
            else:
                # Missing value
                output[f'cloud_genus_s3_{i+1}'] = None
                if automatic_state:
                    output[f'vs_s3_{i+1}'] = 20
                else:
                    output[f'vs_s3_{i+1}'] = None

            if genus_array[i]['cloud_cover'] is not None:
                # This is left in oktas just like group 8 in section 1
                try:
                    N_oktas = genus_array[i]['cloud_cover']['value']
                except Exception:
                    N_oktas = None

                # If the cloud cover is 9 oktas, this means the sky was
                # obscured and we keep the value as None
                if N_oktas == 9:
                    # Replace vertical significance code in this case
                    output[f'vs_s3_{i+1}'] = 5
                else:
                    output[f'cloud_amount_s3_{i+1}'] = N_oktas
            else:
                # Missing value
                output[f'cloud_amount_s3_{i+1}'] = None

            if genus_array[i]['cloud_height'] is not None:
                # In SYNOP the code table values correspond to heights in m,
                # which BUFR requires
                try:
                    output[f'cloud_height_s3_{i+1}'] = genus_array[i]['cloud_height']['value']  # noqa
                except Exception:
                    output[f'cloud_height_s3_{i + 1}'] = None

    #  Group 9 9SpSpspsp is regional supplementary information and is
    #   mostly not present in the B/C1 regulations.
    #  The only part present in the B/C1 regulations are the maximum
    #  wind gust speed for region VI (groups 910fmfm and 911fxfx).
    #  These are given and required to be in m/s.

    if decoded.get('highest_gust') is not None:
        try:
            output['highest_gust_1'] = decoded['highest_gust']['gust_1']['speed']['value']  # noqa
        except Exception:
            output['highest_gust_1'] = None
        try:
            output['highest_gust_2'] = decoded['highest_gust']['gust_2']['speed']['value']  # noqa
        except Exception:
            output['highest_gust_2'] = None

    # ! SECTION 4

    #  Only one group N'C'H'H'Ct - information about cloud layers whose base
    # is below station level
    # NOTE: Section 4 has not been properly implemented in pymetdecoder, so we
    # finish the implementation here.

    #  This group can be repeated for as many such cloud genuses that are
    # witnessed, by B/C1.5.1.5.

    # Create number of s4 clouds variable, in case there are no s4 groups
    num_s4_clouds = 0

    if decoded.get('section4') is not None:

        # Name the array of section 4 items
        genus_array = decoded['section4']
        print("genus_array", genus_array)

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
                output[f'cloud_amount_s4_{i+1}'] = int(cloud_amount)
            else:
                # Missing value
                output[f'cloud_amount_s4_{i+1}'] = 15

            if cloud_genus != '/':
                output[f'cloud_genus_s4_{i+1}'] = int(cloud_genus)
            else:
                # Missing value
                output[f'cloud_genus_s4_{i+1}'] = 63

            if cloud_height != '//':
                # Multiply by 100 to get metres (B/C1.5.2.4)
                output[f'cloud_height_s4_{i+1}'] = int(cloud_height) * 100

            if cloud_top != '/':
                output[f'cloud_top_s4_{i+1}'] = int(cloud_top)
            else:
                # Missing value
                output[f'cloud_top_s4_{i+1}'] = 15

    # ! Return the new dictionary and the number of groups in section 4
    return output, num_s3_clouds, num_s4_clouds


def extract_individual_synop(data: str) -> list:
    """
    Separates the SYNOP tac and returns the individual SYNOP
    messages, ready for conversion

    :param data: The SYNOP tac.

    :returns: `list` of messages
    """
    # Split string based on section 0 of FM-12, beginning with AAXX
    start_position = data.find("AAXX")

    # Start position is -1 if AAXX is not present in the message
    if start_position == -1:
        raise ValueError(
            "Invalid SYNOP message: AAXX could not be found."
        )

    # Split the string by AAXX YYGGiw
    data = re.split('(AAXX\s+[0-9]{5})', data[start_position:])

    # Check if the beginning of the message, that we're about to throw
    # away (data[0]), also contains AAXX and thus there must be a
    # typo present at the AAXX YYGGiw part of the report
    if data[0].__contains__("AAXX"):
        raise ValueError((
            f"The following SYNOP message is invalid: {data[0]}"
            " Please check again for typos."
        ))

    data = data[1:]  # Drop first null element
    # Iterate over messages processing
    messages = []
    for d in data:
        if "AAXX" in d:
            s0 = d
        else:
            if not d.__contains__("="):
                raise ValueError((
                    "Delimiters (=) are not present in the string,"
                    " thus unable to identify separate SYNOP reports."
                    ))

            d = re.sub(r"\n+", " ", d)
            d = re.sub(r"\x03", "", d)
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

    # Check any messages were actually extracted
    if messages == []:
        raise Exception(("No SYNOP reports were extracted."
                         " Perhaps the date group YYGGiw"
                         " is missing."))

    # Return the messages
    return messages


def transform(data: str, metadata: str, year: int,
              month: int) -> Iterator[dict]:
    """
    Convert SYNOP encoded observations to BUFR

    :param data: String containing the data to encode
    :param metadata: String containing CSV encoded metadata
    :param year: year (`int`)
    :param month: month (`int`)

    :returns: iterator
    """
    # Make warning and error messages array global
    global warning_msgs
    global error_msgs

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
            if len(row) == 0:
                continue
            single_row = dict(zip(col_names, row))
            tsi = single_row['traditional_station_identifier']
            try:
                wsi = single_row['wigos_station_identifier']
                metadata_dict[wsi] = deepcopy(single_row)
                if tsi in tsi_mapping:
                    LOGGER.warning(("Duplicate entries found for station"
                                    f" {tsi} in station list file"))
                    warning_msgs.append(("Duplicate entries found for station"
                                        f" {tsi} in station list file"))
                tsi_mapping[tsi] = wsi
            except Exception as e:
                LOGGER.error(e)
                error_msgs.append(str(e))

        fh.close()
        # metadata = metadata_dict[wsi]
    else:
        LOGGER.error("Invalid metadata")
        raise ValueError("Invalid metadata")

    # ===========================================
    # Split the data by the end of message signal
    # ===========================================
    gts_messages = data.upper().split("NNNN")

    # Remove leading or trailing whitespaces from these
    # messages and ignore empty messages after the final NNNN
    gts_messages = [msg.strip()
                    for msg in gts_messages if msg != ""]

    # =====================================
    # Repeat transform for each GTS message
    # =====================================
    for gts_msg in gts_messages:

        # Now extract individual synop reports from string
        try:
            messages = extract_individual_synop(gts_msg)
        except Exception as e:
            LOGGER.error(e)
            error_msgs.append(str(e))
            messages = []  # Fallback to an empty list if no reports extracted
            yield {
                "_meta": {
                    "id": None,
                    "geometry": None,
                    "properties": {
                        "md5": None,
                        "wigos_station_identifier": None,
                        "datetime": None,
                        "originating_centre": None,
                        "data_category": None
                    },
                    "result": {
                        "code": FAILED,
                        "message": "Error encoding, BUFR set to None",
                        "warnings": warning_msgs,
                        "errors": error_msgs
                    },
                    "template": None,
                    "csv": None
                }
            }
            # Reset warning and error messages array for next iteration
            warning_msgs = []
            error_msgs = []

        # Count how many conversions were successful using a dictionary
        conversion_success = {}

        # Now we need to iterate over the reports, parsing
        # and converting to BUFR
        for message in messages:
            # check we have data
            if message is None:
                continue

            # Check data is just a NIL report, if so warn the user and do
            # not create an empty BUFR file
            nil_pattern = r"^[A-Za-z]{4} \d{5} (\d{5}) [Nn][Il][Ll]$"
            match = re.match(nil_pattern, message)
            if match:
                LOGGER.warning(
                    f"NIL report detected for station {match.group(1)}, no BUFR file created.")  # noqa
                warning_msgs.append(
                    f"NIL report detected for station {match.group(1)}, no BUFR file created.")  # noqa
                continue

            # create dictionary to store / return result in
            result = dict()

            # parse data to dictionary and get number of section 3 and 4
            # clouds
            try:
                msg, num_s3_clouds, num_s4_clouds = \
                    parse_synop(message, year, month)
                # get TSI
                tsi = msg['station_id']
            except Exception as e:
                LOGGER.error(
                    f"Error parsing SYNOP report: {message}. {str(e)}!")
                error_msgs.append(f"Error parsing SYNOP report: {message}. {str(e)}!")  # noqa
                result = {
                    "_meta": {
                        "id": None,
                        "geometry": None,
                        "properties": {
                            "md5": None,
                            "wigos_station_identifier": None,
                            "datetime": None,
                            "originating_centre": None,
                            "data_category": None
                        },
                        "result": {
                            "code": FAILED,
                            "message": "Error encoding, BUFR set to None",
                            "warnings": warning_msgs,
                            "errors": error_msgs
                        },
                        "template": None,
                        "csv": None
                    }
                }
                yield result
                # Reset warning and error messages array for next iteration
                warning_msgs = []
                error_msgs = []
                continue

            # Now determine and load the appropriate mappings
            # file depending on the value of the wind indicator.
            # This will be updated for each message.
            bufr_template = msg['template']
            if bufr_template == 307096:
                # Get mapping template, this needs to be
                # reloaded everytime as each SYNOP can have a
                # different number of replications
                mapping = deepcopy(_mapping_307096)
            else:
                # Get mapping template, this needs to be
                # reloaded everytime as each SYNOP can have a
                # different number of replications
                mapping = deepcopy(_mapping_307080)

            # set WSI
            try:
                wsi = tsi_mapping[tsi]
            except Exception:
                conversion_success[tsi] = False
                LOGGER.warning(f"Station {tsi} not found in station file")
                warning_msgs.append(f"Station {tsi} not found in station file")
                result = {
                    "_meta": {
                        "id": None,
                        "geometry": None,
                        "properties": {
                            "md5": None,
                            "wigos_station_identifier": None,
                            "datetime": None,
                            "originating_centre": None,
                            "data_category": None
                        },
                        "result": {
                            "code": FAILED,
                            "message": "Error encoding, BUFR set to None",
                            "warnings": warning_msgs,
                            "errors": error_msgs
                        },
                        "template": None,
                        "csv": None
                    }
                }
                yield result
                # Reset warning and error messages array for next iteration
                warning_msgs = []
                error_msgs = []
                continue

            # parse WSI to get sections
            try:
                wsi_series, wsi_issuer, wsi_issue_number, wsi_local = wsi.split("-")   # noqa

                # get other required metadata
                station_name = metadata_dict[wsi]["station_name"]
                latitude = metadata_dict[wsi]["latitude"]
                longitude = metadata_dict[wsi]["longitude"]
                station_height = metadata_dict[wsi]["elevation"]
                barometer_height = metadata_dict[wsi]["barometer_height"]

                # add these values to the data dictionary
                msg['_wsi_series'] = wsi_series
                msg['_wsi_issuer'] = wsi_issuer
                msg['_wsi_issue_number'] = wsi_issue_number
                msg['_wsi_local'] = wsi_local
                msg['_station_name'] = station_name
                msg['_latitude'] = latitude
                msg['_longitude'] = longitude
                msg['_station_height'] = station_height
                msg['_barometer_height'] = barometer_height
                conversion_success[tsi] = True
            except Exception:
                conversion_success[tsi] = False

                if wsi == "":
                    LOGGER.warning(f"Missing WSI for station {tsi}")
                    warning_msgs.append(f"Missing WSI for station {tsi}")
                else:
                    # If station has not been found in the station
                    # list, don't repeat warning unnecessarily
                    if not (f"Station {tsi} not found in station file"
                            in warning_msgs):
                        LOGGER.warning(f"Invalid metadata for station {tsi} found in station file, unable to parse")  # noqa
                        warning_msgs.append(f"Invalid metadata for station {tsi} found in station file, unable to parse")  # noqa

            if conversion_success[tsi]:
                try:
                    for idx in range(num_s3_clouds):
                        # Build the dictionary of mappings for section 3
                        # group 8NsChshs

                        # NOTE: The following keys have been used
                        # before so the replicator has to be increased:
                        # - cloudAmount: used 2 times (Nh, Ns)
                        # - cloudType: used 4 times (CL, CM, CH, C)
                        # - heightOfBaseOfCloud: used 1 time (h)
                        # - verticalSignificance: used 7 times (for N,
                        # low-high cloud amount, low-high cloud drift)
                        s3_mappings = [
                            {"eccodes_key": (
                                f"#{idx+8}"
                                "#verticalSignificanceSurfaceObservations"
                            ),
                                "value": f"data:vs_s3_{idx+1}"},
                            {"eccodes_key": f"#{idx+3}#cloudAmount",
                                "value": f"data:cloud_amount_s3_{idx+1}",
                                "valid_min": "const:0",
                                "valid_max": "const:8"},
                            {"eccodes_key": f"#{idx+5}#cloudType",
                                "value": f"data:cloud_genus_s3_{idx+1}"},
                            {"eccodes_key": f"#{idx+2}#heightOfBaseOfCloud",
                                "value": f"data:cloud_height_s3_{idx+1}"}
                        ]
                        for m in s3_mappings:
                            mapping.update(m)

                    for idx in range(num_s4_clouds):
                        # Based upon the station height metadata, the
                        # value of vertical significance for section 4
                        # groups can be determined.
                        # Specifically, by B/C1.5.2.1, clouds with bases
                        # below but tops above station level have vertical
                        # significance code 10.
                        # Clouds with bases and tops below station level
                        # have vertical significance code 11.

                        cloud_top_height = msg.get(f'cloud_height_s4_{idx+1}')

                        # Sometimes in section 4 the cloud height is omitted,
                        # so we need to check it exists before comparing it to
                        # the station height below
                        if cloud_top_height is not None:

                            if cloud_top_height > int(station_height):
                                vs_s4 = 10
                            else:
                                vs_s4 = 11

                            # NOTE: Some of the ecCodes keys are used in
                            # the above, so we must add 'num_s3_clouds'
                            s4_mappings = [
                                {"eccodes_key": (
                                    f"#{idx+num_s3_clouds+8}"
                                    "#verticalSignificanceSurfaceObservations"
                                ),
                                    "value": f"const:{vs_s4}"},
                                {"eccodes_key":
                                    f"#{idx+num_s3_clouds+3}#cloudAmount",
                                    "value": f"data:cloud_amount_s4_{idx+1}",
                                    "valid_min": "const:0",
                                    "valid_max": "const:8"},
                                {"eccodes_key":
                                    f"#{idx+num_s3_clouds+5}#cloudType",
                                    "value": f"data:cloud_genus_s4_{idx+1}"},
                                {"eccodes_key":
                                    f"#{idx+1}#heightOfTopOfCloud",
                                    "value": f"data:cloud_height_s4_{idx+1}"},
                                {"eccodes_key":
                                    f"#{idx+1}#cloudTopDescription",
                                    "value": f"data:cloud_top_s4_{idx+1}"}
                            ]
                            for m in s4_mappings:
                                mapping.update(m)
                except Exception as e:
                    LOGGER.error(e)
                    LOGGER.error(f"Missing station height for station {tsi}")
                    error_msgs.append(
                        f"Missing station height for station {tsi}")
                    conversion_success[tsi] = False

            if conversion_success[tsi]:
                # At this point we have a dictionary for the data, a
                # dictionary of the mappings and the metadata
                # The last step is to convert to BUFR.
                unexpanded_descriptors = [301150, bufr_template]
                short_delayed_replications = []
                # update replications
                delayed_replications = [max(1, num_s3_clouds),
                                        max(1, num_s4_clouds)]
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
                    LOGGER.error(e)
                    LOGGER.error("Error creating BUFRMessage")
                    error_msgs.append(str(e))
                    error_msgs.append("Error creating BUFRMessage")
                    conversion_success[tsi] = False

            if conversion_success[tsi]:
                # Parse
                try:
                    # Parse to BUFRMessage object
                    message.parse(msg, mapping)
                except Exception as e:
                    LOGGER.error(e)
                    LOGGER.error("Error parsing message")
                    error_msgs.append(str(e))
                    error_msgs.append("Error parsing message")
                    conversion_success[tsi] = False

            if conversion_success[tsi]:
                # Convert to BUFR

                # Use WSI and observation date as identifier
                isodate = message.get_datetime().strftime('%Y%m%dT%H%M%S')

                # Write message to CSV object in memory
                try:
                    csv_object = StringIO()
                    dict_writer = csv.DictWriter(csv_object, msg.keys())

                    # Add headers
                    dict_writer.writeheader()

                    # Write data to rows
                    dict_writer.writerow(msg)

                    # Get string from CSV object
                    csv_string = csv_object.getvalue()
                except Exception:
                    LOGGER.warning(
                        f"Unable to write report of station {tsi} to CSV")
                    warning_msgs.append(f"Unable to write report of station {tsi} to CSV")  # noqa

                try:
                    result["bufr4"] = message.as_bufr()  # encode to BUFR
                    status = {"code": PASSED,
                              "warnings": warning_msgs,
                              "errors": error_msgs}

                except Exception as e:
                    LOGGER.error("Error encoding BUFR, null returned")
                    error_msgs.append("Error encoding BUFR, null returned")
                    LOGGER.error(e)
                    error_msgs.append(str(e))
                    result["bufr4"] = None
                    status = {
                        "code": FAILED,
                        "message": "Error encoding, BUFR set to None",
                        "warnings": warning_msgs,
                        "errors": error_msgs
                    }
                    conversion_success[tsi] = False

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
                        "originating_centre":
                        message.get_element("bufrHeaderCentre"),
                        "data_category": message.get_element("dataCategory")
                    },
                    "result": status,
                    "template": bufr_template,
                    "csv": csv_string
                }
            # If there were errors before conversion to BUFR, yield
            # an object with the _meta key
            else:
                result = {
                    "_meta": {
                        "id": None,
                        "geometry": None,
                        "properties": {
                            "md5": None,
                            "wigos_station_identifier": None,
                            "datetime": None,
                            "originating_centre": None,
                            "data_category": None
                        },
                        "result": {
                            "code": FAILED,
                            "message": "Error encoding, BUFR set to None",
                            "warnings": warning_msgs,
                            "errors": error_msgs
                        },
                        "template": None,
                        "csv": None
                    }
                }

            # Now yield result back to caller
            yield result

            # Reset warning and error messages array for next iteration
            warning_msgs = []
            error_msgs = []

            # Output conversion status to user
            if conversion_success[tsi]:
                LOGGER.info(f"Station {tsi} report converted")
            else:
                LOGGER.info(f"Station {tsi} report failed to convert")

        # calculate number of successful conversions
        conversion_count = sum(tsi for tsi in conversion_success.values())

        # Log number of messages converted
        LOGGER.info((f"{conversion_count} / {len(messages)}"
                    " reports converted successfully"))
