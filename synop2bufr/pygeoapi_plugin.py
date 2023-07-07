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
import base64
import json
import logging
from pygeoapi.process.base import BaseProcessor

from synop2bufr import transform

LOGGER = logging.getLogger(__name__)

PROCESS_METADATA = {
    "version": "0.1.0",
    "id": "x-wmo:synop2bufr",
    "title": {"en": "synop2bufr"},
    "description": {"en": "Process to convert FM 12-SYNOP bulletin to BUFR"},
    "keywords": ["SYNOP", "BUFR", "FM 12"],
    "links": [{
        "type": "text/html",
        "rel": "about",
        "title": "homepage",
        "href": "https://github.com/wmo-im/synop2bufr",
        "hreflang": "en-US",
    }],
    "inputs": {
        "data": {
            "title": "FM 12-SYNOP bulletin string",
            "description": "Input FM 12-SYNOP bulletin to convert to BUFR.",
            "schema": {"type": "string"},
            "minOccurs": 1,
            "maxOccurs": 1,
            "metadata": None,
            "keywords": [],
        },
        "metadata":{
            "title": "Station metadata",
            "description": "CSV formatted data containing list of stations required by synop2bufr.",  # noqa
            "schema": {"type": "string"},
            "minOccurs": 1,
            "maxOccurs": 1,
            "metadata": None,
            "keywords": [],
        },
        "year": {
            "title": "Year",
            "description": "Year (UTC) corresponding to FM 12-SYNOP bulletin",
            "schema": {"type": "integer"},
            "minOccurs": 1,
            "maxOccurs": 1,
            "metadata": None,
            "keywords": []
        },
        "month":{
            "title": "Month",
            "description": "Month (UTC) corresponding to FM 12-SYNOP bulletin",
            "schema": {"type": "integer"},
            "minOccurs": 1,
            "maxOccurs": 1,
            "metadata": None,
            "keywords": []
        }
    },
    "outputs": {
        "result": {
            "title": "BUFR encoded data in base64",  # noqa
            "schema": {"contentMediaType": "application/json"}
        },
        "errors": {
            "title": "Errors",
            "schema": {"contentMediaType": "application/json"}
        }
    },
    "example": {
        "inputs": {
            "data": r"AAXX 21121 15015 02999 02501 10103 21090 39765 42952 57020 60001 333 4/000 55310 0//// 22591 3//// 60007 91003 91104=",  # noqa
            "metadata": r"station_name,wigos_station_identifier,traditional_station_identifier,facility_type,latitude,longitude,elevation,territory_name,wmo_region\\nOCNA SUGATAG,0-20000-0-15015,15015,Land (fixed),47.77706163,23.94046026,503,Romania,6",  #noqa
            "year": 2022,
            "month": 2
        },
        "output": {}
    },
}


class processor(BaseProcessor):
    def __init__(self, processor_def):
        """
        Initialize object
        :param processor_def: provider definition
        :returns: pygeoapi.process.csv2bufr.csv2bufr
        """

        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):
        """
        This method is invoked by pygeoapi when this class is set as a
        `process` type resource in pygeoapi config file.

        :return: media_type, json
        """

        mimetype = "application/json"
        errors = []
        bufr = []
        try:
            fm12 = data['data']
            metadata = data['metadata']
            year = data['year']
            month = data['month']
            bufr_generator = transform(data=fm12,
                                       metadata=metadata,
                                       year=year,
                                       month=month)

            # transform returns a generator, we need to iterate over
            # and add to single output object
            for result in bufr_generator:
                # need to convert BUFR binary to base64
                result['bufr4'] = base64.b64encode(result['bufr4']).decode("utf-8")  # noqa
                # convert datetime to string
                result['_meta']['properties']['datetime'] = \
                    result['_meta']['properties']['datetime'].isoformat()
                bufr.append(result)

        except Exception as e:
            LOGGER.error(e)
            errors.append(f"{e}")

        output = {"result": json.dumps(bufr), "errors": json.dumps(errors)}

        return mimetype, output

    def __repr__(self):
        return "<synop2bufr> {}".format(self.name)
