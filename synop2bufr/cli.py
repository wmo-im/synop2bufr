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

import logging
import os.path

import click

from synop2bufr import __version__, transform as transform_synop

# Configure logger
LOGGER = logging.getLogger()
log_level = os.environ.get("LOG_LEVEL", "WARNING")
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=getattr(logging, log_level),
    datefmt="%Y-%m-%d %H:%M:%S"
)

# if (LOGGER.hasHandlers()):
#     LOGGER.handlers.clear()

# # Configure error handler
# handler_err = logging.StreamHandler(sys.stderr)
# handler_err.setLevel(logging.ERROR)
# handler_err.setFormatter(logging.Formatter(
#     fmt="%(asctime)s [%(levelname)s] %(message)s",
#     datefmt="%Y-%m-%d %H:%M:%S"
# ))
# LOGGER.addHandler(handler_err)


def cli_option_verbosity(f):
    logging_options = ["ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"]

    def callback(ctx, param, value):
        if value is not None:
            LOGGER.setLevel(getattr(logging, value))
        return True

    return click.option("--verbosity", "-v",
                        type=click.Choice(logging_options),
                        help="Verbosity",
                        callback=callback)(f)


def cli_callbacks(f):
    f = cli_option_verbosity(f)
    return f


@click.group()
@click.version_option(version=__version__)
def cli():
    """synop2bufr"""
    pass


@click.group()
def data():
    """data utilities"""
    pass


@click.command()
@click.pass_context
@click.argument('synop_file', type=click.File(errors="ignore"))
@click.option('--metadata', 'metadata', required=False,
              default="station_list.csv",
              type=click.File(errors="ignore"),
              help="Name/directory of the station metadata")
@click.option('--output-dir', 'output_dir', required=False,
              default=".",
              help="Directory for the output BUFR files")
@click.option('--year', 'year', required=True,
              help="Year that the data corresponds to")
@click.option('--month', 'month', required=True,
              help="Month that the data corresponds to")
@cli_option_verbosity
def transform(ctx, synop_file, metadata, output_dir, year, month, verbosity):

    try:
        # Get content from synop file
        content = synop_file.read()

        # Boolean to know if the decoded CSV has a header
        # or not yet
        header_written = False

        try:
            result = transform_synop(
                content, metadata.read(), year, month
            )

        except Exception as e:
            raise click.ClickException(e)

        for item in result:

            # Return object may not have a datetime if there is an error
            # parsing a report
            if item["_meta"]["properties"].get("datetime") is not None:
                timestamp = item["_meta"]["properties"]["datetime"].strftime(
                    '%Y%m%dT%H%M%S'
                )
                filename = f"decoded_{timestamp}.csv"

                # Write the CSV file of decoded data
                csv_string = item["_meta"]["csv"]

                if header_written:
                    mode = "a"  # Append to file if headers
                else:
                    mode = "w"  # Write to file if no headers

                with open(filename, mode) as f:
                    # Check there was no problem writing the report to CSV
                    if csv_string is not None:
                        if not header_written:
                            # Write the whole string including headers
                            f.write(csv_string)
                            header_written = True
                        else:
                            # Skip the header row of the string
                            f.write(csv_string.split("\n")[1])

                # Check there was no problem encoding the BUFR message
                # before writing to a file
                if item.get("bufr4") is not None:
                    # Write the BUFR file
                    key = item['_meta']["id"]
                    bufr_filename = f"{output_dir}{os.sep}{key}.bufr4"

                    with open(bufr_filename, "wb") as fh:
                        fh.write(item["bufr4"])

    except Exception as e:
        raise click.ClickException(e)


data.add_command(transform)
cli.add_command(data)
