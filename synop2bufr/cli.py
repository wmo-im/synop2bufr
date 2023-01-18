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
import click
import logging
import os.path
import sys

from synop2bufr import __version__, transform as transform_synop


def cli_option_verbosity(f):
    logging_options = ["ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"]

    def callback(ctx, param, value):
        if value is not None:
            logging.basicConfig(stream=sys.stdout,
                                level=getattr(logging, value))
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


@click.command()
@click.pass_context
@click.option('--input', 'input', required=False,
              type=click.File(errors="ignore"),
              help="Name/directory of the SYNOP TAC file to convert to BUFR, or alternatively a SYNOP message itself.")  # noqa
@click.option('--metadata', 'metadata', required=False,
              type=click.File(errors="ignore"),
              help="Name/directory of the station metadata.")
@click.option('--output-dir', 'output_dir', required=False,
              help="Directory for the output BUFR files.")
@click.option('--year', 'year', required=False,
              help="Year that the example_data correspond to.")
@click.option('--month', 'month', required=False,
              help="Month that the example_data correspond to.")
@cli_option_verbosity
def transform(ctx, input, metadata, output_dir, year, month, verbosity):

    try:
        result = transform_synop(input.read(), metadata.read(), year, month)
    except Exception as e:
        raise click.ClickException(e)

    for item in result:
        key = item['_meta']["identifier"]
        bufr_filename = f"{output_dir}{os.sep}{key}.bufr4"
        with open(bufr_filename, "wb") as fh:
            fh.write(item["bufr4"])
        click.echo(item)


cli.add_command(transform)
