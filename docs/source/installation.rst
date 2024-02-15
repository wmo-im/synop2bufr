.. _installation:

Installation
============
Dependencies
************

The synop2bufr module relies on the `ecCodes <https://confluence.ecmwf.int/display/ECC>`_ software library to perform
the BUFR encoding. This needs to be installed prior to installing any of the Python packages, instructions can
be found on the ecCodes documentation pages: `https://confluence.ecmwf.int/display/ECC <https://confluence.ecmwf.int/display/ECC>`_.

The following Python packages are required by the synop2bufr module:

* `eccodes <https://pypi.org/project/eccodes/>`__ (NOTE: this is separate from the ecCodes library)
* `pymetdecoder <https://github.com/antarctica/pymetdecoder>`__ Python module from the British Antarctic Survey (BAS) to decode the WMO FM-12 SYNOP format. A fork (https://github.com/wmo-im/pymetdecoder) of the module is currently used pending an update to the original.
* `csv2bufr <https://github.com/wmo-im/csv2bufrr>`__ Python module to create BUFR from CSV input based on the ecCodes library.

Additionally, the command line interface to synop2bufr requires:

* `click <https://pypi.org/project/click/>`_


Installation
************

Docker
------
The quickest way to install and run the software is via a Docker image containing all the required
libraries and Python modules:

.. code-block:: shell

   docker pull wmoim/synop2bufr

This installs a `Docker image <https://hub.docker.com/r/wmoim/synop2bufr>`_ based on Ubuntu and includes the ecCodes software library, dependencies noted above
and the synop2bufr module (including the command line interface).

Source
------

Alternatively, synop2bufr can be installed from source. First clone the repository and navigate to the cloned folder / directory:

.. code-block:: bash

   git clone https://github.com/wmo-im/synop2bufr.git
   cd synop2bufr

You can then run synop2bufr from an ecCodes base image as follows:

.. code-block:: bash

   docker run -it -v ${pwd}:/local wmoim/dim_eccodes_baseimage:2.34.0 bash
   apt-get update && apt-get install -y git
   cd /local
   python3 setup.py install
   synop2bufr --help

The above step can be skipped if not using Docker. If not using Docker the module and dependencies needs to be installed:

.. code-block:: bash
   
   python3 setup.py install
   synop2bufr --help

The following output should be shown:

.. code-block:: bash

    Usage: synop2bufr [OPTIONS] COMMAND [ARGS]...

      synop2bufr

    Options:
      --version  Show the version and exit.
      --help     Show this message and exit.

    Commands:
      transform


