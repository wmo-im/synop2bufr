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

If running in a Docker environment, build the Docker image and run the container:

.. code-block:: bash

   docker build -t synop2bufr .
   docker run -it -v ${pwd}:/app synop2bufr
   cd /app

The above step can be skipped if not using Docker. If not using Docker the module and dependencies needs to be installed:

.. code-block:: bash

   pip3 install -r requirements.txt
   pip3 install --no-cache-dir https://github.com/wmo-im/csv2bufr/archive/refs/tags/v0.3.1.zip
   pip3 install --no-cache-dir https://github.com/wmo-im/pymetdecoder/archive/refs/tags/v0.1.0.zip
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


