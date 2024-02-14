#!/bin/bash

# Navigate to the pymetdecoder directory
cd /local/pymetdecoder

# Uninstall the pymetdecoder package
pip uninstall -y pymetdecoder

# Install the pymetdecoder package from the local setup.py file
python3 setup.py install

# Navigate to the data directory
cd /local/data

# Clear the terminal screen
clear
