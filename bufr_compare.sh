#!/bin/bash

# Reads the elements of blacklist.txt as an array
keys="bufrHeaderCentre,dataSubCategory,masterTablesVersionNumber,stationOrSiteName,\
wigosIdentifierSeries,delayedDescriptorReplicationFactor,\
wigosIssuerOfIdentifier,wigosIssueNumber,wigosLocalIdentifierCharacter,wigosIssuerOfIdentifier,\
latitude,longitude,heightOfStationGroundAboveMeanSeaLevel,\
heightOfBarometerAboveMeanSeaLevel,heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform,\
unexpandedDescriptors,instrumentationForWindMeasurement"

# Ask the user for the tsi
read -p "TSI:" tsi

# Find file names
file1="./output-bufr/WIGOS_0-20000-0-${tsi}_20220321T120000.bufr4"
file2="./reference-bufr/${tsi}.bin"

# Run command
bufr_compare -b "$keys" $file1 $file2



