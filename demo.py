# Import synop2bufr
from synop2bufr import to_bufr

# Write the file name
file = "data/A_SMRO01YRBK211200_C_EDZW_20220321120500_12524785.txt"

metadata = "metadata.csv"

# Convert the file to BUFR
to_bufr(file, metadata)
