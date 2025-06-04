TRDI ADCP Work Horse (wh) Reader
=================

## Overview ##

Reads data from TRDI ADCP work horse data loggers.

Contains methods to convert from PD15 to PD0.  Once in PD0, the data can be parsed into a Python dictionary.

[Sample Output](https://github.com/USF-COT/trdi_adcp_readers/blob/master/sample_output.txt)

## Installation ##

    pip install trdi-adcp-readers

## Command-line Tools ##

This package provides the following command-line tools after installation:

- `convert_trdi`: Converts a binary PD15 or PD0 TRDI ADCP file to CSV files
- `convert_trdi_uhi`: Converts a binary PD15 or PD0 TRDI ADCP file to University of Hawaii (UHI) format CSV files

## Test ##

From the root directory run tests.py to test against a known PD0 and PD15 file

    python tests/tests.py

## Example ##

### Parse PD0 Data File ###

    from trdi_adcp_readers.pd0.pd0_parser import parse_pd0_bytearray
    pd0 = ''
    with open(<path to PD0 file>, 'rb') as f:
        pd0 = f.read()
    pd0_bytes = bytearray(pd0)
    data = parse_pd0_bytearray(pd0_bytes)

### Parse PD15 Data File ###

    from trdi_adcp_readers.pd15.pd0_converters import PD15_file_to_PD0
    from trdi_adcp_readers.pd0.pd0_parser import parse_pd0_bytearray
    pd0 = PD15_file_to_PD0('./140B97C6', header_lines=2)
    data = parse_pd0_bytearray(pd0)

Note that this example uses the included file 140B97C6.  This file is a test GOES file transmitted by an in-shore [COMPS](http://comps.marine.usf.edu/) station.  This station is located in a shallow area.  Only data from the first few cells is valid.  Transmissions from this station include a GOES header and an empty line before the PD15 data.  The PD15 converter skips these first two lines using the argument header\_lines=2.

### Convert ADCP Data to UHI Format ###

The package includes a command-line utility for converting TRDI ADCP data (PD0 or PD15) to University of Hawaii's (UHI) format, which consists of three CSV files:

    convert_trdi_uhi --format pd0 --headers 0 <input_file> <info_output> <velocity_output> <data_output>

Example:

    convert_trdi_uhi --format pd0 --headers 0 tests/data/1407E0CA.PD0 1407E0CA_info.txt 1407E0CA_velocity.txt 1407E0CA_data.txt

You can also run it as a module:

    python -m trdi_adcp_readers.scripts.convert_trdi_uhi --format pd0 --headers 0 <input_file> <info_output> <velocity_output> <data_output>

Command options:

* `--format pd0|pd15`: Specifies the input file format (pd0 or pd15). If not provided, the script will attempt to determine format from file extension.
* `--headers N`: Number of header lines to skip before parsing data (default: 0).
* `--mag-declination VALUE`: Magnetic declination in degrees (default: 0.0).

Output files:
* `info_output`: Contains header information about the ADCP deployment.
* `velocity_output`: Contains water depth and velocity data for each bin.
* `data_output`: Contains correlation and echo amplitude data for each bin.
