TRDI ADCP Work Horse (wh) Reader
=================

## Overview ##

Reads data from TRDI ADCP work horse data loggers.

Contains methods to convert from PD15 to PD0.  Once in PD0, the data can be parsed into a Python dictionary.

[Sample Output](https://github.com/USF-COT/trdi_adcp_readers/blob/master/sample_output.txt)

## Installation ##

    git clone git://github.com/USF-COT/trdi_adcp_readers.git
    sudo python setup.py install

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
