TRDI ADCP Work Horse (wh) Reader
=================

## Overview ##

Reads data from TRDI ADCP work horse data loggers.

Contains methods to convert from PD15 to PD0.  Once in PD0, the data can be parsed into a Python dictionary.

## Example ##

    from trdi_adcp_readers.pd15.pd0_converters import PD15_file_to_PD0
    from trdi_adcp_readers.pd0.pd0_parser import parse_pd0_bytearray
    pd0 = PD15_file_to_PD0('./140B97C6', header_lines=2)
    data = parse_pd0_bytearray(pd0)

