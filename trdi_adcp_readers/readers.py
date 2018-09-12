from trdi_adcp_readers.pd15.pd0_converters import (
    PD15_file_to_PD0,
    PD15_string_to_PD0
)
from trdi_adcp_readers.pd0.pd0_parser import parse_pd0_bytearray
from trdi_adcp_readers.pd0.pd0_parser_sentinelV import parse_sentinelVpd0_bytearray


def read_PD15_file(path, header_lines=0, return_pd0=False):
    pd0_bytes = PD15_file_to_PD0(path, header_lines)
    data = parse_pd0_bytearray(pd0_bytes)
    if return_pd0:
        return data, pd0_bytes
    else:
        return data


def read_PD15_hex(hex_string, return_pd0=False):
    pd15_byte_string = hex_string.decode("hex")
    pd0_bytes = PD15_string_to_PD0(pd15_byte_string)
    data = parse_pd0_bytearray(pd0_bytes)
    if return_pd0:
        return data, pd0_bytes
    else:
        return data


def read_PD15_string(string, return_pd0=False):
    pd0_bytes = PD15_string_to_PD0(string)
    data = parse_pd0_bytearray(pd0_bytes)
    if return_pd0:
        return data, pd0_bytes
    else:
        return data


def read_PD0_file(path, header_lines=0, return_pd0=False, format='workhorse'):
    pd0_bytes = bytearray()
    with open(path, 'rb') as f:
        for i in xrange(0, header_lines):
            f.readline()

        pd0_bytes = bytearray(f.read())

    data = read_PD0_bytes(pd0_bytes, return_pd0=return_pd0, format=format)

    if return_pd0:
        return data, pd0_bytes
    else:
        return data


def read_PD0_bytes(pd0_bytes, return_pd0=False, format='workhorse'):
    if format=='workhorse':
        data = parse_pd0_bytearray(pd0_bytes)
    elif format=='sentinelV':
        data = parse_sentinelVpd0_bytearray(pd0_bytes)

    if return_pd0:
        return data, pd0_bytes
    else:
        return data

def inspect_PD0_file(path, format='sentinelV'):
    """
    Fetches and organizes several metadata on instrument setup
    and organizes them in a table.
    """
    raise NotImplementedError()
