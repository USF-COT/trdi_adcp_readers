
from trdi_adcp_readers.pd15.pd0_converters import (
    PD15_file_to_PD0,
    PD15_string_to_PD0
)
from trdi_adcp_readers.pd0.pd0_parser import parse_pd0_bytearray


def read_PD15_file(path, header_lines=0):
    pd0_bytes = PD15_file_to_PD0(path, header_lines)
    return parse_pd0_bytearray(pd0_bytes)


def read_PD15_string(string):
    pd0_bytes = PD15_string_to_PD0(string)
    return parse_pd0_bytearray(pd0_bytes)


def read_PD0_file(path, header_lines=0):
    pd0_bytes = bytearray()
    with open(path, 'rb') as f:
        for i in xrange(0, header_lines):
            f.readline()

        pd0_bytes = bytearray(f.read())

    return parse_pd0_bytearray(pd0_bytes)


def read_PD0_bytes(pd0_bytes):
    return parse_pd0_bytearray(pd0_bytes)
