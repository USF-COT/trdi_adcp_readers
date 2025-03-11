from trdi_adcp_readers.pd15.pd0_converters import (
    PD15_file_to_PD0,
    PD15_string_to_PD0
)
from trdi_adcp_readers.pd0.pd0_parser import parse_pd0_bytearray


def read_PD15_file(path, header_lines=0, return_pd0=False):
    pd0_bytes = PD15_file_to_PD0(path, header_lines)
    data = parse_pd0_bytearray(pd0_bytes)
    if return_pd0:
        return data, pd0_bytes
    else:
        return data


def read_PD15_hex(hex_string, return_pd0=False):
    if isinstance(hex_string, str):
        hex_string = hex_string.encode('ascii')
    pd15_byte_string = bytes.fromhex(hex_string.decode('ascii'))
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


def read_PD0_file(path, header_lines=0, return_pd0=False):
    pd0_bytes = bytearray()
    with open(path, 'rb') as f:
        for i in range(0, header_lines):
            f.readline()

        pd0_bytes = bytearray(f.read())

    data = parse_pd0_bytearray(pd0_bytes)
    if return_pd0:
        return data, pd0_bytes
    else:
        return data


def read_PD0_bytes(pd0_bytes, return_pd0=False):
    data = parse_pd0_bytearray(pd0_bytes)
    if return_pd0:
        return data, pd0_bytes
    else:
        return data
