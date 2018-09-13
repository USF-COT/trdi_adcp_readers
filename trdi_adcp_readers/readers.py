import numpy as np
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


def read_PD0_file(path, header_lines=0, return_pd0=False, all_ensembles=True,
                  format='workhorse'):
    pd0_bytes = bytearray()
    with open(path, 'rb') as f:
        pd0_bytes = bytearray(f.read())

    if all_ensembles:
        pd0reader = read_PD0_bytes_ensembles
    else:
        pd0reader = read_PD0_bytes

    data = pd0reader(pd0_bytes, return_pd0=return_pd0,
                     format=format)

    # Get timestamps for all ensembles.
    # Note that these timestamps indicate the Janus' (i.e., beams 1-4) pings,
    # which will not necessarily be the same as the vertical beam's timestamp.
    t = np.array([data[n]['timestamp'] for n in xrange(len(data))])

    if return_pd0:
        return data, t, pd0_bytes
    else:
        return data, t


def read_PD0_bytes_ensembles(PD0_BYTES, return_pd0=False, headerid='\x7f\x7f',
                             format='workhorse'):
    """
    Finds the hex positions in the bytearray that identify the header of each
    ensemble. Then read each ensemble into a dictionary and accumulates them
    in a list.
    """
    if format=='workhorse':
        parsepd0 = parse_pd0_bytearray
    elif format=='sentinel':
        parsepd0 = parse_sentinelVpd0_bytearray
    else:
        print('Unknown *.pd0 format')

    # Split segments of the byte array per ensemble.
    DATA = []
    ensbytes = PD0_BYTES.split(headerid)
    ensbytes = [headerid + ens for ens in ensbytes] # Prepend header id back.
    ensbytes = ensbytes[1:] # First entry is empty, cap it off.
    _ = [DATA.append(parsepd0(ensb)) for ensb in ensbytes]

    if return_pd0:
        return DATA, PD0_BYTES
    else:
        return DATA


def read_PD0_bytes(pd0_bytes, return_pd0=False, format='workhorse'):
    if format=='workhorse':
        data = parse_pd0_bytearray(pd0_bytes)
    elif format=='sentinel':
        data = parse_sentinelVpd0_bytearray(pd0_bytes)
    else:
        print('Unknown *.pd0 format')

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
