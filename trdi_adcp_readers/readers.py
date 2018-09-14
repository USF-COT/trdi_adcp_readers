import numpy as np
from trdi_adcp_readers.pd0.pd0_parser_sentinelV import (ChecksumError,
                                                        ReadChecksumError,
                                                        ReadHeaderError)
from trdi_adcp_readers.pd15.pd0_converters import (
    PD15_file_to_PD0,
    PD15_string_to_PD0
)
from trdi_adcp_readers.pd0.pd0_parser import parse_pd0_bytearray
from trdi_adcp_readers.pd0.pd0_parser_sentinelV import parse_sentinelVpd0_bytearray
from IPython import embed


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


def ensdict2xarray(ensdict):
    """
    Convert a dictionary of ensembles into an xarray Dataset object.
    """
    fbadens = np.array(ensdict)==None
    nt = len(ensdict) - np.sum(fbadens)
    nz = ensdict[0]['fixed_leader_janus']['number_of_cells']
    sk = np.ma.ones((nz, nt)) # Beam vels stored in mm/s
                                              # as int64 to save memory.
    b5 = sk.copy()
    ensdict = np.array(ensdict)[~fbadens]
    ensdict = ensdict.tolist()
    n=0
    for ensarr in ensdict:
        b5[:, n] = ensarr['velocity_beam5']['data'].squeeze()
        if not n%1000: print(n)
        n+=1

    return b5


def read_PD0_file(path, header_lines=0, return_pd0=False, all_ensembles=True,
                  format='sentinel', debug=False, verbose=True):
    """Read a TRDI Workhorse or Sentinel V *.pd0 file."""
    pd0_bytes = bytearray()
    with open(path, 'rb') as f:
        pd0_bytes = bytearray(f.read())
    f.close()

    if all_ensembles:
        pd0reader = read_PD0_bytes_ensembles
        kwread = dict(verbose=verbose)
    else:
        pd0reader = read_PD0_bytes
        kwread = dict()

    ret = pd0reader(pd0_bytes, return_pd0=return_pd0, format=format, **kwread)

    if return_pd0:
        data, BAD_ENS, fbad_ens, errortype_count, pd0_bytes = ret
    else:
        data, BAD_ENS, fbad_ens, errortype_count = ret

    if verbose:
        nens = len(data)
        nbadens = len(fbad_ens)
        ngoodens = nens - nbadens
        pbadens = 100.*nbadens/nens
        print("")
        print("Skipped  %d/%d  bad ensembles (%.2f%%)."%(nbadens, nens, pbadens))
        print("---Breakdown of dud ensembles---")
        print("*Bad checksums:  %d"%errortype_count['bad_checksum'])
        print("*Could not read ensemble's checksum:  %d"%errortype_count['read_checksum'])
        print("*Could not read ensemble's header:  %d"%errortype_count['read_header'])

    # Get timestamps for all ensembles.
    # Note that these timestamps indicate the Janus' (i.e., beams 1-4) pings,
    # which will not necessarily be the same as the vertical beam's timestamp.
    t = np.array([ens['timestamp'] for ens in data if ens is not None])
    # t5 = np.array([ens['timestamp5'] for ens in data if ens is not None])

    if debug:
        if return_pd0:
            ret = data, t, BAD_ENS, fbad_ens, errortype_count, pd0_bytes
        else:
            ret = data, t, BAD_ENS, fbad_ens, errortype_count
    else:
        if return_pd0:
            ret = data, t, pd0_bytes
        else:
            ret = data, t

    return ret


def read_PD0_bytes_ensembles(PD0_BYTES, return_pd0=False, headerid='\x7f\x7f',
                             format='workhorse',
                             verbose=True, print_every=1000):
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
    cont=0
    fbad_ens = []
    BAD_ENS = []
    nChecksumError, nReadChecksumError, nReadHeaderError = 0, 0, 0
    for ensb in ensbytes:
        try:
            dd = parsepd0(ensb)
        except (ChecksumError, ReadChecksumError, ReadHeaderError) as E:
            fbad_ens.append(cont) # Store index of bad ensemble.
            BAD_ENS.append(ens)   # Store bytes of the bad ensemble.

            # Which type of error was it?
            if isinstance(E, ChecksumError):
                nChecksumError += 1
            elif isinstance(E, ReadChecksumError):
                nReadChecksumError += 1
            elif isinstance(E, ReadHeaderError):
                nReadHeaderError += 1

            DATA.append(None)
            cont+=1
            continue

        DATA.append(dd)
        cont+=1
        if verbose and not cont%print_every:
            print("Ensemble %d"%cont)

    errortype_count = dict(bad_checksum=nChecksumError,
                           read_checksum=nReadChecksumError,
                           read_header=nReadHeaderError)


    if return_pd0:
        ret = (DATA, BAD_ENS, fbad_ens, errortype_count, PD0_BYTES)
    else:
        ret = (DATA, BAD_ENS, fbad_ens, errortype_count)

    return ret


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
