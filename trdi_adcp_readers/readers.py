import numpy as np
import dask.array as darr
from dask import compute, delayed
from dask.bag import from_delayed
from pandas import Timedelta
from xarray import Variable, IndexVariable, DataArray, Dataset
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


def _alloc_timestamp(item):
    if type(item)==dict:
        return item['timestamp']
    else:
        return item # NaNs marking bad ensembles.


def _alloc_timestamp_parts(part): # Each partition is an array of dicts.
    return np.array([ens['timestamp'] for ens in part if type(ens)==dict]) # Exclude NaNs marking bad ensembles.


@delayed
def _addtarr(t, dt):
    return np.array([tn + dt for tn in t])


def _alloc_1dvar(ensblk, group='variable_leader_janus', varname=''):
    arrs = []
    for ensarr in ensblk:
        if type(ensarr)==dict:
            arrs.append(ensarr[group][varname])
        else:
            continue

    return darr.array(arrs)


# def alloc_2dvars(ensarr):
#         vjanus = ensarr['velocity_janus']['data']
#         b1[:, n] = vjanus[:, 0]
#         b2[:, n] = vjanus[:, 1]
#         b3[:, n] = vjanus[:, 2]
#         b4[:, n] = vjanus[:, 3]
#         # b5[:, n] = ensarr['velocity_beam5']['data'].squeeze()
#         # corjanus = ensarr['correlation_janus']['data']
#         # cor1[:, n] = corjanus[:, 0]
#         # cor2[:, n] = corjanus[:, 1]
#         # cor3[:, n] = corjanus[:, 2]
#         # cor4[:, n] = corjanus[:, 3]
#         # cor5[:, n] = ensarr['correlation_beam5']['data'].squeeze()
#         # intjanus = ensarr['echo_intensity_janus']['data']
#         # int1[:, n] = intjanus[:, 0]
#         # int2[:, n] = intjanus[:, 1]
#         # int3[:, n] = intjanus[:, 2]
#         # int4[:, n] = intjanus[:, 3]
#         # int5[:, n] = ensarr['echo_intensity_beam5']['data'].squeeze()


def ensembles2dataset_dask(ensdict, ncfpath, dsattrs={}, chunks=10,
                           verbose=True, print_every=1000):
    """
    Convert a dictionary of ensembles into an xarray Dataset object
    using dask.delayed to keep memory usage feasible.
    """
    mms2ms = 1e-3
    n=0
    ensdict_aux = ensdict[0].compute()
    # fbadens = np.array(ensdict_aux)==None
    # nt = len(ensdict) - np.sum(fbadens)

    ensdict0 = None
    while ensdict0 is None:
        ensdict0 = ensdict_aux[n]
        n+=1
    nz = ensdict0['fixed_leader_janus']['number_of_cells']

    fixj = ensdict0['fixed_leader_janus']
    fix5 = ensdict0['fixed_leader_beam5']

    # Add ping offset to get beam 5's timestamps.
    dt5 = fix5['ping_offset_time'] # In milliseconds.
    dt5 = np.array(Timedelta(dt5, unit='ms'))

    th = fixj['beam_angle']
    assert th==25 # Always 25 degrees.
    th = th*np.pi/180.
    Cth = np.cos(th)

    # Scale heading, pitch and roll by 0.01. Sentinel V manual, p. 259.
    phisc = 0.01
    # Construct along-beam/vertical axes.
    cm2m = 1e-2
    r1janus = fixj['bin_1_distance']*cm2m
    r1b5 = fix5['bin_1_distance']*cm2m
    ncj = fixj['number_of_cells']
    nc5 = fix5['number_of_cells']
    lcj = fixj['depth_cell_length']*cm2m
    lc5 = fix5['depth_cell_length']*cm2m
    Lj = ncj*lcj # Distance from center of bin 1 to the center of last bin (Janus).
    L5 = nc5*lc5 # Distance from center of bin 1 to the center of last bin (beam 5).

    rb = r1janus + np.arange(0, Lj, lcj) # Distance from xducer head
                                         # (Janus).
    zab = Cth*rb                         # Vertical distance from xducer head
                                         # (Janus).
    zab5 = r1b5 + np.arange(0, L5, lc5)  # Distance from xducer head, also
                                         # depth for the vertical beam.

    rb = IndexVariable('z', rb, attrs={'units':'meters', 'long_name':"along-beam distance from the xducer's face to the center of the bins, for beams 1-4 (Janus)"})
    zab = IndexVariable('z', zab, attrs={'units':'meters', 'long_name':"vertical distance from the instrument's head to the center of the bins, for beams 1-4 (Janus)"})
    zab5 = IndexVariable('z', zab5, attrs={'units':'meters', 'long_name':"vertical distance from xducer face to the center of the bins, for beam 5 (vertical)"})

    ensdict = from_delayed(ensdict)
    tjanus = ensdict.map_partitions(_alloc_timestamp_parts)
    t5 = _addtarr(tjanus, dt5)

    if verbose: print("Unpacking timestamps.")
    time = IndexVariable('time', tjanus.compute(), attrs={'long_name':'timestamps for beams 1-4 (Janus)'})
    time5 = IndexVariable('time5', t5.compute(), attrs={'long_name':'timestamps for beam 5 (vertical)'})
    if verbose: print("Done unpacking timestamps.")

    coords0 = [('time', time)]
    coords = [('z', zab), ('time', time)]
    coords5 = [('z5', zab5), ('time5', time5)]
    dims = ['z', 'time']
    dims0 = ['time']
    coordsdict = dict(time=('time', time))

    # Load 1d variables into memory to
    # be able to put them in a DataArray.
    # Heading, pitch and roll first.
    vars1d = dict()
    if verbose: print("Saving 1D variables.")
    for varname in ['heading', 'pitch', 'roll']:
        aux = ensdict.map_partitions(_alloc_1dvar, group='variable_leader_janus', varname=varname)
        vars1d.update({varname:DataArray(np.array(aux.compute())*phisc, coords=coords0, dims=dims0, attrs=dict(units='degrees'))})

    # Open an xarray.Dataset in delayed mode and
    # save 1D variables in a single compute cycle.
    if verbose: print("Opening netCDF file: %s"%ncfpath)
    ds1d = Dataset(data_vars=vars1d, coords=coordsdict)
    ds1d = ds1d.to_netcdf(ncfpath, compute=False, mode='w')
    ds1d.compute()
    if verbose: print("Done saving 1D variables.")
    embed()

    # Load 2d variables into memory to
    # be able to put them in a DataArray.
    # coordsdict = dict(z=('z', zab), time=('time', time))


    # ds2d = Dataset(data_vars=vars2d, coords=coordsdict)
    # ds2d = ds2d.to_netcdf(ncfpath, compute=False, mode='a')
    # ds2d.compute()






    long_names = ('Beam 1 velocity', 'Beam 2 velocity',
             'Beam 3 velocity', 'Beam 4 velocity',
             'Beam 5 velocity',
             'Beam 1 correlation', 'Beam 2 correlation',
             'Beam 3 correlation', 'Beam 4 correlation',
             'Beam 5 correlation',
             'Beam 1 echo amplitude', 'Beam 2 echo amplitude',
             'Beam 3 echo amplitude', 'Beam 4 echo amplitude',
             'Beam 5 echo amplitude',
             'heading', 'pitch', 'roll')
    units = ('m/s, positive toward xducer face',
             'm/s, positive toward xducer face',
             'm/s, positive toward xducer face',
             'm/s, positive toward xducer face',
             'm/s, positive toward xducer face',
             'no units', 'no units', 'no units', 'no units',
             'no units',
             'dB', 'dB', 'dB', 'dB',
             'dB',
             'degrees', 'degrees', 'degrees')
    names = ('b1', 'b2', 'b3', 'b4', 'b5',
             'cor1', 'cor2', 'cor3', 'cor4', 'cor5',
             'int1', 'int2', 'int3', 'int4', 'int5',
             'phi1', 'phi2', 'phi3')
    # data_vars = {}


    #
    # sk = darr.zeros((nz, nt), chunks=chunks)*np.nan # Beam vels stored in mm/s
    #                                   # as int64 to save memory.
    # b1, b2, b3, b4 = sk.copy(), sk.copy(), sk.copy(), sk.copy()
    # # embed()
    # sk0 = darr.zeros(nt, chunks=chunks)*np.nan
    # cor1, cor2, cor3, cor4 = sk.copy(), sk.copy(), sk.copy(), sk.copy()
    # int1, int2, int3, int4 = sk.copy(), sk.copy(), sk.copy(), sk.copy()
    # b5, cor5, int5 = sk.copy(), sk.copy(), sk.copy()
    # heading, pitch, roll = sk0.copy(), sk0.copy(), sk0.copy()
    # tjanus = []

    # ensdict = np.array(ensdict)[~fbadens]
    # ensdict = ensdict.tolist()

    # Convert velocities to m/s.
    # b1, b2, b3, b4, b5 = b1*mms2ms, b2*mms2ms, b3*mms2ms, b4*mms2ms, b5*mms2ms

    # Scale heading, pitch and roll. Sentinel V manual, p. 259.

    heading *= phisc
    pitch *= phisc
    roll *= phisc

    arrs = (b1, b2, b3, b4, b5,
            cor1, cor2, cor3, cor4, cor5,
            int1, int2, int3, int4, int5,
            heading, pitch, roll)
            # pressure, temperature, salinity, soundspeed)

    for arr,name,long_name,unit in zip(arrs,names,long_names,units):

        if 'Beam5' in long_name:
            coordsn = coords5
            dimsn = dims
        elif 'phi' in name:
            coordsn = coords0
            dimsn = dims0
        else:
            coordsn = coords
            dimsn = dims

        da = DataArray(arr, coords=coordsn, dims=dimsn, attrs=dict(units=unit, long_name=long_name))
        data_vars.update({name:da})

    allcoords = {'rb':rb} # Along-beam distance for slanted beams.
    allcoords.update(coords)
    allcoords.update(coords5)
    ds = Dataset(data_vars=data_vars, coords=allcoords, attrs=dsattrs)

    return ds


def ensembles2dataset(ensdict, dsattrs={}, verbose=False, print_every=1000):
    """
    Convert a dictionary of ensembles into an xarray Dataset object.
    """
    mms2ms = 1e-3
    fbadens = np.array([not isinstance(ens, dict) for ens in ensdict])
    nt = len(ensdict) - np.sum(fbadens)
    n=0
    ensdict0 = None
    while ensdict0 is None:
        ensdict0 = ensdict[n]
        n+=1
    nz = ensdict0['fixed_leader_janus']['number_of_cells']
    sk = np.ma.zeros((nz, nt))*np.nan # Beam vels stored in mm/s
                                      # as int64 to save memory.
    b1, b2, b3, b4 = sk.copy(), sk.copy(), sk.copy(), sk.copy()
    sk0 = np.ma.zeros(nt)*np.nan
    cor1, cor2, cor3, cor4 = sk.copy(), sk.copy(), sk.copy(), sk.copy()
    int1, int2, int3, int4 = sk.copy(), sk.copy(), sk.copy(), sk.copy()
    b5, cor5, int5 = sk.copy(), sk.copy(), sk.copy()
    heading, pitch, roll = sk0.copy(), sk0.copy(), sk0.copy()
    tjanus = []

    ensdict = np.array(ensdict)[~fbadens]
    ensdict = ensdict.tolist()
    n=0
    for ensarr in ensdict:
        tjanus.append(ensarr['timestamp'])
        heading[n] = ensarr['variable_leader_janus']['heading']
        pitch[n] = ensarr['variable_leader_janus']['pitch']
        roll[n] = ensarr['variable_leader_janus']['roll']
        vjanus = ensarr['velocity_janus']['data']
        b1[:, n] = vjanus[:, 0]
        b2[:, n] = vjanus[:, 1]
        b3[:, n] = vjanus[:, 2]
        b4[:, n] = vjanus[:, 3]
        b5[:, n] = ensarr['velocity_beam5']['data'].squeeze()
        corjanus = ensarr['correlation_janus']['data']
        cor1[:, n] = corjanus[:, 0]
        cor2[:, n] = corjanus[:, 1]
        cor3[:, n] = corjanus[:, 2]
        cor4[:, n] = corjanus[:, 3]
        cor5[:, n] = ensarr['correlation_beam5']['data'].squeeze()
        intjanus = ensarr['echo_intensity_janus']['data']
        int1[:, n] = intjanus[:, 0]
        int2[:, n] = intjanus[:, 1]
        int3[:, n] = intjanus[:, 2]
        int4[:, n] = intjanus[:, 3]
        int5[:, n] = ensarr['echo_intensity_beam5']['data'].squeeze()

        n+=1
        if verbose and not n%print_every: print(n)

    fixj = ensdict0['fixed_leader_janus']
    fix5 = ensdict0['fixed_leader_beam5']

    # Add ping offset to get beam 5's timestamps.
    dt5 = fix5['ping_offset_time'] # In milliseconds.
    dt5 = np.array(Timedelta(dt5, unit='ms'))
    t5 = tjanus + dt5

    th = fixj['beam_angle']
    assert th==25 # Always 25 degrees.
    th = th*np.pi/180.
    Cth = np.cos(th)

    # Construct along-beam/vertical axes.
    cm2m = 1e-2
    r1janus = fixj['bin_1_distance']*cm2m
    r1b5 = fix5['bin_1_distance']*cm2m
    ncj = fixj['number_of_cells']
    nc5 = fix5['number_of_cells']
    lcj = fixj['depth_cell_length']*cm2m
    lc5 = fix5['depth_cell_length']*cm2m
    Lj = ncj*lcj # Distance from center of bin 1 to the center of last bin (Janus).
    L5 = nc5*lc5 # Distance from center of bin 1 to the center of last bin (beam 5).

    rb = r1janus + np.arange(0, Lj, lcj) # Distance from xducer head
                                         # (Janus).
    zab = Cth*rb                         # Vertical distance from xducer head
                                         # (Janus).
    zab5 = r1b5 + np.arange(0, L5, lc5)  # Distance from xducer head, also
                                         # depth for the vertical beam.

    rb = IndexVariable('z', rb, attrs={'units':'meters', 'long_name':"along-beam distance from the xducer's face to the center of the bins, for beams 1-4 (Janus)"})
    zab = IndexVariable('z', zab, attrs={'units':'meters', 'long_name':"vertical distance from the instrument's head to the center of the bins, for beams 1-4 (Janus)"})
    zab5 = IndexVariable('z', zab5, attrs={'units':'meters', 'long_name':"vertical distance from xducer face to the center of the bins, for beam 5 (vertical)"})
    time = IndexVariable('time', tjanus, attrs={'long_name':'timestamp for beams 1-4 (Janus)'})
    time5 = IndexVariable('time', t5, attrs={'long_name':'timestamp for beam 5 (vertical)'})

    coords0 = [('time', time)]
    coords = [('z', zab), ('time', time)]
    coords5 = [('z5', zab5), ('time5', time5)]
    dims = ['z', 'time']
    dims0 = ['time']

    # Convert velocities to m/s.
    b1, b2, b3, b4, b5 = b1*mms2ms, b2*mms2ms, b3*mms2ms, b4*mms2ms, b5*mms2ms

    # Scale heading, pitch and roll. Sentinel V manual, p. 259.
    phisc = 0.01
    heading *= phisc
    pitch *= phisc
    roll *= phisc

    arrs = (b1, b2, b3, b4, b5,
            cor1, cor2, cor3, cor4, cor5,
            int1, int2, int3, int4, int5,
            heading, pitch, roll)
            # pressure, temperature, salinity, soundspeed)
    long_names = ('Beam 1 velocity', 'Beam 2 velocity',
             'Beam 3 velocity', 'Beam 4 velocity',
             'Beam 5 velocity',
             'Beam 1 correlation', 'Beam 2 correlation',
             'Beam 3 correlation', 'Beam 4 correlation',
             'Beam 5 correlation',
             'Beam 1 echo amplitude', 'Beam 2 echo amplitude',
             'Beam 3 echo amplitude', 'Beam 4 echo amplitude',
             'Beam 5 echo amplitude',
             'heading', 'pitch', 'roll')
    units = ('m/s, positive toward xducer face',
             'm/s, positive toward xducer face',
             'm/s, positive toward xducer face',
             'm/s, positive toward xducer face',
             'm/s, positive toward xducer face',
             'no units', 'no units', 'no units', 'no units',
             'no units',
             'dB', 'dB', 'dB', 'dB',
             'dB',
             'degrees', 'degrees', 'degrees')
    names = ('b1', 'b2', 'b3', 'b4', 'b5',
             'cor1', 'cor2', 'cor3', 'cor4', 'cor5',
             'int1', 'int2', 'int3', 'int4', 'int5',
             'phi1', 'phi2', 'phi3')
    data_vars = {}
    for arr,name,long_name,unit in zip(arrs,names,long_names,units):

        if 'Beam5' in long_name:
            coordsn = coords5
            dimsn = dims
        elif 'phi' in name:
            coordsn = coords0
            dimsn = dims0
        else:
            coordsn = coords
            dimsn = dims

        if 'int' in name:
            arr *= 0.45 # Scale factor for echo intensity, see Sentinel V manual
                        # Sentinel V manual p. 264.

        da = DataArray(arr, coords=coordsn, dims=dimsn, attrs=dict(units=unit, long_name=long_name))
        data_vars.update({name:da})

    allcoords = {'rb':rb} # Along-beam distance for slanted beams.
    allcoords.update(coords)
    allcoords.update(coords5)
    ds = Dataset(data_vars=data_vars, coords=allcoords, attrs=dsattrs)

    return ds


def read_PD0_file(path, header_lines=0, return_pd0=False, all_ensembles=True,
                  format='sentinel', use_dask=True, chunks=1e5,
                  debug=False, verbose=True):
    """Read a TRDI Workhorse or Sentinel V *.pd0 file."""
    pd0_bytes = bytearray()
    with open(path, 'rb') as f:
        pd0_bytes = bytearray(f.read())
    f.close()

    if all_ensembles:
        pd0reader = read_PD0_bytes_ensembles
        kwread = dict(verbose=verbose, use_dask=use_dask, chunks=chunks)
    else:
        pd0reader = read_PD0_bytes
        kwread = dict()

    ret = pd0reader(pd0_bytes, return_pd0=return_pd0, format=format, **kwread)

    if return_pd0:
        data, t, fixed_attrs, BAD_ENS, fbad_ens, errortype_count, pd0_bytes = ret
    else:
        data, t, fixed_attrs, BAD_ENS, fbad_ens, errortype_count = ret

    if verbose:
        nens = len(t)
        nbadens = len(fbad_ens)
        ngoodens = nens - nbadens
        pbadens = 100.*nbadens/nens
        print("")
        print("Skipped  %d/%d  bad ensembles (%.2f%%)."%(nbadens, nens, pbadens))
        print("---Breakdown of dud ensembles---")
        print("*Bad checksums:  %d"%errortype_count['bad_checksum'])
        print("*Could not read ensemble's checksum:  %d"%errortype_count['read_checksum'])
        print("*Could not read ensemble's header:  %d"%errortype_count['read_header'])

    if debug:
        if return_pd0:
            ret = data, t, fixed_attrs, BAD_ENS, fbad_ens, errortype_count, pd0_bytes
        else:
            ret = data, t, fixed_attrs, BAD_ENS, fbad_ens, errortype_count
    else:
        if return_pd0:
            ret = data, t, fixed_attrs, pd0_bytes
        else:
            ret = data, t, fixed_attrs

    return ret


def read_PD0_bytes_ensembles(PD0_BYTES, return_pd0=False, headerid='\x7f\x7f',
                             format='sentinel', use_dask=True, chunks=1e4,
                             verbose=True, print_every=1000):
    """
    Finds the hex positions in the bytearray that identify the header of each
    ensemble. Then read each ensemble into a dictionary and accumulates them
    in a list.
    """
    chunks = int(chunks)
    if format=='workhorse':
        parsepd0 = parse_pd0_bytearray
    elif format=='sentinel':
        parsepd0 = parse_sentinelVpd0_bytearray
    else:
        print('Unknown *.pd0 format')

    # Split segments of the byte array per ensemble.
    ensbytes = PD0_BYTES.split(headerid)
    ensbytes = [headerid + ens for ens in ensbytes] # Prepend header id back.
    ensbytes = ensbytes[1:] # First entry is empty, cap it off.
    nens = len(ensbytes)
    nensm = nens - 1
    fbad_ens = []
    BAD_ENS = []
    # embed()

    # Get timestamps for all ensembles.
    # Note that these timestamps indicate the Janus' (i.e., beams 1-4) pings,
    # which will not necessarily be the same as the vertical beam's timestamp.
    t = np.empty(nens, dtype=object)

    if use_dask:
        DATA = []
        # DATA = darr.empty(nens, chunks=chunks)
        ntotalchunks = nens//chunks
        rem_ens = nens%chunks
        has_tail=rem_ens>0
        if has_tail: ntotalchunks+=1 # Last chunk takes remaining ensembles.
        DATAbuffskel = np.empty(chunks, dtype=object)
        DATAbuff = DATAbuffskel.copy()
        daNaN = darr.array(np.array(np.nan, ndmin=1))
        cont_inchnk=0
    else:
        DATA = np.empty(nens, dtype=object)

    nChecksumError, nReadChecksumError, nReadHeaderError = 0, 0, 0
    cont=0
    cont_inchnk=0
    for ensb in ensbytes:
        try:
            dd = parsepd0(ensb)
            # embed()
            t[cont] = dd['timestamp']
        except (ChecksumError, ReadChecksumError, ReadHeaderError) as E:
            t[cont] = np.nan
            fbad_ens.append(cont) # Store index of bad ensemble.
            # BAD_ENS.append(ens)   # Store bytes of the bad ensemble.

            # Which type of error was it?
            if isinstance(E, ChecksumError):
                nChecksumError += 1
            elif isinstance(E, ReadChecksumError):
                nReadChecksumError += 1
            elif isinstance(E, ReadHeaderError):
                nReadHeaderError += 1

            if use_dask:
                if cont_inchnk==chunks:
                    # DATA = darr.concatenate((DATA, daNaN.copy()))
                    DATA.append(delayed(daNaN.copy()))
                    DATAbuff = DATAbuffskel.copy()
                    cont_inchnk=0
                else:
                    DATAbuff[cont_inchnk] = np.nan
                    cont_inchnk+=1
                    if has_tail and cont==nensm: # Save the last chunk.
                        # DATA = darr.concatenate((DATA, daNaN.copy()))
                        DATA.append(delayed(daNaN.copy()))
            else:
                DATA[cont] = np.nan

            cont+=1
            continue

        if use_dask:
            if cont_inchnk==chunks:
                # DATA = darr.concatenate((DATA, darr.array(DATAbuff)))
                DATA.append(delayed(DATAbuff))
                DATAbuff = DATAbuffskel.copy()
                cont_inchnk=0
                # embed()
            else:
                DATAbuff[cont_inchnk] = dd
                cont_inchnk+=1
                if has_tail and cont==nensm: # Save the last chunk.
                    DATA.append(delayed(DATAbuff))
        else:
            DATA[cont] = dd

        cont+=1
        if verbose and not cont%print_every: print("Ensemble %d"%cont)

    errortype_count = dict(bad_checksum=nChecksumError,
                           read_checksum=nReadChecksumError,
                           read_header=nReadHeaderError)

    # Extract ensemble-independent fields (store in xr.Dataset attributes).
    # fixed_attrs = _pick_misc(DATA) # FIXME
    fixed_attrs = []
    # embed()

    if return_pd0:
        ret = (DATA, t, fixed_attrs, BAD_ENS, fbad_ens, errortype_count, PD0_BYTES)
    else:
        ret = (DATA, t, fixed_attrs, BAD_ENS, fbad_ens, errortype_count)

    return ret


def read_PD0_bytes(pd0_bytes, return_pd0=False, format='sentinel'):
    if format=='workhorse':
        data = parse_pd0_bytearray(pd0_bytes)
    elif format=='sentinel':
        data = parse_sentinelVpd0_bytearray(pd0_bytes)
    else:
        print('Unknown *.pd0 format')

    if return_pd0:
        ret = data, pd0_bytes
    else:
        ret = data

    return ret


def inspect_PD0_file(path, format='sentinelV'):
    """
    Fetches and organizes metadata on instrument setup
    and organizes them in a table.
    """
    raise NotImplementedError()


confparams = ['data_source', # START Header.
              'number_of_bytes',
              'address_offsets',
              'number_of_data_types', # END Header.
              'system_power',         # START fixed_leader_janus.
              'system_configuration_MSB',
              'sensor_source',
              'system_configuration_LSB',
              'system_bandwidth',
              'number_of_cells',
              'pings_per_ensemble',
              'false_target_threshold',
              'serial_number',
              'lag_length',
              'sensor_available',
              'depth_cell_length',
              'beam_angle',
              'error_velocity_threshold',
              'coordinate_transformation_process',
              'heading_bias',
              'transmit_pulse_length',
              'heading_alignment',
              'starting_depth_cell',
              'number_of_beams',
              'low_correlation_threshold',
              'simulation_data_flag',
              'cpu_firmware_version',
              'transmit_lag_distance',
              'ending_depth_cell',
              'minimum_percentage_water_profile_pings',
              'signal_processing_mode',
              'blank_after_transmit',
              'bin_1_distance',    # END fixed_leader_janus.
              'depth_cell_length', # START fixed_leader_beam5.
              'vertical_mode',
              'ping_offset_time',
              'vertical_lag_length',
              'transmit_pulse_length',
              'number_of_cells',
              'bin_1_distance',
              'transmit_code_elements',
              'pings_per_ensemble',      # END fixed_leader_beam5.
              'roll_standard_deviation', # START variable_leader_janus.
              'error_status_word',
              'attitude',
              'contamination_sensor',
              'attitude_temperature',
              'temperature',
              'speed_of_sound',
              'pitch_standard_deviation',
              'pressure_variance',
              'heading_standard_deviation',
              'pressure',
              'transmit_current',
              'ensemble_roll_over',
              'depth_of_transducer',
              'bit_result',
              'ambient_temperature',
              'salinity',
              'pressure_positive',
              'pressure_negative',
              'transmit_voltage', # END variable_leader_janus.
              ]


def _pick_misc(d, confparams=confparams):
    """
    Check whether the configuration parameters change over ensembles.
    If not, replace them with a single value.
    """
    dconfparams = dict()
    d.reverse()
    while d:
        dn = d.pop()
        for group in dn.keys():
            for param in dn[group].keys():
                if param in confparams:
                    dconfparams.update({param:dconfparams[param].extend(dn[group][param])})

    ddesc = np.unique([dnn['descriptors'] for dnn in d if dnn is not None]) # Array of lists.

    if ddesc.size==1: # If all the lists store the exact same strings.
        dconfparams['descriptors'] = ddesc
    else:
        # print("Warning: Some setup parameters changed during deployment.")
        pass

    return dconfparams
