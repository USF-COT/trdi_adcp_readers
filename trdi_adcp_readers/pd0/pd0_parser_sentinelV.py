import numpy as np
import struct
from datetime import datetime
import sys
from os import system
# from IPython import embed


def unpack_bytes(pd0_bytes, data_format_tuples, offset, verbose=False):
    data = {}
    for fmt in data_format_tuples:
        try:
            struct_offset = offset+fmt[2]
            size = struct.calcsize(fmt[1])
            data_bytes = buffer(pd0_bytes, struct_offset, size)
            data[fmt[0]] = (
                struct.unpack(fmt[1], data_bytes)[0]
            )
        except Exception:
            if verbose:
                print('Error parsing %s with the arguments '
                      '%s, offset:%d size:%d' % (fmt[0], fmt[1],
                                                 struct_offset,
                                                 size))
                # embed()
                pass

    return data


def parse_fixed_header(pd0_bytes):
    """
    Parses the fixed part of the header (up to byte #6).
    TRDI Sentinel V manual, p. 250.
    """
    header_data_format = (
        ('id', 'B', 0),
        ('data_source', 'B', 1),
        ('number_of_bytes', '<H', 2),
        ('spare', 'B', 4),
        ('number_of_data_types', 'B', 5)
    )

    return unpack_bytes(pd0_bytes, header_data_format, 0)


def parse_address_offsets(pd0_bytes, num_datatypes, offset=6):
    """
    Parse address offsets (in bytes) for each variable.
    This information is in the header, but changes with
    deployment, instrument configuration, etc.
    See TRDI Sentinel V manual, p. 250.
    """
    address_data = []
    for bytes_start in xrange(offset, offset+(num_datatypes*2), 2):
        try:
            buff = buffer(pd0_bytes, bytes_start, 2)
            data = (struct.unpack('<H', buff)[0])
        except:
            # embed()
            pass
        address_data.append(data)

    return address_data


#----- Parsers for beam 5 data.

def parse_fixed_leader5(pd0_bytes, offset, data):
    """
    Same as the function 'parse_fixed_leader()', but for
    the vertical beam. See Sentinel V manual, p. 277-278.
    """
    fixed_leader5_format = (
        ('id', '<H', 0),
        ('number_of_cells', '<H', 2),
        ('pings_per_ensemble', '<H', 4),
        ('depth_cell_length', '<H', 6),
        ('bin_1_distance', '<H', 8),
        ('vertical_mode', '<H', 10),
        ('transmit_pulse_length', '<H', 12),
        ('vertical_lag_length', '<H', 14),
        ('transmit_code_elements', '<H', 16),
        ('ping_offset_time', '<H', 30)
        )

    return unpack_bytes(pd0_bytes, fixed_leader5_format, offset)


def parse_velocity5(pd0_bytes, offset, data, fbad=-32768):
    velocity_format = (('id', '<H', 0),)

    velocity_data5 = unpack_bytes(pd0_bytes, velocity_format,
                                  offset)
    offset += 2  # Move past id field
    ncells = data['fixed_leader_beam5']['number_of_cells']
    vel = parse_per_cell_beam5(pd0_bytes, offset, ncells, '<h')
    vel = np.array(vel)
    vel = np.ma.masked_where(vel==fbad, vel)
    velocity_data5['data'] = vel

    return velocity_data5


def parse_correlation5(pd0_bytes, offset, data):
    correlation_format = (('id', '<H', 0),)

    correlation_data5 = unpack_bytes(pd0_bytes, correlation_format,
                                     offset)
    offset += 2
    ncells = data['fixed_leader_janus']['number_of_cells']
    corr = parse_per_cell_beam5(pd0_bytes, offset, ncells, 'B')
    correlation_data5['data'] = np.array(corr)

    return correlation_data5


def parse_echo_intensity_beam5(pd0_bytes, offset, data):
    echo_intensity_format = (('id', '<H', 0),)

    echo_intensity_data5 = unpack_bytes(pd0_bytes,
                                        echo_intensity_format,
                                        offset)
    offset += 2
    ncells = data['fixed_leader_janus']['number_of_cells']
    echo = parse_per_cell_beam5(pd0_bytes, offset, ncells, 'B')
    echo_intensity_data5['data'] = np.array(echo)

    return echo_intensity_data5


def parse_percent_good5():
    raise NotImplementedError()


def parse_eventlog(pd0_bytes, offset, data):
    raise NotImplementedError()

#----- Parsers for beams 1-4.

def parse_fixed_leader(pd0_bytes, offset, data):
    """
    Parses the variables that do not change with time since
    the start of deployment. The format depends on the type
    of *.pd0 file (Workhorse or Sentinel V). This format is for
    Sentinel V *.pd0 data and follows Table 37 on the TRDI
    Sentinel V manual, p. 251-255.
    """
    fixed_leader_format = (
        ('id', '<H', 0),
        ('cpu_firmware_version', 'B', 2),
        ('cpu_firmware_revision', 'B', 3),
        ('system_configuration_LSB', 'B', 4), # Identifies model and beam orientation (up/down).
        ('system_configuration_MSB', 'B', 5), # Identifies 4 or 5 beam Janus.
        ('simulation_data_flag', 'B', 6),
        ('lag_length', 'B', 7),
        ('number_of_beams', 'B', 8),
        ('number_of_cells', 'B', 9),
        ('pings_per_ensemble', '<H', 10),
        ('depth_cell_length', '<H', 12),
        ('blank_after_transmit', '<H', 14),
        ('signal_processing_mode', 'B', 16),
        ('low_correlation_threshold', 'B', 17),
        ('number_of_code_repetitions', 'B', 18),
        ('minimum_percentage_water_profile_pings', 'B', 19),
        ('error_velocity_threshold', '<H', 20),
        ('minutes', 'B', 22),
        ('seconds', 'B', 23),
        ('hundredths', 'B', 24),
        ('coordinate_transformation_process', 'B', 25),
        ('heading_alignment', '<H', 26),
        ('heading_bias', '<H', 28),
        ('sensor_source', 'B', 30),
        ('sensor_available', 'B', 31),
        ('bin_1_distance', '<H', 32),
        ('transmit_pulse_length', '<H', 34),
        ('starting_depth_cell', 'B', 36),
        ('ending_depth_cell', 'B', 37),
        ('false_target_threshold', 'B', 38),
        ('spare', 'B', 39),
        ('transmit_lag_distance', '<H', 40),
        ('spare', '<Q', 42), #* Sentinel V manual marks this field as 'spare'.
        ('system_bandwidth', '<H', 50),
        ('system_power', 'B', 52),
        ('spare', 'B', 53),
        ('serial_number', '<I', 54),
        ('beam_angle', 'B', 58),
        ('spare', 'B', 59) #* Sentinel V manual marks this field as 'spare'.
        )

    return unpack_bytes(pd0_bytes, fixed_leader_format, offset)


def parse_variable_leader(pd0_bytes, offset, data):
    variable_leader_format = (
        ('id', '<H', 0),
        ('ensemble_number', '<H', 2),
        ('rtc_year', 'B', 4),
        ('rtc_month', 'B', 5),
        ('rtc_day', 'B', 6),
        ('rtc_hour', 'B', 7),
        ('rtc_minute', 'B', 8),
        ('rtc_second', 'B', 9),
        ('rtc_hundredths', 'B', 10),
        ('ensemble_roll_over', 'B', 11),
        ('bit_result', '<H', 12),
        ('speed_of_sound', '<H', 14),
        ('depth_of_transducer', '<H', 16),
        ('heading', '<H', 18),
        ('pitch', '<h', 20),
        ('roll', '<h', 22),
        ('salinity', '<H', 24),
        ('temperature', '<h', 26),
        ('mpt_minutes', 'B', 28),
        ('mpt_seconds', 'B', 29),
        ('mpt_hundredths', 'B', 30),
        ('heading_standard_deviation', 'B', 31),
        ('pitch_standard_deviation', 'B', 32),
        ('roll_standard_deviation', 'B', 33),
        ('transmit_current', 'B', 34),
        ('transmit_voltage', 'B', 35),
        ('ambient_temperature', 'B', 36),
        ('pressure_positive', 'B', 37),
        ('pressure_negative', 'B', 38),
        ('attitude_temperature', 'B', 39),
        ('attitude', 'B', 40),
        ('contamination_sensor', 'B', 41),
        ('error_status_word', '<I', 42),
        ('reserved', '<H', 46),
        ('pressure', '<I', 48),
        ('pressure_variance', '<I', 52),
        ('spare', 'B', 56),
        ('rtc_y2k_century', 'B', 57),
        ('rtc_y2k_year', 'B', 58),
        ('rtc_y2k_month', 'B', 59),
        ('rtc_y2k_day', 'B', 60),
        ('rtc_y2k_hour', 'B', 61),
        ('rtc_y2k_minute', 'B', 62),
        ('rtc_y2k_seconds', 'B', 63),
        ('rtc_y2k_hundredths', 'B', 64)
    )

    variable_data = unpack_bytes(pd0_bytes, variable_leader_format, offset)
    data['timestamp'] = datetime(
        variable_data['rtc_year'] + 2000,
        variable_data['rtc_month'],
        variable_data['rtc_day'],
        variable_data['rtc_hour'],
        variable_data['rtc_minute'],
        variable_data['rtc_second'],
        variable_data['rtc_hundredths']
    )
    return variable_data


def parse_per_cell_per_beam(pd0_bytes, offset,
                            number_of_cells, number_of_beams,
                            struct_format, debug=True):
    """
    Parses fields that are stored in serial cells and beams
    structures.

    Returns an array of cell readings where each reading is an
    array containing the value at that beam.
    """
    data_size = struct.calcsize(struct_format)
    data = []
    for cell in xrange(0, number_of_cells):
        cell_start = offset + cell*number_of_beams*data_size
        cell_data = []
        for field in xrange(0, number_of_beams):
            field_start = cell_start + field*data_size
            data_bytes = buffer(pd0_bytes, field_start, data_size)
            try:
                field_data = (
                    struct.unpack(struct_format,
                                  data_bytes)[0]
                )
            except:
                print 'Bytes: %s, Data: %s' % (data_bytes, field_data)
                # raise ByteUnpackingError
                break
            cell_data.append(field_data)
        data.append(cell_data)

    return data


def parse_per_cell_beam5(pd0_bytes, offset, number_of_cells, struct_format,
                         debug=False):
    """
    Parses fields that are stored in serial cells and beams
    structures.

    Returns an array of cell readings where each reading is an
    array containing the value at beam 5.
    """
    data_size = struct.calcsize(struct_format)
    data = []
    for cell in xrange(0, number_of_cells):
        cell_start = offset + cell*data_size
        data_bytes = buffer(pd0_bytes, cell_start, data_size)
        cell_data = (struct.unpack(struct_format,
                                   data_bytes)[0])
        if debug:
            print 'Bytes: %s, Data: %s' % (data_bytes, cell_data)

        data.append(cell_data)

    return data


def parse_velocity(pd0_bytes, offset, data, fbad=-32768):
    velocity_format = (
        ('id', '<H', 0),
    )

    velocity_data = unpack_bytes(pd0_bytes, velocity_format, offset)
    offset += 2  # Move past id field
    vel = parse_per_cell_per_beam(pd0_bytes, offset,
                                  data['fixed_leader_janus']['number_of_cells'],
                                  data['fixed_leader_janus']['number_of_beams'],
                                  '<h')

    vel = np.array(vel)
    vel = np.ma.masked_where(vel==fbad, vel)
    velocity_data['data'] = vel

    return velocity_data


def parse_correlation(pd0_bytes, offset, data):
    correlation_format = (('id', '<H', 0),)

    correlation_data = unpack_bytes(pd0_bytes, correlation_format,
                                    offset)
    offset += 2
    corr = parse_per_cell_per_beam(
                pd0_bytes, offset,
                data['fixed_leader_janus']['number_of_cells'],
                data['fixed_leader_janus']['number_of_beams'], 'B')
    correlation_data['data'] = np.array(corr)

    return correlation_data


def parse_echo_intensity(pd0_bytes, offset, data):
    echo_intensity_format = (
        ('id', '<H', 0),
    )

    echo_intensity_data = unpack_bytes(pd0_bytes,
                                       echo_intensity_format,
                                       offset)
    offset += 2
    echo = parse_per_cell_per_beam(
        pd0_bytes,
        offset,
        data['fixed_leader_janus']['number_of_cells'],
        data['fixed_leader_janus']['number_of_beams'],
        'B'
    )
    echo_intensity_data['data'] = np.array(echo)

    return echo_intensity_data


def parse_percent_good(pd0_bytes, offset, data):
    percent_good_format = (
        ('id', '<H', 0),
    )

    percent_good_data = unpack_bytes(pd0_bytes, percent_good_format,
                                     offset)
    offset += 2
    pgood = parse_per_cell_per_beam(
        pd0_bytes,
        offset,
        data['fixed_leader_janus']['number_of_cells'],
        data['fixed_leader_janus']['number_of_beams'],
        'B'
    )
    percent_good_data['data'] = np.array(pgood)

    return percent_good_data


def parse_bottom_track(pd0_bytes, offset, data):
    raise NotImplementedError()
    # bottom_track_format = (
    #     ('id', '<H', 0),
    #     ('bottomtrack_pings_per_ensemble', '<H', 2),
    #     ('reserved', '<H', 4),
    #     ('correlation_magnitude_minimum', 'B', 6),
    #     ('evaluation_amplitude_minimum', 'B', 7),
    #     ('reserved', 'B', 8),
    #     ('bottom_track_mode', 'B', 9),
    #     ('error_velocity_maximum', '<H', 10)
    # )
    #
    # bottom_track_data, data_size = unpack_bytes(pd0_bytes,
    #                                             bottom_track_format, offset)
    # offset += data_size


class ChecksumError(Exception):
    """
    Raised when the checksum of an ensemble doesn't match the calculated one.
    """
    def __init__(self, calc_checksum, given_checksum):
        self.calc_checksum = calc_checksum
        self.given_checksum = given_checksum

    def __str__(self):
        return 'Bad checksum: Calculated %d, Given: %d'%(self.calc_checksum, self.given_checksum)


class ReadChecksumError(Exception):
    """
    Raised when the calculation of an ensemble fails, so cannot compare
    the calculated and read checksums.
    """
    pass


class ReadHeaderError(Exception):
    """
    Raised when cannot read any one ensemble's header.
    """
    pass


def validate_checksum(pd0_bytes, offset):
    calc_checksum = sum(pd0_bytes[:offset]) & 0xFFFF # Modulo 65535 checksum.
                                                     # The '&' operator discards
                                                     # overflow bits.
    buff = buffer(pd0_bytes, offset, 2) # Count all bytes in
                                        # ensemble excluding the
                                        # last 2 (the checksum).
    try:
        given_checksum = struct.unpack('<H', buff)[0]
    except:
        # buff2 = buffer(pd0_bytes, len(pd0_bytes)-2, 2)
        # given_checksum = struct.unpack('<H', buff2)[0]
        # embed()
        raise ReadChecksumError("Could not read checksum of ensemble.")
        # try:
        #     buff = buffer(pd0_bytes, offset-1, 2)
        #     given_checksum = struct.unpack('<H', buff)[0]
        # except:
        #     # embed()
        #     # print(pd0_bytes[:5])
        #     raise ReadChecksumError("Could not read checksum of ensemble.")
        #     # print("Warning: Could not read checksum of ensemble.")
        #     # return
    # else:
    #     print(pd0_bytes[:10])

    if calc_checksum != given_checksum:
        E = ChecksumError(calc_checksum, given_checksum)
        print(E) # First print error message,
        raise E  # then propagate exception to caller.
    else: # Read checksum successfully and also, it matched the calculated one.
        pass


output_data_parsers = {
    #--- Janus (beams 1-4) data.
    0x0000: ('fixed_leader_janus', parse_fixed_leader),
    0x0080: ('variable_leader_janus', parse_variable_leader),
    0x0100: ('velocity_janus', parse_velocity),
    0x0200: ('correlation_janus', parse_correlation),
    0x0300: ('echo_intensity_janus', parse_echo_intensity),
    0x0004: ('percent_good_janus', parse_percent_good),
    #--- Sentinel V specific outputs.
    # 0x7000: ('sentinelV_system_configuration', parse_sentinelV_syscfg),
    # 0x7001: ('sentinelV_ping_setup', parse_sentinelV_pingsetup),
    # 0x7002: ('sentinelV_ADC_data', parse_sentinelV_ADC),
    # 0x7003: ('sentinelV_features_data', parse_sentinelV_features),
    #--- Beam 5 data.
    0x0f01: ('fixed_leader_beam5', parse_fixed_leader5),
    0x0a00: ('velocity_beam5', parse_velocity5),
    0x0b00: ('correlation_beam5', parse_correlation5),
    0x0c00: ('echo_intensity_beam5', parse_echo_intensity_beam5),
    # 0x000d: ('percent_good_beam5', parse_percent_good5),
    # misc.
    # 0x3200: ('instrument_transformation_matrix', parse_transfmatrix), #TODO
    # 0x0006: ('bottom_track', parse_bottom_track),
    # 0x000b: ('wave_parameters', parse_waves), #TODO
    # 0x000c: ('wave_parameters2', parse_sea_swell), #TODO
    # 0x7004: ('event_log', parse_eventlog), #TODO
}


def parse_sentinelVpd0_bytearray(pd0_bytes):
    """
    This is the main parsing loop. It uses output_data_parsers
    to determine what functions to run given a specified offset and header ID at that offset.

    Returns a dictionary of values parsed out into Python types.
    """
    data = {}
    assert pd0_bytes[:2]==b'\x7f\x7f', "Bad header id."

    # Read in header
    data['header'] = parse_fixed_header(pd0_bytes)

    # Run checksum.
    try:
        nbytes_in_ens = data['header']['number_of_bytes']
    except KeyError:
        raise ReadHeaderError("Could not read number of bytes in ensemble.")

    # print(nbytes_in_ens)
    # if nbytes_in_ens>1126:
    #     embed()
    validate_checksum(pd0_bytes, nbytes_in_ens)

    num_datatypes = data['header']['number_of_data_types']
    # if num_datatypes>14:
    #     print(num_datatypes)
    #     print(pd0_bytes)
    data['header']['address_offsets'] = (parse_address_offsets(pd0_bytes,
                                                               num_datatypes))

    for offset in data['header']['address_offsets']:
        try:
            header_id = struct.unpack('<H', buffer(pd0_bytes, offset, 2))[0]
        except:
            continue
            # embed()
        if header_id in output_data_parsers:
            key = output_data_parsers[header_id][0]
            parser = output_data_parsers[header_id][1]
            data[key] = (parser(pd0_bytes, offset, data))
        else:
            pass
            # print 'No parser found for header at %s' % (hex(header_id),)

    # Add metadata.
    if 'fixed_leader_janus' in data.keys():
        data = get_pd0metadata(data)

    return data


def get_pd0metadata(D):
    """
    Gets additional instrument setup information from
    the fixed leader data.
    """
    # Set the maps for the relevant bytes (TRDI Sentinel V manual Table 37).
    d_system_configuration_LSB = {'xxxxx010':'Sentinel V100 (300 kHz)',
                                  'xxxxx011':'Sentinel V50 (500 kHz)',
                                  'xxxxx100':'Sentinel V20 (1000 kHz)',
                                  '0xxxxxxx':'downward-looking',
                                  '1xxxxxxx':'upward-looking'}
    d_system_configuration_MSB = {'0100xxxx':'4-beam Janus',
                                  '0101xxxx':'5-beam Janus'}

    d_sensor_source = {'x1xxxxxx':'speed of sound calculated from depth, salinity, temperature',
                       'xx1xxxxx':'uses depth sensor',
                       'xxx1xxxx':'uses xducer heading sensor'}
    d_coord_trans = {'xxx00xxx':'beam coordinates (no coordinate transformation)',
                     'xxx01xxx':'instrument coordinates (b1->b2, b4->b3, towards instrument, error velocity)',
                     'xxx10xxx':'ship coordinates (port->starboard, aft->forward, away from instrument, error velocity)',
                     'xxx11xxx':'earth coordinates (west->east, vel2 = south->north, upwards, error velocity)',
                     'xxxxx1xx':'pitch and roll used in ship or Earth transformation',
                     'xxxxxx1x':'3-beam solution used if one beam is below the correlation threshold set by the WC command',
                     'xxxxxxx1':'bin mapping was applied'}
    metadata_maps = {
        ('system_configuration_LSB'):d_system_configuration_LSB,
        ('system_configuration_MSB'):d_system_configuration_MSB,
        ('coordinate_transformation_process'):d_coord_trans,
        ('sensor_source'):d_sensor_source
        }

    # Read metadata from fixed leader.
    dFL = D['fixed_leader_janus']
    allmeta = []
    for k in dFL.iterkeys():
        if k in metadata_maps.keys():
            dmap = metadata_maps[k]
            # Test all fields and include the ones
            # that match the bit sequences read.
            bytei = bin(dFL[k])
            for bitstr in dmap.keys():
                if _bitcmp(bytei, bitstr):
                    word = dmap[bitstr]
                    dFL.update({k:word})
                    allmeta.append(word)
    D.update({'fixed_leader_janus':dFL})
    # embed()

    return D


def _bitcmp(bit, bit_ref, wildcard='x'):
    """
    Compares the string representation of a binary number ignoring any bits
    on the reference string 'bit_ref' marked with a wildcard.
    """
    bit = bit.replace('0b', '')
    bit, bit_ref = list(bit), list(bit_ref)
    bit.reverse(); bit_ref.reverse() # Start comparing from LSbit.
    isbiteq = []
    for n in range(len(bit)):
        if bit_ref[n]!=wildcard:
            isbiteq.append(bit[n]==bit_ref[n])

    return all(isbiteq)
