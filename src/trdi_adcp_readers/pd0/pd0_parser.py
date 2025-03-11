import struct
from datetime import datetime


def unpack_bytes(pd0_bytes, data_format_tuples, offset=0):
    data = {}
    for fmt in data_format_tuples:
        try:
            struct_offset = offset+fmt[2]
            size = struct.calcsize(fmt[1])
            data_bytes = memoryview(pd0_bytes)[struct_offset:struct_offset + size]
            data[fmt[0]] = (
                struct.unpack(fmt[1], data_bytes)[0]
            )
        except Exception as e:
            print(f'Error parsing {fmt[0]} with the arguments '
                  f'{fmt[1]}, offset:{struct_offset} size:{size}')

    return data


def parse_fixed_header(pd0_bytes):
    header_data_format = (
        ('id', 'B', 0),
        ('data_source', 'B', 1),
        ('number_of_bytes', '<H', 2),
        ('spare', 'B', 4),
        ('number_of_data_types', 'B', 5)
    )

    return unpack_bytes(pd0_bytes, header_data_format)


def parse_address_offsets(pd0_bytes, num_datatypes, offset=6):
    address_data = []
    for bytes_start in range(offset, offset+(num_datatypes * 2), 2):
        data = (
            struct.unpack_from('<H',
                             memoryview(pd0_bytes)[bytes_start:bytes_start + 2])[0]
        )
        address_data.append(data)

    return address_data


def parse_fixed_leader(pd0_bytes, offset, data):
    fixed_leader_format = (
        ('id', '<H', 0),
        ('cpu_firmware_version', 'B', 2),
        ('cpu_firmware_revision', 'B', 3),
        ('system_configuration', 'B', 5),
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
        ('cpu_board_serial_number', '<Q', 42),
        ('system_bandwidth', '<H', 50),
        ('system_power', 'B', 52),
        ('spare', 'B', 53),
        ('serial_number', '<I', 54),
        ('beam_angle', 'B', 58)
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
                          struct_format, debug=False):
    """
    Parses fields that are stored in serial cells and beams
    structures.

    Returns an array of cell readings where each reading is an
    array containing the value at that beam.
    """

    data_size = struct.calcsize(struct_format)
    data = []
    for cell in range(0, number_of_cells):
        cell_start = offset + cell*number_of_beams*data_size
        cell_data = []
        for field in range(0, number_of_beams):
            field_start = cell_start + field*data_size
            data_bytes = memoryview(pd0_bytes)[field_start:field_start + data_size]
            field_data = (
                struct.unpack(struct_format,
                            data_bytes)[0]
            )
            if debug:
                print(f'Bytes: {data_bytes}, Data: {field_data}')
            cell_data.append(field_data)
        data.append(cell_data)

    return data


def parse_velocity(pd0_bytes, offset, data):
    velocity_format = (
        ('id', '<H', 0),
    )

    velocity_data = unpack_bytes(pd0_bytes, velocity_format, offset)
    offset += 2  # Move past id field
    velocity_data['data'] = parse_per_cell_per_beam(
        pd0_bytes,
        offset,
        data['fixed_leader']['number_of_cells'],
        data['fixed_leader']['number_of_beams'],
        '<h'
    )

    return velocity_data


def parse_correlation(pd0_bytes, offset, data):
    correlation_format = (
        ('id', '<H', 0),
    )

    correlation_data = unpack_bytes(pd0_bytes, correlation_format, offset)
    offset += 2
    correlation_data['data'] = parse_per_cell_per_beam(
        pd0_bytes,
        offset,
        data['fixed_leader']['number_of_cells'],
        data['fixed_leader']['number_of_beams'],
        'B'
    )

    return correlation_data


def parse_echo_intensity(pd0_bytes, offset, data):
    echo_intensity_format = (
        ('id', '<H', 0),
    )

    echo_intensity_data = unpack_bytes(pd0_bytes,
                                       echo_intensity_format, offset)
    offset += 2
    echo_intensity_data['data'] = parse_per_cell_per_beam(
        pd0_bytes,
        offset,
        data['fixed_leader']['number_of_cells'],
        data['fixed_leader']['number_of_beams'],
        'B'
    )

    return echo_intensity_data


def parse_percent_good(pd0_bytes, offset, data):
    percent_good_format = (
        ('id', '<H', 0),
    )

    percent_good_data = unpack_bytes(pd0_bytes, percent_good_format, offset)
    offset += 2
    percent_good_data['data'] = parse_per_cell_per_beam(
        pd0_bytes,
        offset,
        data['fixed_leader']['number_of_cells'],
        data['fixed_leader']['number_of_beams'],
        'B'
    )

    return percent_good_data


def parse_status(pd0_bytes, offset, data):
    status_format = (
        ('id', '<H', 0)
    )

    status_data = unpack_bytes(pd0_bytes, status_format, offset)
    offset += 2
    status_data['data'] = parse_per_cell_per_beam(
        pd0_bytes,
        offset,
        data['fixed_leader']['number_of_cells'],
        data['fixed_leader']['number_of_beams'],
        'B'
    )

    return status_data


def parse_bottom_track(pd0_bytes, offset, data):
    bottom_track_format = (
        ('id', '<H', 0),
        ('pings_per_ensemble', '<H', 2),
        ('delay_before_reaquire', '<H', 4),
        ('correlation_magnitude_minimum', 'B', 6),
        ('evaluation_amplitude_minimum', 'B', 7),
        ('percent_good_minimum', 'B', 8),
        ('bottom_track_mode', 'B', 9),
        ('error_velocity_maximum', '<H', 10)
    )

    bottom_track_data, data_size = unpack_bytes(pd0_bytes,
                                                bottom_track_format, offset)
    offset += data_size
    # Plan to implement as needed
    raise NotImplementedError()


class ChecksumError(Exception):
    """
    Raised when an invalid checksum is found
    """
    def __init__(self, calc_checksum, given_checksum):
        self.calc_checksum = calc_checksum
        self.given_checksum = given_checksum

    def __str__(self):
        return f'Calculated {self.calc_checksum}, Given: {self.given_checksum}'


def validate_checksum(pd0_bytes, offset):
    calc_checksum = sum(pd0_bytes[:offset]) & 0xFFFF
    given_checksum = struct.unpack('<H', memoryview(pd0_bytes)[offset:offset + 2])[0]

    if calc_checksum != given_checksum:
        raise ChecksumError(calc_checksum, given_checksum)


output_data_parsers = {
    0x0000: ('fixed_leader', parse_fixed_leader),
    0x0080: ('variable_leader', parse_variable_leader),
    0x0100: ('velocity', parse_velocity),
    0x0200: ('correlation', parse_correlation),
    0x0300: ('echo_intensity', parse_echo_intensity),
    0x0400: ('percent_good', parse_percent_good),
    0x0500: ('status', parse_status),
    0x0600: ('bottom_track', parse_bottom_track)
}


def parse_pd0_bytearray(pd0_bytes):
    """
    This is the main parsing loop. It uses output_data_parsers
    to determine what funcitons to run given a specified offset and header
    ID at that offset.

    Returns a dictionary of values parsed out into Python types.
    """

    data = {}

    # Read in header
    data['header'] = parse_fixed_header(pd0_bytes)

    # Run checksum
    validate_checksum(pd0_bytes, data['header']['number_of_bytes'])

    data['header']['address_offsets'] = (
        parse_address_offsets(pd0_bytes,
                              data['header']['number_of_data_types'])
    )

    for offset in data['header']['address_offsets']:
        header_id = struct.unpack('<H', memoryview(pd0_bytes)[offset:offset + 2])[0]
        if header_id in output_data_parsers:
            key = output_data_parsers[header_id][0]
            parser = output_data_parsers[header_id][1]
            data[key] = (
                parser(pd0_bytes, offset, data)
            )
        else:
            print(f'No parser found for header {header_id}')

    return data
