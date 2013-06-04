import struct


def unpack_bytes(pd0_bytes, data_format_tuples, offset=0):
    data = {}
    for fmt in data_format_tuples:
        try:
            data_bytes = buffer(pd0_bytes[offset+fmt[2]:offset+fmt[3]])
            data[fmt[0]] = (
                struct.unpack(fmt[1], data_bytes)[0]
            )
        except:
            print('Error parsing %s with the arguments '
                  '%s, %d:%d' % (fmt[0], fmt[1], fmt[2], fmt[3]))

    return data


def parse_fixed_header(pd0_bytes):
    header_data_format = (
        ('id', 'B', 0, 1),
        ('data_source', 'B', 1, 2),
        ('number_of_bytes', '<H', 2, 4),
        ('spare', 'B', 4, 5),
        ('number_of_data_types', 'B', 5, 6)
    )

    return unpack_bytes(pd0_bytes, header_data_format)


def parse_address_offsets(pd0_bytes, num_datatypes, offset=6):
    address_data = []
    for bytes_start in xrange(offset, offset+(num_datatypes * 2), 2):
        data = (
            struct.unpack_from('<H',
                               buffer(pd0_bytes[bytes_start:bytes_start+2]))[0]
        )
        address_data.append(data)

    return address_data


def parse_fixed_leader(pd0_bytes, offset, number_of_cells):
    fixed_leader_format = (
        ('id', '<H', 0, 2),
        ('cpu_firmware_version', 'B', 2, 3),
        ('cpu_firmware_revision', 'B', 3, 4),
        ('system_configuration', 'B', 5, 6),
        ('simulation_data_flag', 'B', 6, 7),
        ('lag_length', 'B', 7, 8),
        ('number_of_beams', 'B', 8, 9),
        ('number_of_cells', 'B', 9, 10),
        ('pings_per_ensemble', '<H', 10, 12),
        ('depth_cell_length', '<H', 12, 14),
        ('blank_after_transmit', '<H', 14, 16),
        ('signal_processing_mode', 'B', 16, 17),
        ('low_correlation_threshold', 'B', 17, 18),
        ('number_of_code_repetitions', 'B', 18, 19),
        ('minimum_percentage_water_profile_pings', 'B', 19, 20),
        ('error_velocity_threshold', '<H', 20, 22),
        ('minutes', 'B', 22, 23),
        ('seconds', 'B', 23, 24),
        ('hundredths', 'B', 24, 25),
        ('coordinate_transformation_process', 'B', 25, 26),
        ('heading_alignment', '<H', 26, 28),
        ('heading_bias', '<H', 28, 30),
        ('sensor_source', 'B', 30, 31),
        ('sensor_available', 'B', 31, 32),
        ('bin_1_distance', '<H', 32, 34),
        ('transmit_pulse_length', '<H', 34, 36),
        ('starting_depth_cell', 'B', 36, 37),
        ('ending_depth_cell', 'B', 37, 38),
        ('false_target_threshold', 'B', 38, 39),
        ('spare', 'B', 39, 40),
        ('transmit_lag_distance', '<H', 40, 42),
        ('cpu_board_serial_number', '<Q', 42, 50),
        ('system_bandwidth', '<H', 50, 52),
        ('system_power', 'B', 52, 53),
        ('spare', 'B', 53, 54),
        ('serial_number', '<I', 54, 58),
        ('beam_angle', 'B', 58, 59)
    )

    return unpack_bytes(pd0_bytes, fixed_leader_format, offset)


def parse_variable_leader(pd0_bytes, offset, number_of_cells=None):
    variable_leader_format = (
        ('id', '<H', 0, 2),
        ('ensemble_number', '<H', 2, 4),
        ('rtc_year', 'B', 4, 5),
        ('rtc_month', 'B', 5, 6),
        ('rtc_day', 'B', 6, 7),
        ('rtc_hour', 'B', 7, 8),
        ('rtc_minute', 'B', 8, 9),
        ('rtc_second', 'B', 9, 10),
        ('rtc_hundredths', 'B', 10, 11),
        ('ensemble_roll_over', 'B', 11, 12),
        ('bit_result', '<H', 12, 14),
        ('speed_of_sound', '<H', 14, 16),
        ('depth_of_transducer', '<H', 16, 18),
        ('heading', '<H', 18, 20),
        ('pitch', '<h', 20, 22),
        ('roll', '<h', 22, 24),
        ('salinity', '<H', 24, 26),
        ('temperature', '<h', 26, 28),
        ('mpt_minutes', 'B', 28, 29),
        ('mpt_seconds', 'B', 29, 30),
        ('mpt_hundredths', 'B', 30, 31),
        ('heading_standard_deviation', 'B', 31, 32),
        ('pitch_standard_deviation', 'B', 32, 33),
        ('roll_standard_deviation', 'B', 33, 34),
        ('transmit_current', 'B', 34, 35),
        ('transmit_voltage', 'B', 35, 36),
        ('ambient_temperature', 'B', 36, 37),
        ('pressure_positive', 'B', 37, 38),
        ('pressure_negative', 'B', 38, 39),
        ('attitude_temperature', 'B', 39, 40),
        ('attitude', 'B', 40, 41),
        ('contamination_sensor', 'B', 41, 42),
        ('error_status_word', '<I', 42, 46),
        ('reserved', '<H', 46, 48),
        ('pressure', '<I', 48, 52),
        ('pressure_variance', '<I', 52, 56),
        ('spare', 'B', 56, 57),
        ('rtc_y2k_century', 'B', 57, 58),
        ('rtc_y2k_year', 'B', 58, 59),
        ('rtc_y2k_month', 'B', 59, 60),
        ('rtc_y2k_day', 'B', 60, 61),
        ('rtc_y2k_hour', 'B', 61, 62),
        ('rtc_y2k_minute', 'B', 62, 63),
        ('rtc_y2k_seconds', 'B', 63, 64),
        ('rtc_y2k_hundredths', 'B', 64, 65)
    )

    return unpack_bytes(pd0_bytes, variable_leader_format, offset)


def parse_velocity(pd0_bytes, offset, number_of_cells):
    velocity_format = (
        ('id', '<H', 0, 2),
    )

    velocity_data = unpack_bytes(pd0_bytes, velocity_format, offset)
    offset += 2  # Move past id field
    velocity_data['data'] = []
    for cell in xrange(0, number_of_cells):
        cell_start = offset + cell*2
        cell_velocity = (
            struct.unpack('<h', buffer(pd0_bytes[cell_start:cell_start+2]))[0]
        )
        velocity_data['data'].append(cell_velocity)

    return velocity_data


def parse_correlation(pd0_bytes, offset, number_of_cells):
    correlation_format = (
        ('id', '<H', 0, 2),
    )

    correlation_data = unpack_bytes(pd0_bytes, correlation_format, offset)
    offset += 2
    correlation_data['data'] = []
    for cell in xrange(0, number_of_cells):
        cell_start = offset + cell*4
        cell_correlation = []
        for field in xrange(0, 4):
            field_start = cell_start + field
            field_velocity = (
                struct.unpack('B',
                              buffer(pd0_bytes[field_start:field_start+1]))[0]
            )
            cell_correlation.append(field_velocity)
        correlation_data['data'].append(cell_correlation)

    return correlation_data


output_data_parsers = {
    0x0000: ('fixed_leader', parse_fixed_leader),
    0x0080: ('variable_leader', parse_variable_leader),
    0x0100: ('velocity', parse_velocity),
    0x0200: ('correlation', parse_correlation),
    0x0300: ('echo_intensity', None),
    0x0400: ('percent_good', None),
    0x0500: ('status', None)
}


def parse_pd0_bytearray(pd0_bytes):
    data = {}

    # Read in header
    data['header'] = parse_fixed_header(pd0_bytes)

    data['header']['address_offsets'] = (
        parse_address_offsets(pd0_bytes,
                              data['header']['number_of_data_types'])
    )

    # Must initialize this to avoid error in parsing loop
    data['fixed_leader'] = {}
    data['fixed_leader']['number_of_cells'] = 0

    for offset in data['header']['address_offsets']:
        header_id = struct.unpack('<H', buffer(pd0_bytes[offset:offset+2]))[0]

        print 'Header ID: %d' % (header_id,)
        if header_id in output_data_parsers:
            key = output_data_parsers[header_id][0]
            parser = output_data_parsers[header_id][1]
            data[key] = (
                parser(pd0_bytes, offset,
                       data['fixed_leader']['number_of_cells'])
            )

    return data
