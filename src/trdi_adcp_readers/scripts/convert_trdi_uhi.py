#!/usr/bin/python

from trdi_adcp_readers.readers import (
    read_PD0_file,
    read_PD15_file
)

import argparse
import math
from os import path
import sys

ext_parser_map = {
    'pd0': read_PD0_file,
    'pd15': read_PD15_file
}


def calculate_water_depth(bin_number, bin_size, bin1dist):
    """
    Calculate water depth for a specific bin
    
    Args:
        bin_number: The bin number (1-based)
        bin_size: Size of each bin in centimeters
        bin1dist: Distance to the first bin in centimeters
        
    Returns:
        Water depth in meters
    """
    # Convert from centimeters to meters and calculate depth
    return (bin1dist + (bin_number - 1) * bin_size) / 100.0


def calculate_current_speed_direction(eastward, northward):
    """
    Calculate current speed and direction from eastward and northward components
    
    Args:
        eastward: Eastward velocity component in mm/s
        northward: Northward velocity component in mm/s
        
    Returns:
        Tuple of (current_speed, current_direction)
        - current_speed in cm/s
        - current_direction in degrees (0-360, where 0 is North)
    """
    # Convert from mm/s to cm/s
    eastward_cm = eastward / 10.0
    northward_cm = northward / 10.0
    
    # Calculate speed
    speed = math.sqrt(eastward_cm**2 + northward_cm**2)
    
    # Calculate direction (meteorological convention - direction wind is coming from)
    # For currents, we want the direction the current is going to
    direction = math.degrees(math.atan2(eastward_cm, northward_cm))
    
    # Convert to 0-360 range
    if direction < 0:
        direction += 360
    
    return speed, direction


def find_last_good_bin(echo_amp_data):
    """
    Determine the last good bin based on where Echo Amplitude jumps
    
    Args:
        echo_amp_data: List of echo amplitude data for all bins
        
    Returns:
        Index of the last good bin (0-based)
    """
    # Convert the 2D array to a 1D array by averaging across beams
    avg_echo = [sum(bin_data)/len(bin_data) for bin_data in echo_amp_data]
    
    # Look for significant jumps in echo amplitude
    for i in range(1, len(avg_echo) - 1):
        # Check for a significant increase in echo amplitude
        if avg_echo[i+1] - avg_echo[i] > 20:  # Threshold for significant jump
            return i
    
    # If no significant jump is found, return the last bin
    return len(echo_amp_data) - 1


def writeinfo(filename, time_str, bit, ssval, tiltx, tilty, bin1dist, ens_number, 
              temperature, heading, xducer_depth, bin_size, blank, pings, 
              transmit_lag, transmit_pulse, mag_declination, bins, last_good_counter):
    """
    Write header information to a file
    """
    with open(filename, 'w') as f:
        f.write(f"{time_str},{bit}, {ssval:8.2f}, {tiltx:8.2f}, {tilty:8.2f}, {bin1dist:8.2f}, "
                f"{ens_number:8d}, {temperature:8.2f}, {heading:8.2f}, {xducer_depth:8.2f}, "
                f"{bin_size:8.2f}, {blank:8.2f}, {pings:8d}, {transmit_lag:8.2f}, "
                f"{transmit_pulse:8.2f}, {mag_declination:8.2f}, {bins:8d}, {last_good_counter:8d}\n")


def writedat1(filename, water_depths, eastward_currents, northward_currents, upward_currents,
              error_velocities, current_speeds, current_directions):
    """
    Write velocity data to a file
    """
    with open(filename, 'w') as f:
        for i in range(len(water_depths)):
            f.write(f"{water_depths[i]:8.2f}, {eastward_currents[i]:8.2f}, {northward_currents[i]:8.2f}, "
                    f"{upward_currents[i]:8.2f}, {error_velocities[i]:8.2f}, "
                    f"{current_speeds[i]:8.2f}, {current_directions[i]:8.2f}\n")


def writedat2(filename, water_depths, correlation1, correlation2, correlation3, correlation4,
              echo_amp1, echo_amp2, echo_amp3, echo_amp4, 
              percent_good1, percent_good2, percent_good3, percent_good4):
    """
    Write correlation and echo amplitude data to a file
    """
    with open(filename, 'w') as f:
        for i in range(len(water_depths)):
            f.write(f"{water_depths[i]:8.2f}, {correlation1[i]:8.2f}, {correlation2[i]:8.2f}, "
                    f"{correlation3[i]:8.2f}, {correlation4[i]:8.2f}, "
                    f"{echo_amp1[i]:8.2f}, {echo_amp2[i]:8.2f}, {echo_amp3[i]:8.2f}, "
                    f"{echo_amp4[i]:8.2f}, {percent_good1[i]:8.2f}, {percent_good2[i]:8.2f}, "
                    f"{percent_good3[i]:8.2f}, {percent_good4[i]:8.2f}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Converts a binary PD15 or PD0 TRDI ADCP file to UHI format CSV files",
    )

    parser.add_argument(
        "--headers",
        default=0,
        help="Number of header lines to skip before looking for data",
        type=int
    )

    parser.add_argument(
        "--format",
        default=None,
        help="Binary format (pd15 or pd0) to convert from. " +
             "Derives from file extension if not defined."
    )
    
    parser.add_argument(
        "--mag-declination",
        default=0.0,
        help="Magnetic declination in degrees",
        type=float
    )

    parser.add_argument(
        "input_file",
        help="file to be converted"
    )
    
    parser.add_argument(
        "info_file",
        help="output file for header information"
    )
    
    parser.add_argument(
        "velocity_file",
        help="output file for velocity data"
    )
    
    parser.add_argument(
        "data_file",
        help="output file for correlation and echo amplitude data"
    )

    args = parser.parse_args()

    if args.format is None:
        (root, ext) = path.splitext(args.input_file)
        args.format = ext.lower()[1:]
    else:
        args.format = args.format.lower()

    if args.format in ext_parser_map:
        dataset = ext_parser_map[args.format](
            args.input_file,
            header_lines=args.headers
        )
        
        # Extract required data from the dataset
        fixed_leader = dataset['fixed_leader']
        variable_leader = dataset['variable_leader']
        velocity_data = dataset['velocity']['data']
        correlation_data = dataset['correlation']['data']
        echo_intensity_data = dataset['echo_intensity']['data']
        percent_good_data = dataset['percent_good']['data']
        
        # Calculate header information
        # Create timestamp from individual components
        try:
            # First try to use timestamp if it exists
            time_str = variable_leader['timestamp'].strftime("%Y/%m/%d %H:%M:%S")
        except KeyError:
            # If timestamp doesn't exist, create it from individual components
            year = variable_leader.get('rtc_y2k_year', variable_leader.get('rtc_year', 0))
            if 'rtc_y2k_century' in variable_leader:
                year = variable_leader['rtc_y2k_century'] * 100 + year
            elif year < 100:
                # Assume 20xx for years less than 100
                year += 2000
            
            month = variable_leader.get('rtc_y2k_month', variable_leader.get('rtc_month', 1))
            day = variable_leader.get('rtc_y2k_day', variable_leader.get('rtc_day', 1))
            hour = variable_leader.get('rtc_y2k_hour', variable_leader.get('rtc_hour', 0))
            minute = variable_leader.get('rtc_y2k_minute', variable_leader.get('rtc_minute', 0))
            second = variable_leader.get('rtc_y2k_seconds', variable_leader.get('rtc_second', 0))
            
            time_str = f"{year}/{month:02d}/{day:02d} {hour:02d}:{minute:02d}:{second:02d}"
        bit = 0  # Default value, not clear from the data structure
        ssval = variable_leader['speed_of_sound'] / 10.0  # Convert to m/s
        tiltx = variable_leader['pitch'] / 100.0  # Convert to degrees
        tilty = variable_leader['roll'] / 100.0  # Convert to degrees
        bin1dist = fixed_leader['bin_1_distance'] / 100.0  # Convert to meters
        ens_number = variable_leader['ensemble_number']
        temperature = variable_leader['temperature'] / 100.0  # Convert to degrees C
        heading = variable_leader['heading'] / 100.0  # Convert to degrees
        xducer_depth = variable_leader['depth_of_transducer'] / 10.0  # Convert to meters
        bin_size = fixed_leader['depth_cell_length'] / 100.0  # Convert to meters
        blank = fixed_leader['blank_after_transmit'] / 100.0  # Convert to meters
        pings = fixed_leader['pings_per_ensemble']
        transmit_lag = fixed_leader['transmit_lag_distance'] / 100.0  # Convert to meters
        transmit_pulse = fixed_leader['transmit_pulse_length'] / 100.0  # Convert to meters
        mag_declination = args.mag_declination
        bins = fixed_leader['number_of_cells']
        
        # Find the last good bin
        last_good_counter = find_last_good_bin(echo_intensity_data) + 1  # Convert to 1-based
        
        # Write header information
        writeinfo(args.info_file, time_str, bit, ssval, tiltx, tilty, bin1dist, ens_number,
                 temperature, heading, xducer_depth, bin_size, blank, pings,
                 transmit_lag, transmit_pulse, mag_declination, bins, last_good_counter)
        
        # Process velocity data
        water_depths = []
        eastward_currents = []
        northward_currents = []
        upward_currents = []
        error_velocities = []
        current_speeds = []
        current_directions = []
        
        # Process correlation and echo amplitude data
        correlation1 = []
        correlation2 = []
        correlation3 = []
        correlation4 = []
        echo_amp1 = []
        echo_amp2 = []
        echo_amp3 = []
        echo_amp4 = []
        percent_good1 = []
        percent_good2 = []
        percent_good3 = []
        percent_good4 = []
        
        # Process data for each bin up to the last good bin
        for bin_idx in range(last_good_counter):
            # Calculate water depth for this bin
            bin_number = bin_idx + 1  # Convert to 1-based
            water_depth = calculate_water_depth(bin_number, fixed_leader['depth_cell_length'], fixed_leader['bin_1_distance'])
            water_depths.append(water_depth)
            
            # Extract velocity components
            # Assuming beam order is East, North, Up, Error
            # Note: Velocity data is in mm/s
            eastward = velocity_data[bin_idx][0]  # Already in mm/s
            northward = velocity_data[bin_idx][1]  # Already in mm/s
            upward = velocity_data[bin_idx][2]  # Already in mm/s
            error_velocity = velocity_data[bin_idx][3]  # Already in mm/s
            
            # Calculate current speed and direction
            speed, direction = calculate_current_speed_direction(eastward, northward)
            
            # Store velocity data
            eastward_currents.append(eastward / 10.0)  # Convert to cm/s for output
            northward_currents.append(northward / 10.0)  # Convert to cm/s for output
            upward_currents.append(upward / 10.0)  # Convert to cm/s for output
            error_velocities.append(error_velocity / 10.0)  # Convert to cm/s for output
            current_speeds.append(speed)
            current_directions.append(direction)
            
            # Extract correlation data
            correlation1.append(correlation_data[bin_idx][0])
            correlation2.append(correlation_data[bin_idx][1])
            correlation3.append(correlation_data[bin_idx][2])
            correlation4.append(correlation_data[bin_idx][3])
            
            # Extract echo amplitude data
            echo_amp1.append(echo_intensity_data[bin_idx][0])
            echo_amp2.append(echo_intensity_data[bin_idx][1])
            echo_amp3.append(echo_intensity_data[bin_idx][2])
            echo_amp4.append(echo_intensity_data[bin_idx][3])
            
            # Extract percent good data
            percent_good1.append(percent_good_data[bin_idx][0])
            percent_good2.append(percent_good_data[bin_idx][1])
            percent_good3.append(percent_good_data[bin_idx][2])
            percent_good4.append(percent_good_data[bin_idx][3])
        
        # Write velocity data
        writedat1(args.velocity_file, water_depths, eastward_currents, northward_currents, 
                 upward_currents, error_velocities, current_speeds, current_directions)
        
        # Write correlation and echo amplitude data
        writedat2(args.data_file, water_depths, correlation1, correlation2, correlation3, correlation4,
                 echo_amp1, echo_amp2, echo_amp3, echo_amp4,
                 percent_good1, percent_good2, percent_good3, percent_good4)
        
    else:
        raise ValueError(
            'Unrecognized extension %s found for input file' % args.format
        )


if __name__ == '__main__':
    sys.exit(main())
