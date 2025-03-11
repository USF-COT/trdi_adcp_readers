import unittest
import os
from trdi_adcp_readers.readers import (
    read_PD0_file,
    read_PD15_file
)
import pprint


class TestPD0File(unittest.TestCase):
    test_dir = os.path.dirname(os.path.abspath(__file__))
    parsed_pd0 = read_PD0_file(os.path.join(test_dir, 'C12AN_90.PD0'))

#    def test_print_data(self):
#        pp = pprint.PrettyPrinter(indent=4)
#        pp.pprint(self.parsed_pd0)

    def test_header_fields(self):
        known_values = {
            'id': 0x7f,
            'data_source': 127,
            'number_of_bytes': 1152,
            'number_of_data_types': 6,
            'spare': 0
        }
        for k, v in known_values.items():
            self.assertEqual(self.parsed_pd0['header'][k], v)

    def test_ids(self):
        known_values = {
            'header': 127,
            'fixed_leader': 0,
            'variable_leader': 128
        }
        for k, v in known_values.items():
            self.assertEqual(self.parsed_pd0[k]['id'], v)

    def test_fixed_data(self):
        known_values = {
            'bin_1_distance': 273,  # Centimeters
            'depth_cell_length': 100,  # Centimeters
            'pings_per_ensemble': 360,
            'minutes': 0,
            'seconds': 1,
            'hundredths': 0
        }
        for k, v in known_values.items():
            self.assertEqual(self.parsed_pd0['fixed_leader'][k], v)

    def test_variable_data(self):
        known_values = {
            'ensemble_number': 90,
            'rtc_year': 11,
            'rtc_month': 3,
            'rtc_day': 30,
            'rtc_hour': 16,
            'rtc_minute': 0,
            'rtc_second': 0,
            'pitch': -89,  # 0.01 degrees
            'roll': -92,  # 0.01 degrees
            'heading': 510,  # 0.01 degrees
            'temperature': 2267,  # 0.01 degrees
            'depth_of_transducer': 10,  # decimeters
            'bit_result': 0,
            'transmit_voltage': 131
        }
        for k, v in known_values.items():
            self.assertEqual(self.parsed_pd0['variable_leader'][k], v)

    def run_known_array_test(self, known_values, pd0_data):
        known_data = zip(known_values[0], known_values[1],
                        known_values[2], known_values[3])

        for pd0_bin, known_bin in zip(pd0_data, known_data):
            for pd0_beam, known_beam in zip(pd0_bin, known_bin):
                self.assertEqual(pd0_beam, known_beam)

    def test_echo_intensity(self):
        known_values = [
            [154, 138, 125, 122, 125, 131, 127, 113, 105, 100, 99, 100, 99, 93, 90, 88, 84, 80, 76, 74, 71, 69, 70, 69, 67, 64, 62, 61, 60, 58, 58, 56, 54, 51, 50, 49, 48, 47, 47, 46, 46, 46, 50, 87, 127, 159, 175, 165, 141, 117],  # NOQA
            [184, 153, 131, 126, 130, 128, 119, 110, 105, 102, 104, 106, 104, 98, 94, 91, 89, 86, 83, 81, 78, 75, 72, 68, 66, 63, 61, 60, 59, 59, 58, 58, 55, 53, 51, 50, 49, 48, 48, 47, 47, 48, 50, 82, 122, 154, 170, 163, 141, 118],  # NOQA
            [179, 142, 122, 123, 132, 132, 119, 103, 97, 96, 97, 97, 96, 92, 87, 84, 80, 77, 75, 72, 68, 64, 61, 58, 57, 56, 57, 57, 56, 54, 53, 52, 50, 48, 47, 46, 45, 45, 44, 43, 44, 50, 58, 96, 135, 163, 171, 159, 137, 117],  # NOQA
            [162, 143, 128, 124, 127, 132, 129, 119, 112, 106, 104, 105, 104, 100, 97, 95, 89, 86, 83, 80, 76, 74, 72, 70, 70, 68, 63, 61, 59, 58, 58, 57, 55, 53, 51, 50, 49, 49, 48, 48, 48, 49, 50, 81, 120, 149, 167, 164, 148, 127]  # NOQA
        ]

        self.run_known_array_test(known_values,
                                  self.parsed_pd0['echo_intensity']['data'])

    def test_correlation(self):
        known_values = [
            [87, 77, 101, 106, 115, 117, 134, 104, 113, 116, 112, 125, 135, 121, 118, 119, 121, 122, 121, 122, 121, 119, 121, 123, 118, 119, 116, 117, 118, 118, 115, 119, 112, 106, 104, 99, 101, 97, 94, 92, 89, 88, 80, 58, 70, 80, 108, 104, 92, 96],  # NOQA
            [124, 61, 80, 102, 117, 122, 121, 110, 111, 117, 118, 132, 132, 121, 119, 119, 127, 124, 125, 120, 128, 122, 123, 118, 119, 114, 113, 115, 119, 114, 117, 112, 114, 105, 103, 100, 97, 99, 93, 88, 91, 85, 71, 63, 76, 97, 96, 109, 95, 86],  # NOQA
            [130, 54, 77, 99, 116, 121, 127, 88, 114, 109, 120, 127, 126, 125, 116, 118, 121, 117, 121, 122, 121, 114, 116, 111, 106, 112, 106, 112, 115, 106, 110, 107, 103, 97, 93, 91, 86, 83, 80, 76, 74, 79, 80, 63, 76, 82, 117, 102, 100, 97],  # NOQA
            [90, 80, 98, 107, 112, 115, 129, 105, 115, 115, 114, 126, 129, 127, 119, 123, 117, 123, 124, 127, 120, 124, 120, 118, 124, 114, 120, 114, 120, 113, 113, 116, 111, 104, 104, 99, 97, 98, 94, 94, 91, 85, 73, 65, 71, 96, 90, 109, 104, 85]  # NOQA
        ]

        self.run_known_array_test(known_values,
                                  self.parsed_pd0['correlation']['data'])

    def test_velocity(self):
        known_values = [
            [99, 121, 132, 99, 119, 55, 72, 83, 13, 9, -61, -110, -159, -156, -136, -131, -96, -68, -41, -47, -30, -10, -7, 27, 32, 25, 39, 33, 51, 53, 36, 46, 53, 65, 68, 85, 59, 66, 52, 27, 5, 3, -3, 82, 418, 166, 77, 51, 9, 30],  # NOQA
            [130, 85, 85, 95, 43, 9, -81, 6, 4, 6, -19, 23, 40, 49, 43, -2, 48, 67, 93, 63, 37, 36, 28, 26, -16, 22, 22, 3, 14, -23, -52, -61, -65, -82, -70, -63, -83, -63, -76, -96, -80, -83, -58, 178, -207, -41, 8, -32, -69, 9],  # NOQA
            [-65, -40, -33, -39, -34, -34, -35, -41, -38, -31, -36, -41, -40, -43, -44, -47, -42, -38, -39, -40, -38, -35, -33, -34, -41, -37, -35, -33, -35, -29, -32, -32, -31, -33, -34, -36, -32, -35, -34, -33, -35, -40, -39, -26, 29, 19, 2, -23, 29, -18],  # NOQA
            [20, 17, 41, 16, -3, 36, 31, 68, 27, 9, 9, 25, 6, -3, 28, 43, 22, 17, 25, 23, 22, 29, 13, 23, 32, 14, 4, 5, 31, 28, 17, 14, 21, 14, 34, 25, 16, 25, 7, 24, 31, 26, 16, 290, -32768, 110, 18, 13, -16, 268]  # NOQA
        ]

        self.run_known_array_test(known_values,
                                  self.parsed_pd0['velocity']['data'])

    def test_percent_good(self):
        known_values = [
            [33, 19, 37, 34, 30, 27, 25, 30, 29, 25, 23, 10, 9, 13, 16, 18, 15, 10, 11, 12, 13, 13, 14, 18, 16, 19, 21, 15, 10, 13, 8, 10, 18, 20, 20, 29, 32, 35, 33, 38, 38, 43, 29, 6, 3, 9, 16, 24, 13, 9],  # NOQA
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  # NOQA
            [48, 75, 38, 24, 34, 40, 30, 39, 20, 11, 4, 2, 0, 1, 2, 1, 1, 0, 0, 0, 0, 0, 0, 2, 2, 4, 1, 0, 0, 1, 0, 0, 1, 1, 2, 4, 5, 5, 8, 12, 17, 20, 56, 93, 96, 88, 71, 66, 79, 90],  # NOQA
            [18, 5, 23, 40, 35, 32, 44, 29, 50, 62, 71, 86, 90, 84, 81, 80, 83, 89, 87, 86, 85, 86, 85, 79, 80, 76, 76, 83, 89, 85, 90, 89, 80, 77, 76, 65, 62, 59, 57, 48, 44, 35, 13, 0, 0, 2, 12, 8, 6, 0]  # NOQA
        ]

        self.run_known_array_test(known_values,
                                  self.parsed_pd0['percent_good']['data'])


class TestPD15(unittest.TestCase):
    test_dir = os.path.dirname(os.path.abspath(__file__))
    parsed_pd0 = read_PD15_file(os.path.join(test_dir, '140B97C6'),
                               header_lines=2)

    def test_print_data(self):
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(self.parsed_pd0)

    def test_header_fields(self):
        known_values = {
            'id': 0x7f,
            'number_of_bytes': 952,
            'data_source': 127,
            'number_of_data_types': 6,
            'spare': 0
        }
        for k, v in known_values.items():
            self.assertEqual(self.parsed_pd0['header'][k], v)

    def test_ids(self):
        known_values = {
            'header': 127,
            'fixed_leader': 0,
            'variable_leader': 128
        }
        for k, v in known_values.items():
            self.assertEqual(self.parsed_pd0[k]['id'], v)


#class TestPD15String(unittest.TestCase):
#    pd15_hex = "f114f014f112f909f40cf30df40df30cf50cf60bf50afb03f90af906fb04f0f2f113f013f213f40df50ef50ef30df40cf60bf50d0001f905fa09f907f904f1eb000000000000000000000000000000007373777005390703180000000000fdd3"  # NOQA
#
#    parsed_pd0 = read_PD15_hex(pd15_hex)
#
#    def test_header_fields(self):
#        known_values = {
#            'id': 0x7f,
#            'number_of_bytes': 952,
#            'data_source': 127,
#            'number_of_data_types': 6,
#            'spare': 0
#        }
#        for k, v in known_values.items():
#            self.assertEqual(self.parsed_pd0['header'][k], v)
#
#    def test_ids(self):
#        known_values = {
#            'header': 127,
#            'fixed_leader': 0,
#            'variable_leader': 128
#        }
#        for k, v in known_values.items():
#            self.assertEqual(self.parsed_pd0[k]['id'], v)


if __name__ == '__main__':
    unittest.main()
