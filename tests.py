import unittest
from trdi_adcp_readers.pd15.pd0_converters import PD15_file_to_PD0
from trdi_adcp_readers.pd0.pd0_parser import parse_pd0_bytearray
import pprint


class TestPD0(unittest.TestCase):

    def setUp(self):
        self.test_pd0 = PD15_file_to_PD0('./140B97C6', header_lines=2)
        self.parsed_pd0 = parse_pd0_bytearray(self.test_pd0)

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


if __name__ == '__main__':
    unittest.main()
