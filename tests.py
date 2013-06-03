import unittest
from trdi_adcp_readers.pd15.pd0_converters import PD15_string_to_PD0
from trdi_adcp_readers.pd0.pd0_parser import parse_pd0_bytearray
import pprint


class TestPD0(unittest.TestCase):

    def setUp(self):
        self.test_pd15 = """_w~x@p@FD`AM@Hx@t@Er@aPC@@@rJLmA@EtDJF`AFPAd@@E@@`CPAp@A@A|@@C{~_Qvh@DL@@PTr@Bp@Hp@@@MRBpPd@@O|@z@H@@AR@@C`GCPT]C`@@@@@@@@LFB`COBZK^?]_|c@HlJ@@@F@@@@bxYM^?^?}M`i|@@@BH@i|@@@@@@@@@@@@TCPT]C`@@@@@A[O^?{^?pX@DpBP^?^?S^?M`@p@Fo^?J`@~@Oc^?`o^?H^?pl@F`Cp^?|O^?}o|R@Gt@io^?N^?rH@|_~]^?^?s^?^O~n^?{o^?|_|z@M_^?@@@@@KW^?m_^?N^?^?{^?A`Cg^?}w^?~O|F@Os^?|^?^?d^?rl@tO|h@Mk^?N@Bb^?~w^?wO}E@Mk^?^?_^?o^?^?c^?l^?^?H^?p\@P`CY^?|g^?@@@L@MS^?xO^?s^?pP@vO^?e^?~_^?EPCZ^?pT@x_|{@L^?^?n_^?a^?pH@v_^?s^?^?s^?~_^?w^?}G^?~o^?j^?|k^?zo^?w^?pX@uo^?r^?^?[^?BpCV^?~_^?~_^?~^?^?w^?uo^?s^?q`@@PCp^?^?g^?^?^?^?G^?~W^?~O^?h^?^?[^?xo^?r^?p|@sO^?t^?^?{^?^?^?^?a^?}_^?~O|R@M^?^?z^?^?z^?qX@to^?b^?^?S^?C@C]^?~s^?|^?|M@NC^?vO^?{^?pt@}o|E@Ow^?E`C\^?~s^?~^?|F@Mg^?zo^?z^?qh@|_^?g^?^?g^?A@@@@dQROueTTSqaSCiQT\~twLZ^?v[z}PEl{RCpwNsqRPdyMSSuLTZBHhyVxqkvzXgqe[DUDPTYPQT}KUeAXUf]dZFVNbyFM`HbCaE}dXVUZVuu\WUqaWuq[XE}iYVee\gEr\Fml[fydYvYjYVQdYfUbYf]gYVQgYvYhZfaiZvifZFikYfaiZfigZFqiYvmkZFaiZvagZvqiZFmkZVqi[Fel[F|@@{Zilk_Jql^?Ps}KUu]GTum[ItL{Omk~woZrnkkBslKZxn{r~o{n}o{~woKn{jZzkki~bhZR[gYz`gY~`hYr`hJF\hI~afIv[giNXeYbNdiFSbh~MdHbMbxzFbxfLaHfGbhBF`x]}`g~D^W}|`W]|^Wyt^W]|\g]u^gAu\wan\wAv[GAn]Fin[GIh[VipYfmh[fQiYfqcYvUkXVYcZe}dXV`@AAt@O@h\@ChME`AEApl@@U\G@@E[D@ALAaD@SpLc@CHNFP@}CQX@AT\B@@A`HP@UKAl@Opd_@BxUH@@_Hq|@CC`H@@A[B`@@VBH@BsXd@A@oH`@KMbP@CSD[@@UBCp@ATqL@@dtX@@IHG@@CPqp@ATHZ@@UDE@@DRa\@@t`V@@QHFp@DQAd@@tXZ@@IFEp@BRQ`@@d`X@@EJD`@BSqD@@d|^LHjf"""  # NOQA

        self.test_pd0 = PD15_string_to_PD0(self.test_pd15)
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
