from ctc.abstract_cycle_time import AbstractCycleTime
from ctc import CycleTimeCalc
import unittest
import numpy as np

class TestCycleTime(unittest.TestCase):

    def test_alt_az_distance(self):

        az_start = 0
        az_end = 90
        alt_start = 0
        alt_end = 0
        res = 90
        c = AbstractCycleTime._alt_az_distance(start_az=az_start, end_az=az_end, start_alt=alt_start, end_alt=alt_end)
        self.assertEqual(res, c)

    def test_vector(self):

        az_end = 90
        alt_end = 0
        c2r = np.array([0.0, 0.0, 1.0])
        c2 = AbstractCycleTime._vector(alt=alt_end, az=az_end)
        self.assertAlmostEqual(first=np.mean(c2-c2r), second=0, places=15)

    def test_from_string(self):

        z = CycleTimeCalc('dev', '/data/misc/cycle_time_calc').calc_time("OBJECT alt=45 az=0 seq=3/Red/4")
        # print(z)
        self.assertGreater(a=z, b=0)


if __name__ == '__main__':
    unittest.main()