from ctc.abstract_cycle_time import AbstractCycleTime
from ctc import CycleTimeCalc
import unittest
import numpy as np
import datetime
import tempfile

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
        self.assertAlmostEqual(first=float(np.mean(c2-c2r)), second=0, places=15)

    def test_from_string(self):

        z = CycleTimeCalc('dev', '/data/misc/cycle_time_calc').calc_time("OBJECT alt=45 az=0 seq=3/Red/4")
        # print(z)
        self.assertGreater(a=z, b=0)

    def test_ut(self):

        z = CycleTimeCalc('dev', '/data/misc/cycle_time_calc')
        z.set_start_time(datetime.datetime.now(datetime.timezone.utc))
        # print(datetime.datetime.now(datetime.timezone.utc))
        a = z.calc_time("WAIT ut=16:37:00")
        # print(a / 60)
        # self.assertGreater(a=z, b=0)

    def test_ut_time_length(self):

        with tempfile.TemporaryDirectory() as base_folder:
            z = CycleTimeCalc('dev', base_folder)
            z.set_start_time(datetime.datetime(2026, 10, 27, 20, 0, 0, tzinfo=datetime.timezone.utc))
            self.assertEqual(z.calc_time("WAIT ut=21:00"), 3600)
            self.assertEqual(z.time_lenght_sec, 3600)
            # next ut counts from 21:00, not from start time
            self.assertEqual(z.calc_time("WAIT ut=21:30:00"), 1800)
            self.assertEqual(z.time_lenght_sec, 5400)
            # ut already passed
            self.assertEqual(z.calc_time("WAIT ut=21:00"), 0)
            self.assertEqual(z.time_lenght_sec, 5400)
            # after midnight
            self.assertEqual(z.calc_time("WAIT ut=00:30"), 3 * 3600)
            self.assertEqual(z.time_lenght_sec, 5400 + 3 * 3600)

    def test_ut_nearest_occurrence(self):

        with tempfile.TemporaryDirectory() as base_folder:
            z = CycleTimeCalc('dev', base_folder)
            # ut more than 12 h ahead is treated as passed (yesterday)
            z.set_start_time(datetime.datetime(2026, 10, 27, 1, 0, 0, tzinfo=datetime.timezone.utc))
            self.assertEqual(z.calc_time("WAIT ut=23:00"), 0)
            self.assertEqual(z.time_lenght_sec, 0)
            z.reset_time()
            # ut less than 12 h ahead after midnight
            z.set_start_time(datetime.datetime(2026, 10, 27, 23, 50, 0, tzinfo=datetime.timezone.utc))
            self.assertEqual(z.calc_time("WAIT ut=00:30"), 40 * 60)
            z.reset_time()
            # ut less than 12 h before midnight
            z.set_start_time(datetime.datetime(2026, 10, 27, 0, 10, 0, tzinfo=datetime.timezone.utc))
            self.assertEqual(z.calc_time("WAIT ut=23:50"), 0)

    def test_ut_nearest_occurrence_cases(self):

        # (start h, start m, ut, expected wait in sec)
        cases = [
            (20, 0, "21:00", 3600),             # ut ahead
            (20, 0, "19:00", 0),                # ut passed
            (20, 0, "20:00", 0),                # ut now
            (23, 50, "00:30", 40 * 60),         # ut ahead, after midnight
            (0, 10, "23:50", 0),                # ut passed, before midnight
            (1, 0, "23:00", 0),                 # 22 h ahead -> yesterday, passed
            (13, 0, "00:30", 11.5 * 3600),      # 11.5 h ahead, after midnight
            (11, 0, "22:59:59", 12 * 3600 - 1), # just under 12 h ahead
            (11, 0, "23:00:01", 0),             # just over 12 h ahead -> yesterday, passed
            (12, 1, "00:00:30", 12 * 3600 - 30),  # just under 12 h ahead, after midnight
            (11, 59, "00:00:30", 0),            # just under 12 h passed, after midnight
            (2, 0, "14:00:01", 0),              # just over 12 h ahead -> yesterday, passed
            # exactly 12 h: ut today is 12 h ahead -> waits, ut today is 12 h ago -> 0
            # (tie depends on which side of midnight ut is; check with TOI wait_ut_sec)
            (11, 0, "23:00", 12 * 3600),
            (12, 0, "00:00", 0),
        ]
        with tempfile.TemporaryDirectory() as base_folder:
            z = CycleTimeCalc('dev', base_folder)
            for start_h, start_m, ut, expected in cases:
                with self.subTest(start=f'{start_h:02d}:{start_m:02d}', ut=ut):
                    z.reset_time()
                    z.set_start_time(datetime.datetime(2026, 10, 27, start_h, start_m, 0,
                                                       tzinfo=datetime.timezone.utc))
                    self.assertEqual(z.calc_time(f"WAIT ut={ut}"), expected)
                    self.assertEqual(z.time_lenght_sec, expected)

    def test_ut_nearest_occurrence_sequence(self):

        # start at 22:00, plan crosses midnight
        with tempfile.TemporaryDirectory() as base_folder:
            z = CycleTimeCalc('dev', base_folder)
            z.set_start_time(datetime.datetime(2026, 10, 27, 22, 0, 0, tzinfo=datetime.timezone.utc))
            self.assertEqual(z.calc_time("WAIT ut=23:30"), 1.5 * 3600)
            self.assertEqual(z.calc_time("WAIT ut=01:00"), 1.5 * 3600)
            # 23:30 is now 1.5 h in the past, not 22.5 h ahead
            self.assertEqual(z.calc_time("WAIT ut=23:30"), 0)
            self.assertEqual(z.calc_time("WAIT sec=600"), 600)
            self.assertEqual(z.calc_time("WAIT ut=02:00"), 50 * 60)
            self.assertEqual(z.time_lenght_sec, 4 * 3600)


if __name__ == '__main__':
    unittest.main()