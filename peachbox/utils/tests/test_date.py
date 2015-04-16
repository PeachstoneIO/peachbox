import unittest
import pytz
from datetime import datetime

from peachbox.utils.date import Date

class TestDate(unittest.TestCase):
    def test_construct_w_timezone(self):
        d = Date(timezone=pytz.utc)
        assert d.timezone is not None

    def test_construct_w_date_wo_tzinfo(self):
        d = Date(timezone=pytz.utc, date=datetime(2014,04,01))
        assert d.datetime().tzinfo is not None

    # case0: datetime is None 
    def test_localize_case0(self):
        d = Date()
        assert (d is not None) and (d._datetime is None)

    # case1: datetime object has tzinfo
    def test_localize_case1(self):
        d = Date(date=datetime(2014,04,01, tzinfo=pytz.utc))
        assert d.datetime().tzinfo is not None

    # case2: datetime object has no tzinfo, tzinfo given in ctor
    def test_localize_case2(self):
        d = Date(timezone=pytz.utc, date=datetime(2014,04,01))
        self.assertEqual(pytz.utc, d.datetime().tzinfo)

    # TODO
    # case3: datetime object has no tzinfo, tzinfo not given in ctor
    #def test_localize_case3(self):
    #    d = Date()
    #    self.assertRaises(Exception, d.localize(datetime(2014,04,01)))


    def test_parse(self):
        d = Date(timezone=pytz.timezone('Europe/Berlin'))
        dt = d.parse('20140402').datetime()
        self.assertEqual(dt.year,  2014)
        self.assertEqual(dt.month, 04)
        self.assertEqual(dt.day,   02)

    def test_seconds_from_start_epoch(self):
        d = Date(timezone=pytz.utc)
        d.parse('19700101')
        self.assertEqual(0, d.seconds())

    def test_seconds_with_tzinfo(self):
        d = Date(timezone=pytz.timezone('Europe/Berlin'))
        d.parse('19700101')
        self.assertEqual(-3600, d.seconds())

    def test_tz_berlin(self):
        d = Date.tz_berlin(2014,4,2)
        self.assertEqual(2014, d.datetime().year)
        self.assertEqual(4, d.datetime().month)
        self.assertEqual(2, d.datetime().day)
        d2 = pytz.timezone('Europe/Berlin').localize(datetime(2014,4,2))
        self.assertEqual(d2, d.datetime())

    def test_from_utime(self):
        utime = Date.tz_berlin(2014,3,1).seconds()
        d = Date.from_utime(pytz.timezone('Europe/Berlin'), utime)
        self.assertEqual(utime, d.seconds())
        





