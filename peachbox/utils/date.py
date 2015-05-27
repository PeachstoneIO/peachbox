from dateutil import parser
from datetime import datetime
import pytz

class Date(object):

    @staticmethod
    def tz_berlin(year, month, day):
        return Date(date=datetime(year, month, day), timezone=pytz.timezone('Europe/Berlin'))

    @staticmethod
    def from_utc_utime(timezone, seconds):
        d = datetime.fromtimestamp(float(seconds))
        d = d.replace(tzinfo=pytz.utc)
        d.astimezone(timezone)
        return Date(timezone=timezone, date=d)

    def __init__(self, timezone=None, date=None):
        self.timezone  = timezone
        self._datetime = self.localize(date)
        self.start_epoch  = datetime(1970,1,1, tzinfo=pytz.utc)

    def localize(self, _datetime):
        dt = None
        if _datetime is None:
            dt = None
        elif _datetime.tzinfo is not None:
            dt = _datetime
        elif self.timezone:
            dt = self.timezone.localize(_datetime, is_dst=True)
        else:
            raise Exception('Date.localize: datetime has no tz or Date has no timezone information')
        return dt

    # @parameter string: YYYYMMDD
    def parse(self, string):
        dt = parser.parse(string)
        self._datetime = self.localize(dt)
        return self

    def datetime(self):
        if not self._datetime.tzinfo:
            raise 'datetime does not have timezone information'
        return self._datetime

    def seconds(self):
        return int((self.datetime()-self.start_epoch).total_seconds())
