import time


class Event(object):
    def __init__(self, id):
        self._id = id
        self._payload = None
        self._param = {}

    def getId(self):
        return self._id
    
    def setPayload(self, payload):
        self._payload = payload
    
    def getPayload(self):
        return self._payload

    def getParam(self):
        self._param['timestamp'] = time.time()
        self._param['payload'] = self._payload
        return self._param


class TimedEvent(Event):
    def getNextTimestamp(self, timestamp):
        return 0


class TimeIntervalEvent(TimedEvent):
    def __init__(self, delay):
        super(TimeIntervalEvent, self).__init__("TimedIntervalEvent_%d" % delay)
        self.setDelay(delay)

    def setDelay(self, delay):
        self._delay = delay

    def getDelay(self):
        return self._delay

    def getNextTimestamp(self, timestamp):
        return timestamp + self._delay

    def getParam(self):
        super(TimeIntervalEvent, self).getParam()
        self._param['path'] = 'example path'
        print "TimeIntervalEvent:getParam():", self._param
        return self._param


class AbsDateTimeEvent(TimedEvent):
    def __init__(self, dayOfMonth=None, dayOfWeek=None, hour=None, min=None, sec=None):
        pass

    def getNextTimestamp(self, timestamp):
        return timestamp + self._delay
