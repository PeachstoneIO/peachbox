import time
from pubsub import pub


class Event(object):
    def __init__(self, event_id):
        self._id = event_id
        self._payload = None
        self._param = {"_id": self._id}

    def get_id(self):
        return self._id
    
    def set_payload(self, payload):
        self._payload = payload
    
    def get_payload(self):
        return self._payload

    def get_param(self):
        self._param['timestamp'] = time.time()
        self._param['payload'] = self._payload
        return self._param


class TimedEvent(Event):
    def __init__(self, event_id, periodic = False):
        super(TimedEvent, self).__init__(event_id)
        self._is_periodic = periodic
    
    def is_periodic(self):
        return self._is_periodic

    def get_delay_to_next(self):
        return None


class DelayedTimedEvent(TimedEvent):
    def __init__(self, delay, periodic = False):
        super(DelayedTimedEvent, self).__init__("DelayedTimedEvent_%d" % delay, periodic)
        self.set_delay(delay)

    def set_delay(self, delay):
        self._delay = delay

    def get_delay(self):
        return self._delay

    def get_delay_to_next(self):
        return self._delay

    def get_param(self):
        super(DelayedTimedEvent, self).get_param()
        self._param['path'] = 'example path'
        return self._param


class ConditionalEvent(Event):
    def __init__(self, event_id):
        super(ConditionalEvent, self).__init__("ConditionalEvent_%s" % event_id)
        self._events = {}
        self._event_counters = {}
 
    def _reset_child_event_counters(self):
        for event_id in self._events:
            self._event_counters[event_id] = 1

    def _register_child_event(self, param={}):
        event_id = param["_id"]
 
        if event_id in self._events:
            if self._event_counters[event_id] == 0:
                self._reset_child_event_counters()

            self._event_counters[event_id] -= 1
 
        # retrieve and merge the param values of all child events:
        sum_counters = 0
        aggregated_param = {}
        for event_id in self._events:
            sum_counters += self._event_counters[event_id]
            aggregated_param.update(self._events[event_id].get_param())
 
        # add the param values of the conditional event:
        aggregated_param.update(self.get_param())

        if sum_counters == 0:
            # all child events have been published, 
            # i.e. reset counters and publish conditional event:
            self._reset_child_event_counters()
            pub.sendMessage(self._id, param=aggregated_param)

    def subscribe(self, event):
        # subscribe this conditional event to another event:
        self._events[event._id] = event
        self._event_counters[event._id] = 1

    def subscribe_to_child_events(self):
        # This method finally subscribes this conditional event to all child events.
        # It will only be called if some task is subscribed to this conditional event
        # by the scheduler subscribe method and the conditional event is not yet registerd. 
        # If the conditional element is never used, this method will not be called.
   
        for event_id in self._events:
            if isinstance(self._events[event_id], ConditionalEvent):
                # call this method recursively if a child event is a conditional event:
                self._events[event_id].subscribe_to_child_events()

            # call self._register_child_event always if a child event is published: 
            pub.subscribe(self._register_child_event, event_id)
