import peachbox
import multiprocessing
import time
import pubsub

class Event(object):
    def __init__(self, name):
        self._name = name
        self._status = {}

    def name(self):
        return self._name

    def status(self):
        self._status.update({'name':self.name()})
        return self._status

    def set_status(self, status):
        self._status = status

class ConditionalEvent(Event):
    def __init__(self, name):
        super(ConditionalEvent, self).__init__(name)
        self.scheduler = peachbox.scheduler.Scheduler.Instance()
        self._conditions = []

    def subscribe(self, event):
        self._conditions.append(event)
        status = self.get_current_status() 
        if not 'subscriptions' in status: status['subscriptions'] = {}
        status['subscriptions'].update({event.name():1})

        self.set_status(status)
        self.persist_status()

        pubsub.pub.subscribe(self.track_event, event.name())

    def track_event(self, param):
        status = self.get_current_status()
        if 'payload' not in status: status['payload'] = {}
        if 'payload' in param: status['payload'].update(param['payload'])

        subscriptions = self.get_subscriptions()
        event_name = param['name']
        subscriptions[event_name] = max(0, subscriptions[event_name]-1)

        if self.sum_event_counts(subscriptions) == 0:
            status['subscriptions'] = self.reset_subscriptions()
            self.set_status(status)
            self.scheduler.publish(self)
        else:
            status['subscriptions'] = subscriptions
            self.set_status(status)
            self.persist_status()

    def get_subscriptions(self):
        return self.scheduler.get_event_status(self.name())['subscriptions']

    def sum_event_counts(self, subscriptions):
        return sum([v for (k,v) in subscriptions.iteritems()])

    def get_current_status(self):
        return self.scheduler.get_event_status(self.name())
    
    def persist_status(self):
        self.scheduler.set_event_status(self.name(), self.status())

    def reset_subscriptions(self):
        s = self.get_subscriptions()
        for k in s:
            s[k] = 1
        return s

class PeriodicEvent(Event):
    def __init__(self, name, period):
        super(PeriodicEvent, self).__init__(name)
        self._period = period
        self._scheduler = peachbox.scheduler.Scheduler.Instance()
        self.process = None
        self._max_publications = -1

    def start(self):
        self.process = multiprocessing.Process(target=self.publish, args=(self._scheduler,))
        self.process.start()

    def publish(self, scheduler):
        time.sleep(self._period)
        while self._max_publications is not 0:
            scheduler.publish(self)
            time.sleep(self._period)
            if self._max_publications > 0: self._max_publications -= 1

