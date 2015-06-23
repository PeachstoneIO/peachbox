# Copyright 2015 Philipp Pahl, Sven Schubert
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import multiprocessing
from pubsub import pub
import peachbox.scheduler

class Scheduler(object):

    _active_instance = None

    @staticmethod
    def Instance():
        if not Scheduler._active_instance:
            Scheduler._active_instance = Scheduler()
        return Scheduler._active_instance

    def __init__(self):
        if Scheduler._active_instance:
            raise ValueError(
                "Cannot run multiple Scheduler instances at once."
                "Use peachbox.Scheduler.Instance() to access existing instance.")
        Scheduler._active_instance = self
        self._events = []
        self._periodic_events = []
        self._conditional_events = []
        self._event_status = multiprocessing.Manager().dict() 

    def publish(self, event):
        print 'publishing ' + event.name()
        self.set_event_status(event.name(), event.status())
        pub.sendMessage(event.name(), param=self.get_event_status(event.name()))

    def subscribe(self, task, event):
        if isinstance(event, peachbox.scheduler.PeriodicEvent):
            self._periodic_events.append(event)
        elif isinstance(event, peachbox.scheduler.ConditionalEvent):
            self._conditional_events.append(event)
        else:
            self._events.append(event)

        pub.subscribe(task.run_scheduled_task, event.name())

    def get_event_status(self, event_name):
        s = {}
        if event_name in self._event_status:
            s = self._event_status[event_name]
        return s

    def set_event_status(self, event_name, status):
        self._event_status[event_name] = status
    
    def run(self):
        for e in self._events:
            self.publish(e)

        for e in self._periodic_events:
            e.start()
        self.run_conditional_events()

    def run_conditional_events(self):
        for e in self._conditional_events:
            self.publish(e)
            for c in e._conditions:
                if isinstance(c, peachbox.scheduler.PeriodicEvent) and c not in self._periodic_events:
                    c.start()


