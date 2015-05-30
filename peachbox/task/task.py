from peachbox.scheduler.scheduler import Scheduler
from peachbox.scheduler.event import Event
from multiprocessing import Process
#from threading import Thread

import peachbox


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

from multiprocessing import Process
import pubsub

class Task(object):
    """Base class for data tasks:
    """

    def __init__(self):
        self.source  = None
        self.sources = None
        self.sink    = None
        self.process = None
        self.payload = None

    def execute(self, param={}):
        if self.source:
            self.source.set_param(param)
        if self.sources:
            for s in self.sources: s.set_param(param)
        if self.sink:
            self.sink.set_param(param)

        self.process = Process(target=self.run_in_process, args=(Scheduler.Instance(),))
        self.process.start()

    def run_in_process(self, scheduler):
       self._execute()
       self.tear_down()
       e = self.get_event_finished() 
       scheduler.publish(e)


#    def execute(self, param={}):
#        self.process = Process(target=self.run_in_process, args=(param,))
#        self.process.start()
#
#    def run_in_process(self, param):
#        if self.source and self.sink:
#            self.source.set_param(param)
#            self.sink.set_param(param)
#        else:
#            raise ValueError("Source/Sink not defined.")
#
#        t = Thread(target=self._execute_notify_teardown(), args=())
#        t.daemon = False
#        t.start()
#
#    def _execute_notify_teardown(self):
#        self.notify_scheduler(self.started())
#        self.process = Process(target=self._execute, args=())
#        self.process.start()
#        self.process.join()
#        self.notify_scheduler(self.finished())
#        self.tear_down()

    def _execute(self):
        raise NotImplementedError

    def tear_down(self):
        peachbox.Spark.Instance().stop()

    def get_event_finished(self):
        e = Event(self.__class__.__name__ + "Finished")
        status = e.status()
        try:
            status['payload'] = self.payload 
        except AttributeError:
            pass
        return e
        
