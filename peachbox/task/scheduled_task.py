import peachbox
from peachbox.task import Task
from peachbox.scheduler.scheduler import Scheduler
from peachbox.scheduler.event import Event
from multiprocessing import Process
from multiprocessing import Process
import pubsub

"""
Copyright 2015 D. Britzger, P. Pahl, S. Schubert

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""


class ScheduledTask(Task):
    """Peachbox scheduled task.
    A scheduled task ....
    """

    def __init__(self):
        super(ScheduledTask, self).__init__()
        self.source  = None
        self.sources = None
        self.sink    = None
        self.process = None
        self.payload = None

    def execute(self):
        """User defined ScheduledTaks must implement 'execute'"""
        raise NotImplementedError

    def run_scheduled_task(self, param={}):
        """ run_scheduled_task is executed by peachbox"""
        if self.source:
            self.source.set_param(param)
        if self.sources:
            for s in self.sources: s.set_param(param)
        if self.sink:
            self.sink.set_param(param)

        self.process = Process(target=self.run_in_process, args=(Scheduler.Instance(),))
        self.process.start()

    def run_in_process(self, scheduler):
       self.execute()
       self.tear_down()
       e = self.get_event_finished() 
       scheduler.publish(e)

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
        
