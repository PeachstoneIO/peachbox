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
    """Base class for data pipelines:

    >>> from model.review_edge import ReviewEdge
    >>>
    >>> class ExampleTaskImplementation(peachbox.pipeline.Task):
    >>>      def __init__(object):
    >>>          # connector can be pre-defined or manually defined
    >>>          self.source = peachbox.connector.source.JSON()
    >>>          # MasterDataSet: model has an output target, which is determined by partition_time_range
    >>>          self.sink   = peachbox.connector.sink.MasterDataSet(model=ReviewEdge)
    >>>
    >>>      # param are passed to source and sink beforehand
    >>>      # super class has execute(): Sets params in connectors and sends out "ImportReviewsFinished"
    >>>      def _execute(self):
    >>>          df = self.source.data_frame()
    >>>          reviews = df.validate(['user_id', 'product_id', 'time']) \ 
    >>>              .map(lambda entry: ReviewEdge(user_id=entry['user_id'], 
    >>>                  product_id=entry['product_id'], 
    >>>                  true_as_of_seconds=entry['time'])
    >>>          self.sink.absorb(reviews)
    >>>
    >>>      # optional: is called by execute()
    >>>      def tear_down(self):
    >>>          pass

    """
        

    def __init__(self):
        self.source = None
        self.sink   = None
        self.process = None
        self.payload = None

    def execute(self, param={}):
       if self.source and self.sink:
           self.source.set_param(param)
           self.sink.set_param(param)
       else:
           raise ValueError("Source/Sink not defined.")

       self.process = Process(target=self.run_in_process, args=(Scheduler.Instance(),))
       self.process.start()
       #self.run_in_process()

    def run_in_process(self, scheduler):
       #self.notify_scheduler(self.started())
       self._execute()
       #self.notify_scheduler(self.finished())
       self.tear_down()
       e = self.get_event_finished() 
       scheduler.publish(e)
       #Scheduler.Instance().publish(self.get_event_finished())


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
        pass

    def get_event_finished(self):
        e = Event(self.__class__.__name__ + "Finished")
        status = e.status()
        try:
            status['payload'] = self.payload 
        except AttributeError:
            pass
        return e
        
