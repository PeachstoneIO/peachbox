from peachbox.scheduler.scheduler import Scheduler
from peachbox.scheduler.event import Event

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

    def execute(self, param={}):
        self.process = Process(target=self.run_in_process, args=(param,))
        self.process.start()

    def run_in_process(self, param):
        if self.source and self.sink:
            self.source.set_param(param)
            self.sink.set_param(param)
        else:
            raise ValueError("Source/Sink not defined.")
        self._execute()
        self.tear_down()
        self.notify_scheduler()

    def _execute(self):
        raise NotImplementedError

    def tear_down(self):
        pass


    def notify_scheduler(self):
        Scheduler.Instance().publish(self.finished())

    def finished(self):
        return Event(self.__class__.__name__ + "Finished")

