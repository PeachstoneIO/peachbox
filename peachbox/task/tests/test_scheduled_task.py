import unittest
import time
import peachbox.connector.source
import peachbox.connector.sink
import peachbox
from peachbox.task import ScheduledTask

class TestConnector(peachbox.connector.Connector):
    def set_param(self, param):
        self.param = param

class MyTask(ScheduledTask):
    def __init__(self):
        self.source  = TestConnector()
        self.sources = [TestConnector(), TestConnector()]
        self.sink    = TestConnector()

    def execute(self):
        print "In execute"

class TestTask(unittest.TestCase):
    def test_execute(self):
        assert False
#        t = MyTask()
#        t.run_scheduled_task(param={'k':'v'})
#        self.assertEqual('v', t.source.param['k'])
#        self.assertEqual('v', t.sink.param['k'])
#        self.assertEqual('v', t.sources[0].param['k'])
#        self.assertEqual('v', t.sources[1].param['k'])
#
#        t.process.join()

