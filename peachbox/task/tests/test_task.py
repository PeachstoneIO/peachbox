import unittest
import peachbox.connector.source
import peachbox.connector.sink
import time
import peachbox

class TestConnector(peachbox.connector.Connector):
    def set_param(self, param):
        self.param = param

class MyTask(peachbox.task.Task):
    def __init__(self):
        self.source  = TestConnector() 
        self.sources = [TestConnector(), TestConnector()]
        self.sink    = TestConnector() 

    def _execute(self):
        pass


class TestTask(unittest.TestCase):
    def test_execute(self):
        t = MyTask()
        t.execute(param={'k':'v'})
        self.assertEqual('v', t.source.param['k'])
        self.assertEqual('v', t.sink.param['k'])
        self.assertEqual('v', t.sources[0].param['k'])
        self.assertEqual('v', t.sources[1].param['k'])

        t.process.join()



