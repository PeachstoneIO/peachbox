import unittest
import peachbox.connector.source
import peachbox.connector.sink
import time
import peachbox


class MyTask(peachbox.task.Task):
    def __init__(self):
        self.source = peachbox.connector.source.JSON()
        self.sink   = peachbox.connector.sink.MasterData()

    def _execute(self):
        print 'Running MyTask'
        #print peachbox.Spark.Instance().context().parallelize([1,2,3]).collect()
        time.sleep(1)

    def tear_down(self):
        peachbox.Spark.Instance().stop()


class TestTask(unittest.TestCase):
    def test_simple_execute(self):
        pass
        #t = MyTask()
        #start_1 = time.time()
        #t.execute(param={'path':'/'})

        #t2 = MyTask()
        #start_2 = time.time()
        #t2.execute(param={'path':'/'})

        #t.process.join()
        #end_1 = time.time()

        #t2.process.join()
        #
        #assert (start_2-start_1) < 1
        #assert (end_1-start_1)   > 1


