import unittest
from peachbox.scheduler.scheduler import Scheduler
from peachbox.scheduler.event import Event
from peachbox.scheduler.event import TimeIntervalEvent
import peachbox.task
import peachbox.connector.source
import peachbox.connector.sink


class TestTask1(peachbox.task.Task):
    def __init__(self):
        super(TestTask1, self).__init__()
        self.source = peachbox.connector.source.JSON()
        self.sink = peachbox.connector.sink.MasterData()

    def _execute(self):
        print "executing task 1"

    def finished(self):
        event = super(TestTask1, self).finished()
        event._param['path'] = 'task 1 result path'
        return event


class TestTask2(peachbox.task.Task):
    def __init__(self):
        super(TestTask2, self).__init__()
        self.source = peachbox.connector.source.JSON()
        self.sink = peachbox.connector.sink.MasterData()

    def _execute(self):
        print "executing task 2"


class TestScheduler(unittest.TestCase):

    def test_get_instance(self):
        scheduler = Scheduler.Instance()
        task1 = TestTask1()
        task2 = TestTask2()
        timedEvent = TimeIntervalEvent(delay=1)
        scheduler.subscribe(task1, timedEvent)
        scheduler.subscribe(task2, task1.finished())
        scheduler.publish(timedEvent)
        scheduler.run()

        assert True
