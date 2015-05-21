import unittest
from peachbox.scheduler.scheduler import Scheduler
import peachbox.scheduler.event as scheduler_event
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


class TestTask3(peachbox.task.Task):
    def __init__(self):
        super(TestTask3, self).__init__()
        self.source = peachbox.connector.source.JSON()
        self.sink = peachbox.connector.sink.MasterData()

    def _execute(self):
        print "executing task 3"


class TestScheduler(unittest.TestCase):

    def test_get_instance(self):
        scheduler = Scheduler.Instance()

        task1 = TestTask1()
        task2 = TestTask2()
        task3 = TestTask3()

        # create a periodic timed event:
        timedEvent = scheduler_event.DelayedTimedEvent(delay=1, periodic=True)

        # create a conditional event:
        condEvent = scheduler_event.ConditionalEvent("cond1")
        # publish this event every second and after task1 has been finished:
        condEvent.subscribe(timedEvent)
        condEvent.subscribe(task1.finished())

        # execute task1 every second:
        scheduler.subscribe(task1, timedEvent)

        # execute task2 when task1 is finished:
        scheduler.subscribe(task2, task1.finished())
        
        # execute task3 every second and after task1 has been finshed
        scheduler.subscribe(task3, condEvent)

        # start the timed event:
        scheduler.register_timed_event(timedEvent)
        
        scheduler.run()

        assert True
