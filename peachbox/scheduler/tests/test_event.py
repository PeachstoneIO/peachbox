import unittest
import peachbox.scheduler
from pubsub import pub
import multiprocessing

class TestEvent(unittest.TestCase):
    def setUp(self):
        self.e = peachbox.scheduler.Event('e1')

    def test_init(self):
        self.assertEqual('e1', self.e.name())

    def test_status(self):
        self.e.set_status({'status':'success'})
        self.assertEqual('success', self.e.status()['status'])

class TestPeriodicEvent(unittest.TestCase):
    def test_max_publications(self):
        def receiver(param):
            self.assertEqual('p2', param['name'])

        pub.subscribe(receiver, 'p2')
        e = peachbox.scheduler.PeriodicEvent('p2', period=0.1)
        e._max_publications = 1
        e.start()
        e.process.join()

    def test_init(self):
        def receiver(param):
            print param

        p1 = peachbox.scheduler.PeriodicEvent('p1', period=3)
        pub.subscribe(receiver, p1.name())

        #p1.start()
        #p1.process.join()


class TestConditionalEvent(unittest.TestCase):
    def setUp(self):
        self.e = peachbox.scheduler.ConditionalEvent('c1')
        self.s = peachbox.scheduler.Scheduler.Instance()

    def test_init(self):
        self.assertEqual('c1', self.e.name())

    def test_subscribe(self):
        e = peachbox.scheduler.Event('simple_event')
        self.e.subscribe(e)
        self.assertEqual(self.s.get_event_status(self.e.name()), self.e.get_current_status())
        self.assertEqual(1, self.e.get_current_status()['subscriptions']['simple_event'])

    def test_track_event_single_subscription(self):
        e = peachbox.scheduler.Event('simple_event')
        self.e.subscribe(e)
        status = e.status()
        status['payload'] = {'latest_kafka_offset':123} 
        def receiver(param):
            self.assertEqual(123, param['payload']['latest_kafka_offset'])
        pub.subscribe(receiver, self.e.name())
        self.s.publish(e)

    def test_track_event_seveal_subscriptions(self):
        c  = peachbox.scheduler.ConditionalEvent('c5')
        e1 = peachbox.scheduler.Event('finished')
        status = e1.status()
        status['payload'] = {'latest_kafka_offset':123} 

        e2 = peachbox.scheduler.Event('timer')
        status2 = e2.status()
        status2['payload'] = {'timestamp':123456}

        c.subscribe(e1)
        c.subscribe(e2)

        def receiver(param):
            self.assertEqual(123, param['payload']['latest_kafka_offset'])
            self.assertEqual(123456, param['payload']['timestamp'])

        pub.subscribe(receiver, c.name())

        def publish_in_process(event):
            self.s.publish(event)

        p1 = multiprocessing.Process(target=self.s.publish, args=(e1,))
        p2 = multiprocessing.Process(target=self.s.publish, args=(e2,))
        p1.start()
        p2.start()
        p1.join()
        p2.join()

        # Check if the counters are reset
        subscriptions = c.get_subscriptions()
        self.assertEqual({'finished':1, 'timer':1}, subscriptions)

    def test_get_subscriptions(self):
        c = peachbox.scheduler.ConditionalEvent('c2')
        e1 = peachbox.scheduler.Event('e1')
        e2 = peachbox.scheduler.Event('e2')
        c.subscribe(e1)
        c.subscribe(e2)
        self.assertEqual({'e1':1, 'e2':1}, c.get_subscriptions())

    def test_sum_event_counts(self):
        self.assertEqual(2, self.e.sum_event_counts({'e1':1, 'e2':1}))

    def test_persist_status(self):
        c = peachbox.scheduler.ConditionalEvent('c3')
        c.set_status({'hello':'world'})
        c.persist_status()
        self.assertEqual(c.status(), self.s.get_event_status(c.name()))

    def test_reset_subscriptions(self):
        c = peachbox.scheduler.ConditionalEvent('c4')
        c.set_status({'subscriptions':{'e1':0, 'e2':0}})
        c.persist_status()
        self.assertEqual({'e1':1, 'e2':1}, c.reset_subscriptions())


