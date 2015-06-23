import unittest
import peachbox.scheduler
import pubsub.pub
import multiprocessing
import peachbox.task

class MockTask(peachbox.task.ScheduledTask):
    def run_scheduled_task(self, param):
        pass

class TestScheduler(unittest.TestCase):
    def setUp(self):
        self.s = peachbox.scheduler.Scheduler.Instance()

    def test_publish(self):

        def receiver(param):
            self.assertEqual('world', param['hello'])
        
        def sender():
            pubsub.pub.sendMessage('EventName', param={'hello':'world'})

        pubsub.pub.subscribe(receiver, 'EventName')
        p = multiprocessing.Process(target=sender)
        p.start()
        p.join()

    def test_get_default_event_status(self):
        self.assertEqual({}, self.s.get_event_status('e1'))

    def test_get_event_status(self):
        self.s._event_status['e1'] = {'k':'v'}
        self.assertEqual({'k':'v'}, self.s.get_event_status('e1'))

    def test_set_event_status(self):
        self.s.set_event_status('e1', {'k':'v'})
        self.assertEqual({'k':'v'}, self.s.get_event_status('e1'))

    def test_get_event_status_from_process(self):
        def sender(scheduler):
            scheduler.set_event_status('e1', {'k':'process_v'})

        p = multiprocessing.Process(target=sender, args=(self.s,))
        p.start()
        p.join()
        self.assertEqual(self.s.get_event_status('e1'), {'k':'process_v'})

    def test_get_event_status_in_process(self):
        def sender(scheduler):
            scheduler.set_event_status('e1', {'k':'process_v'})

        p = multiprocessing.Process(target=sender, args=(self.s,))
        p.start()
        p.join()

        def consumer(scheduler):
            self.assertEqual(scheduler.get_event_status('e1'), {'k':'process_v'})

        p2 = multiprocessing.Process(target=consumer, args=(self.s,))
        p2.start()
        p2.join()

    def test_publish(self):
        def receiver(param):
            self.assertEqual({'name':'e1', 'status':'success'}, param)

        pubsub.pub.subscribe(receiver, 'e1')

        e = peachbox.scheduler.Event('e1')
        e.set_status({'status':'success'})
        self.s.publish(e)

    def test_publish_from_process(self):
        def receiver(param):
            self.assertEqual({'status':'success', 'name':'e1'}, param)
        pubsub.pub.subscribe(receiver, 'e1')
        def sender(scheduler):
            e = peachbox.scheduler.Event('e1')
            e.set_status({'status':'success'})
            scheduler.publish(e)
        p = multiprocessing.Process(target=sender, args=(self.s,))
        p.start()
        p.join()


    def test_publish_from_process_with_former_status(self):
        def receiver(param):
            self.assertEqual('success', param['status'])
            self.assertEqual('info', param['old'])
        pubsub.pub.subscribe(receiver, 'e1')

        self.s.set_event_status('e1', {'old':'info'})
        
        def sender(scheduler):
            e = peachbox.scheduler.Event('e1')
            status = scheduler.get_event_status(e.name())
            status.update({'status':'success'})
            e.set_status(status)

            scheduler.publish(e)

        p = multiprocessing.Process(target=sender, args=(self.s,))
        p.start()
        p.join()

    def test_publish_conditional_event(self):
        e1 = peachbox.scheduler.Event('e1')
        e2 = peachbox.scheduler.Event('e2')

        c = peachbox.scheduler.ConditionalEvent('c_event')
        c.subscribe(e1)
        c.subscribe(e2)

        def receiver(param):
            self.assertEqual(123, param['payload']['latest_kafka_offset'])
            self.assertEqual(123456, param['payload']['timestamp'])

        e1.set_status({'payload':{'latest_kafka_offset':123}})
        e2.set_status({'payload':{'timestamp':123456}})

        pubsub.pub.subscribe(receiver, 'c_event')

        self.s.publish(e1)
        self.s.publish(e2)

    def test_subscribe_event_registration(self):
        t = peachbox.task.ScheduledTask()
        self.s.subscribe(t, peachbox.scheduler.Event('e1'))
        self.s.subscribe(t, peachbox.scheduler.Event('e2'))
        self.s.subscribe(t, peachbox.scheduler.PeriodicEvent('p1', 2))
        assert set(['e1','e2']).issubset(set([e.name() for e in self.s._events]))
        assert set(['p1']).issubset(set([e.name() for e in self.s._periodic_events]))

    def test_run(self):
        t = MockTask()

        periodic_event = peachbox.scheduler.PeriodicEvent('test_run_p1', 0.1)
        periodic_event._max_publications = 2
        
        test_run_e1 = peachbox.scheduler.Event('test_run_e1')
        test_run_e2 = peachbox.scheduler.Event('test_run_e2')
        conditional_event = peachbox.scheduler.ConditionalEvent('test_run_c1')

        conditional_event.subscribe(test_run_e1)
        conditional_event.subscribe(periodic_event)

        self.s.subscribe(t, test_run_e1) 
        self.s.subscribe(t, test_run_e2) 
        self.s.subscribe(t, periodic_event) 
        self.s.subscribe(t, conditional_event) 

        def receive_e1(param):
            self.assertEqual('test_run_e1', param['name'])
            
        def receive_e2(param):
            self.assertEqual('test_run_e2', param['name'])

        def receive_p1(param):
            self.assertEqual('test_run_p1', param['name'])

        def receive_c1(param):
            self.assertEqual('test_run_c1', param['name'])

        pubsub.pub.subscribe(receive_c1, 'test_run_c1')
        pubsub.pub.subscribe(receive_e1, 'test_run_e1')
        pubsub.pub.subscribe(receive_e2, 'test_run_e2')
        pubsub.pub.subscribe(receive_p1, 'test_run_p1')

        self.s.run()
        self.s._conditional_events[0]._conditions[1].process.join()
        
    def test_run_conditional_events(self):
        t = MockTask()

        periodic_event = peachbox.scheduler.PeriodicEvent('test_run2_p1', 0.1)
        periodic_event._max_publications = 1
        
        test_run_e1 = peachbox.scheduler.Event('test_run2_e1')
        conditional_event = peachbox.scheduler.ConditionalEvent('test_run2_c1')

        conditional_event.subscribe(test_run_e1)
        conditional_event.subscribe(periodic_event)

        self.s.subscribe(t, conditional_event) 

        def receive_e1(param):
            assert False
            
        def receive_p1(param):
            self.assertEqual('test_run2_p1', param['name'])

        def receive_c1(param):
            self.assertEqual('test_run2_c1', param['name'])

        pubsub.pub.subscribe(receive_c1, 'test_run2_c1')
        pubsub.pub.subscribe(receive_e1, 'test_run2_e1')
        pubsub.pub.subscribe(receive_p1, 'test_run2_p1')

        self.s.run()
        self.s._conditional_events[0]._conditions[1].process.join()
