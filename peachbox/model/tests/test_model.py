import unittest
import peachbox
import peachbox.spark
import pickle

class TestModel(unittest.TestCase):
    pass
    #def test_create_node(self):
        #n = peachbox.models.models.create_node(0, 'UserId', 'user_id', 'string')
        #n.set_user_id('4567ab87cfe9')
        #n.set_true_as_of_seconds(1)
        
        #print 'created new instance'
        #n2 = peachbox.models.models.node_instance("UserId")
        #n2.set_user_id('2nd user')
        #n2.set_true_as_of_seconds(2)

        #n3 = peachbox.models.models.node_instance("UserId")
        #n3.set_true_as_of_seconds(3)

        #print n.true_as_of_seconds
        #print n2.true_as_of_seconds
        #print n3.true_as_of_seconds

        #self.assertNotEqual(n2.value, n.value)

    #def test_create_property(self):
        #n = peachbox.models.models.create_node(0, 'UserId', 'user_id', 'string')
        #p = peachbox.models.models.create_property(0, n, 'Helpful', 'helpful', 'int')

        #p.set_user_id('345678af65')
        #p.set_helpful(3)

    #def test_class_creation(self):

        #peachbox.models.models.create_node2(0, 'UserId', 'user_id')

        #n = peachbox.models.models.UserId(user_id='user2')
        ##globals()['UserId'] = peachbox.models.models.UserId
        ##n = (globals()['UserId'])(user_id='user3')
        ##global UserId
        #c = peachbox.Spark().context()
        #rdd = c.parallelize([n])
        #print rdd.map(lambda node: node.user_id).collect()
        #print pickle.dumps(n)

        #python -m peachbox "peachbox.generator.start_project("MovieReviews")"

        #assert False



