import unittest
import peachbox
import peachbox.utils
import peachbox.gen

class TestModel(unittest.TestCase):
    def setUp(self):
        self.output_dir = peachbox.utils.TestHelper.mkdir_tmp()
        peachbox.utils.TestHelper.mkdir(self.output_dir + '/model')
        peachbox.gen.Model.PROJECT_HOME = self.output_dir

    def test_define_node(self):
        peachbox.gen.model.define_node('MyNode', 'my_node_id', 'string')
        first_line = ''
        with open(self.output_dir+'/model/my_node.py') as f:
            first_line = f.readline()
        assert 'MyNodeId' in first_line

    def test_define_edge(self):
        peachbox.gen.model.define_node('MyNode', 'my_node_id', 'string')
        peachbox.gen.model.define_node('MyNode2', 'my_node_id2', 'string')

        peachbox.gen.model.define_edge(0, 'MyEdge', 'MyNode', 'MyNode2')
        first_line = ''
        with open(self.output_dir+'/model/my_edge.py') as f:
            first_line = f.readline()
        assert 'MyEdgeEdge' in first_line

    def test_define_property(self):
        peachbox.gen.model.define_node('MyNode', 'my_node_id', 'string')

        peachbox.gen.model.define_property(1, 'MyProperty', 'MyNode', 'my_property', 'string')
        with open(self.output_dir+'/model/my_property.py') as f:
            first_line = f.readline()
        assert 'MyPropertyProperty' in first_line

    def test_behavior_defined_models(self):
        assert False
