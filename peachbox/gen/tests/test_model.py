import os.path
import sys

import unittest
import peachbox
import peachbox.utils
import peachbox.gen

class TestModel(unittest.TestCase):
    def setUp(self):
        self.project_home = peachbox.utils.TestHelper.mkdir_tmp()
        peachbox.gen.app.create_app('MyApp', self.project_home)
        peachbox.gen.Model.PROJECT_HOME = self.project_home
        peachbox.gen.model.define_node('MyNode', 'my_node_id', 'string')
        peachbox.gen.model.define_node('MyNode2', 'my_node_id2', 'string')
        peachbox.gen.model.define_edge(0, 'MyEdge', 'MyNode', 'MyNode2')
        peachbox.gen.model.define_edge(1, name='MyEdge2', name_node1='MyNode', name_node2='MyNode2',
                partition_key='my_index', partition_granularity=24*3600)

        sys.path.append(self.project_home)

    def test_define_node(self):
        peachbox.gen.model.define_node('MyNode', 'my_node_id', 'string')
        first_line = ''
        with open(self.output_dir+'/model/my_node.py') as f:
            first_line = f.readline()
        assert 'MyNodeId' in first_line

    def test_edge_creation(self):
        assert os.path.exists(self.output_dir+'/model/my_edge_edge.py')

    def test_define_property(self):
        peachbox.gen.model.define_node('MyNode', 'my_node_id', 'string')

        peachbox.gen.model.define_property(1, 'MyProperty', 'MyNode', 'my_property', 'string')
        with open(self.output_dir+'/model/my_property.py') as f:
            first_line = f.readline()
        assert 'MyPropertyProperty' in first_line

    def test_edge_defaults(self):
        from model.my_edge_edge import MyEdgeEdge
        edge = MyEdgeEdge()
        self.assertEqual(0, edge.data_unit_index)
        self.assertEqual('true_as_of_seconds', edge.partition_key)
        self.assertEqual(3600, edge.partition_granularity)
        self.assertEqual(peachbox.model.FileFormat.Parquet, edge.output_format)

    def test_edge(self):
        from model.my_edge2_edge import MyEdge2Edge
        edge = MyEdge2Edge()
        self.assertEqual(1, edge.data_unit_index)
        self.assertEqual('my_index', edge.partition_key)
        self.assertEqual(24*3600, edge.partition_granularity)


