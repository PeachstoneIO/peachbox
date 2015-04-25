import unittest
import peachbox.gen

class TestGenerator(unittest.TestCase):
    def setUp(self):
        self.g = peachbox.gen.Generator()

    def test_project_home_default(self):
        assert False

    def test_project_home_adapted(self):
        assert False

    def test_write_class(self):
        assert False

    def test_path(self):
        assert False

    def test_underscore(self):
        self.assertEqual('hello_world', self.g.underscore('HelloWorld'))
        self.assertEqual('hello_world', self.g.underscore('helloWorld'))
        self.assertEqual('helloworld', self.g.underscore('helloworld'))

