import unittest
import peachbox.utils

class TestSubstitutor(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = peachbox.utils.TestHelper.mkdir_tmp()
        peachbox.utils.TestHelper.mkdir(self.tmp_dir + '/templates')

        self.substitutor      = peachbox.gen.Substitutor()
        self.substitutor.path = self.tmp_dir + '/templates'

        self.input = "Hello $Berlin"
        peachbox.utils.TestHelper.write_string(self.tmp_dir + '/templates/app.tmpl', self.input)

    def test_read_template(self):
        self.assertEqual('Hello $Berlin', self.substitutor.read_template('app').safe_substitute())

    def test_substitute(self):
        self.assertEqual('Hello World', self.substitutor.substitute('app', {'Berlin':'World'}))



