# Copyright 2015 Philipp Pahl, Sven Schubert
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
import peachbox.gen
import os
import peachbox.utils

class TestGenerator(unittest.TestCase):
    def setUp(self):
        self.g = peachbox.gen.Generator()

    def test_project_home_default(self):
        self.assertEqual(os.getcwd(), self.g.project_home())

    def test_project_home_manual(self):
        self.g.PROJECT_HOME = '/tmp/my_dir'
        self.assertEqual('/tmp/my_dir', self.g.project_home())

    def test_write_class(self):
        dir = peachbox.utils.TestHelper().mkdir_tmp()
        peachbox.utils.TestHelper().mkdir_p(dir + '/mod')
        self.g.PROJECT_HOME = dir
        self.g.write_class('MyClass', 'mod', 'Hello World')
        with open(self.g.path('MyClass', 'mod'), 'r') as f:
            self.assertEqual('Hello World', f.readline())

    def test_write_class_not_existing_path(self):
        dir = peachbox.utils.TestHelper().mkdir_tmp()
        self.g.PROJECT_HOME = dir
        with self.assertRaises(IOError):
            self.g.write_class('MyClass', 'mod', 'Hello World')

    def test_path(self):
        class_name = "MyClass"
        module = 'model'
        self.g.PROJECT_HOME = '/tmp/my_dir'
        self.assertEqual('/tmp/my_dir/model/my_class.py', self.g.path(class_name, module))

    def test_underscore(self):
        self.assertEqual('hello_world', self.g.underscore('HelloWorld'))
        self.assertEqual('hello_world', self.g.underscore('helloWorld'))
        self.assertEqual('helloworld', self.g.underscore('helloworld'))

