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

import string
import os

class Substitutor(object):
    """Reads template files and substitutes fields."""

    def __init__(self):
        dir = os.path.dirname(os.path.realpath(__file__))
        self.path = os.path.join(dir, 'templates')
                
    def read_template(self, name):
        filename = os.path.join(self.path, name+'.tmpl')
        template = None
        with open(filename, 'r') as f:
            template = string.Template(f.read())
        return template

    def substitute(self, name, mapping):
        s = self.read_template(name)
        return s.substitute(mapping)

