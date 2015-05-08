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

import os
import re
import peachbox.gen

class Generator(object):
    """Base class of generators."""

    PROJECT_HOME = '.'

    def __init__(self):
        self.substitutor = peachbox.gen.Substitutor()

    def project_home(self):
        base = '.'
        if(self.PROJECT_HOME):
            base = os.getcwd() if (self.PROJECT_HOME == '.') else self.PROJECT_HOME 
        return base

    def write_class(self, name, module, input):
        print 'writing to: ' + self.path(name, module)
        dir = os.path.dirname(self.path(name, module))
        if os.path.exists(dir):
            with open(self.path(name, module), 'w') as f:
                f.write(input)
        else:
            raise IOError("Trying to write to non-existing path: %s" % (dir,))

    def path(self, name, module):
        filename = self.underscore(name)+'.py'
        path = os.path.join(self.project_home(), module, filename)
        return path
    
    def underscore(self, name):
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
