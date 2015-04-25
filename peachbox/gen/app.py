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
import subprocess
from peachbox.gen.generator import Generator

class App(Generator):
    """ Sets up a new project: Creates project folders and sample code."""

    def __init__(self):
        super(App, self).__init__()

    def create_app(self, name):
        """Create a template of the app."""
        replaced = self.substitutor.substitute('app', {'ClassName':name})
        self.write_class(name, '', replaced)

    def create_model_setup(self):
        """Create template for model setup."""
        replaced = self.substitutor.substitute('model', {})
        self.write_class('Model', 'setup', replaced)

    def create_dirs(self):
        """Create the project folders."""
        dirs = ['conf', 'setup', 'model', 'pipelines']
        for d in dirs:
            dir = os.path.join(self.project_home(), d)
            subprocess.call(['mkdir', '-p', dir])


def create_app(name, project_home='.'):
    """Create app with name in project_home. Default is current work directory."""

    App.PROJECT_HOME = project_home
    app = App()
    app.create_dirs()
    app.create_app(name)
    app.create_model_setup()
