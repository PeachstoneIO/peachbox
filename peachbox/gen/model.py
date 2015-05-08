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

from peachbox.gen.generator import Generator
import imp 
import sys

class Model(Generator):
    """Model generator."""

    def __init__(self):
        super(Model, self).__init__()

    def write(self, name, module, template, substitutes):
        replaced = self.substitutor.substitute(module+'/'+template, substitutes)
        self.write_class(name, module, replaced)

    def load_node(self, name, module):
        module_node = imp.load_source(module, self.path(name, module))
        cls = getattr(module_node, name+'Id')
        return cls

def define_node(name, identifier, type):
    m = Model()
    substitutes = {'ClassName':name,
            'identifier':identifier,
            'type':type}
    m.write(name, 'model', 'node', substitutes)

def define_edge(data_unit_index, name, name_node1, name_node2, partition_key='true_as_of_seconds', 
        partition_granularity=3600):
    
    m = Model()
    node1 = m.load_node(name_node1, 'model')
    node2 = m.load_node(name_node2, 'model')

    substitutes = {'ClassName':name,
            'node1':node1.identifier,
            'node2':node2.identifier, 
            'data_unit_index':data_unit_index,
            'partition_key':partition_key,
            'partition_granularity':partition_granularity,
            'output_format':'peachbox.model.FileFormat.Parquet'}

    m.write(name+"Edge", 'model', 'master_data_set_edge', substitutes)

    # Existing model module might interfere with later usage of proper model module
    if sys.modules.get('model'): del sys.modules['model']

def define_property(data_unit_index, name, name_node, name_property, type):
    m = Model()
    node = m.load_node(name_node, 'model')

    substitutes = {'ClassName':name,
            'data_unit_index':data_unit_index,
            'node_identifier':node.identifier,
            'property':name_property,
            'type':type}
    m.write(name, 'model', 'property', substitutes)

    # Existing model module might interfere with later usage of proper model module
    if sys.modules.get('model'): del sys.modules['model']


    


