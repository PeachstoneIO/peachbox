import ujson as json
from peachbox.task import Task

"""Copyright 2015 D. Britzger, P. Pahl, S. Schubert

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

class ImportProperties(Task):
    """
    Import attributes to define an edge.
    An edge (or also called `relationship`) relates two nodes (also called entity) with each other.
    Abstract class. Edged must implement functions: `rh_side`, `lh_side` and `partition_key`.
    """
    

    def __init__(self):
        """Model defines schema of edge and must inhert from `peachbox.model.MasterDataSet`."""
        self.ms = None

    def set_master_schema(self, master_schema):
        self.ms = master_schema

    def execute(self, rdd):
        if not self.ms:
            raise AttributeError
        return self.ms.fill_properties(rdd)
#        return rdd.map(lambda row: self.fill_properties(row))

