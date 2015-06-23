import ujson as json
from peachbox.task import Task

"""
Copyright 2015 D. Britzger, P. Pahl, S. Schubert

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

class TaskImportEdge(Task):
    """
    Import attributes to define an edge.
    An edge (or also called `relationship`) relates two nodes (also called entity) with each other.
    Abstract class. Edged must implement functions: `rh_side`, `lh_side` and `partition_key`.
    """

    def __init__(self, model):
        """Model defines schema of edge and must inhert from `peachbox.model.MasterDataSet`."""
        self.model = model

    def execute(self, rdd):
        return rdd.map(lambda row: self.fill_edge(row))

    def rhs_node(self, row):
        raise NotImplementedError

    def lhs_node(self, row):
        raise NotImplementedError

    def partition_key(self, row):
        raise NotImplementedError

    def fill_edge(self, row):
        lhs = self.lhs_node(row)
        rhs = self.rhs_node(row)        
        prt = self.partition_key(row)
        return self.model.spark_row(model.schema.field[1]=lhs,rhs=rhs,prt=prt)

