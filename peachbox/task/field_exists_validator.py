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

class FieldExistsValidator(Task):
    """ Peachbox validator.
    Checks if entries of 'fields' exists in entries of rdd's and drops the entry if one or more fields are not existing.
    """
    def __init__(self, fields):
        super(FieldExistsValidator, self).__init__()
        self.fields = fields

    def execute(self, rdd):
        return rdd.filter(lambda row: self.validate(row))

    def validate(self, entry):
        return all (k in entry for k in self.fields)
