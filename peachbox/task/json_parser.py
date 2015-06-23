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

class JSONParser(Task):
    """Parse JSON from rdd in line format"""

    def execute(self, rdd):
        parsed_rdd = rdd.map(lambda row: self.parse(row))
        valid_rows = parsed_rdd.filter(lambda row: row is not None)
        return valid_rows

    def parse(self, row):
        j = None
        try:
            j = json.loads(row)
        except:
            pass
        return j


