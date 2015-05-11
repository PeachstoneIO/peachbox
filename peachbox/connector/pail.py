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

import os.path

class Pail(object):
    """Pail for data. Contains data, a model and vertical partition key to determine where to store it."""

    @staticmethod
    def create_pails(data, model):
        """Creates a list of pails for given data set and model.
           The data is partitioned with respect to the partition_key and granularity as defined 
           in the model."""

        partition_key         = model.partition_key
        partition_granularity = model.partition_granularity
        keys =  (data.map(lambda d: getattr(d, partition_key) / int(partition_granularity))
                .distinct().collect())
        
        pails = []
        for key in keys:
            p = Pail()
            p.partition_key = str((key+1) * model.partition_granularity)
            p.data = (data.where(partition_key + '>' + str(key * partition_granularity))
                          .where(partition_key + '<' + str((key+1) * partition_granularity)))
            p.model = model
            pails.append(p)
        return pails

    def __init__(self):
        self.partition_key = None
        self.model         = None
        self.data          = None

    def target(self):
        return os.path.join(self.model().target(), self.partition_key)

    def _key(self, entry, partition_key, partition_granularity):
        return getattr(entry, partition_key) / partition_granularity

    

