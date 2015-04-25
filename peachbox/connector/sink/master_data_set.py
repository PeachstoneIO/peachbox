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

class MasterDataSet(object):
    """Absorbs data into the master data set"""

    def __init__(self, model=None):
        self.model = model

    def absorb(self, data):
        """Absorbs data corresponding to target of model. Takes into account vertical partitioning 
        wrt. true_as_of_seconds"""
        pass

