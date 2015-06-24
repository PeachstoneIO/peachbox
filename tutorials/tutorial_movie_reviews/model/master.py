# Copyright 2015 Philipp Pahl, Sven Schubert, Daniel Britzger
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

"""Model definition for the master data set.
   The entities of the master data set consist of 'edges' and 'properties' and conform to the
   fact based model. <Reference>
   Every entity has a unique data_unit_index. It is horizontally partitioned wrt. the 
   partition key and a granularity.

   An edge or property must define a unique data_unit_index, the partition key and granularity. 

   The schema contains the edges and properties of an entity and the entities which are related by the edge."""

import peachbox.model

class UserReviewEdge(peachbox.model.MasterDataSet,TaskImportModel):
    """A particular realization of an 'edge'. Here: the user review edge """
    data_unit_index = 0
    partition_key = 'true_as_of_seconds'
    partition_granularity = 60*60*24*360 
    schema = [{'field':'user_id', 'type':'StringType'},
              {'field':'review_id', 'type':'StringType'}]


    def lhs_node(self, row):
        pass

    def calc_value(self,field,row):
        field = 'review_id'
        val = 4*3*row.review_id
        self.set_value(field,val)


    def import(row):
        self.lhs_node(row.user_id)
        self.rhs_node(row.review_id)
        self.partition_key(row.time)


class ProductReviewEdge(peachbox.model.MasterDataSet):
    """A particular realization of an 'edge'. Here: the product review edge """
    data_unit_index = 1
    partition_key = 'true_as_of_seconds'
    partition_granularity = 60*60*24*360 
    schema = [{'field':'review_id', 'type':'StringType'},
              {'field':'product_id', 'type':'StringType'}]

class ReviewProperties(peachbox.model.MasterDataSet):
    """A particular realization of a node, containing several properties. Here: the review properties """
    data_unit_index = 2
    partition_key = 'true_as_of_seconds'
    partition_granularity = 60*60*24*360 
    time_fill_method = fill_name('time')
    model = [{'field':'review_id', 'type':'StringType', 'fill_method': fill_review_id},
             {'field':'helpful', 'type':'IntegerType', 'fill_method': helpful},
             {'field':'nothelpful', 'type':'IntegerType', 'fill_method':fill_nothelpful},
             {'field':'score', 'type':'IntegerType'},
             {'field':'summary', 'type':'StringType'},
             {'field':'text', 'type':'StringType'}]

    source_fields = [{'field:review_id','type:StringType','validation:notempty'},
                    {'field':'text','validation:notempty'}]


    def __init__(self):
        self.build_model()

    def helpful(self, row, field=''):
        lambda row: int(row['helpfulness'].split('/')[0])

    def fill_review_id(self, row, field):
        user_id = row['user_id']
        product_id = row['product_id']
        true_as_of_seconds = row['time']
        return unicode(hash(user_id+product_id+str(true_as_of_seconds)))

    def fill_nothelpful(self, row, field):
        return int(row['helpfulness'].split('/')[1]) - fill_method['helpful'](row,'helpful')



class UserProperties(peachbox.model.MasterDataSet):
    """A particular realization of properties. Here: the user properties """
    data_unit_index = 3
    partition_key = 'true_as_seconds'
    partition_granularity = 60*60*24*360 
    schema = [{'field':'user_id', 'type':'StringType'},
              {'field':'profile_name', 'type':'StringType'}]


