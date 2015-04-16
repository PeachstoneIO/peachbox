# Copyright 2015 Philipp Pahl 
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.
"""
Data warehouse
"""

import peachbox
import peachbox.fs

import simplejson as json

class DWH(object):
    def __init__(self, fs):
        self.fs    = fs

    def query(self, model, from_utime, before_utime):
        pass
        #self.fs.mart = model.mart
        #sources = ','.join(self.fs.dirs_of_period(model, from, before))
        #rdd     = self.retrieve(sources) 
        #return self.load_json(rdd)

    ## TODO: Handle target times properly
    #def write(self, model, rdd, seconds=0):
        #self.fs.mart = model.mart
        #path = model.path() + '/' + str(seconds)
        #self.fs.rm_fr(path)
        #json_rdd = self.dump_json(rdd)
        #json_rdd.saveAsTextFile(self.fs.url_prefix() + path)

    #def retrieve(self, sources):
        #return self.spark.context().textFile(sources)

    #def load_json(self, rdd)
        #return rdd.map(lambda line: json.loads(line))

    #def dump_json(self, rdd):
        #return rdd.map(lambda line: json.dumps(line))


    ##U##

    #def get_rdd(self, model, from_date, before_date):
        #self.file_system.bucket_name = model.bucket
        #dirs = self.file_system.sub_directories_in_date_range(model.path(), from_date, before_date)
        #sources = ','.join(dirs)
        #rdd = self.spark.context().textFile(sources).map(lambda line: json.loads(line))
        #return rdd

    #def write(self, model=None, data=None, seconds=0):
        #data = data.map(lambda entry: json.dumps(entry))
        #self.file_system.bucket_name = model.bucket 
        #path = model.path() + '/' + str(seconds)
        #self.file_system.remove_directory(path)
        #data.saveAsTextFile(self.file_system.url_prefix() + path)

    #def write_parquet(self, model=None, data=None, seconds=0):
        #self.file_system.bucket_name = model.bucket 
        #path = model.path() + '/' + str(seconds)
        #self.file_system.remove_directory(path)
        #schema_rdd = self.spark.sql_context().applySchema(data, model.schema)
        #print 'saving parquet file: ' + self.file_system.url_prefix() + path
        #schema_rdd.saveAsParquetFile(self.file_system.url_prefix() + path)

    #def read_parquet(self, model, from_date, before_date):
        #self.file_system.bucket_name = model.bucket
        #dirs = self.file_system.sub_directories_in_date_range(model.path(), from_date, before_date)
        #sources = ','.join(dirs)
        #schema_rdd = self.spark.sql_context().parquetFile(sources)
        #return schema_rdd



