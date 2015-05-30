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


import peachbox
import peachbox.fs
import peachbox.utils

#import simplejson as json

class DWH(object):
    """ Interface to the data warehouse."""

    @staticmethod
    def Instance():
        """DWH is singleton. Access via Instance()"""
        if not DWH._active_instance:
            DWH._active_instance = DWH()
        return DWH._active_instance

    _active_instance = None

    def __init__(self):
        """Data warehouse constructor. Default for underlying file system is S3."""

        if DWH._active_instance:
            raise ValueError(
                "Cannot run multiple DWH instances at once."
                "Use peachbox.dwh.Instance() to access existing instance.")

        self.fs = peachbox.fs.S3()
        DWH._active_instance = self

    #TODO: Thread safety; atomic changes to the fs!
    def append(self, pail):
        mart = pail.model.mart
        path = pail.target()
        df = pail.data
        if self.fs.path_exists(mart, path):
            current_data_dir = self.fs.mv_to_tmp(mart, path)
            current_df = peachbox.Spark.Instance().sql_context().parquetFile(current_data_dir)
            df = current_df.unionAll(pail.data)
            df.saveAsParquetFile(self.fs.uri(mart, path))
            self.fs.rmtree(path=current_data_dir)
        else:
            df.saveAsParquetFile(self.fs.uri(mart, path))

    def read_data_frame(self, mart, path):
        uri = self.fs.uri(mart, path)
        return peachbox.Spark.Instance().sql_context().parquetFile(uri)

    def query_by_key_range(self, model, smallest_key, biggest_key):
        self.fs.mart = model.mart
        uris = self.fs.dirs_of_period(model.mart, model.target(), smallest_key, biggest_key)
        df = peachbox.Spark.Instance().sql_context().parquetFile(*uris) if uris else None
        return df

    #TODO: Clean up legacy code
    #
    # Writes data
    # The model defines:
    #   * mart, output path (including the index, eg. timestamp and time range)
    #   * output format, eg. JSON, parquet
    # deprecated
    #def absorb(self, model, data, pail):
        #"""Deprecated: Moved to peachbox.connector.sink
        
        #Absorbs data into the data warehouse

        #:param pail: The pail the data goes to
        #:param model: Data is stored with respect to the model properties, ie. type, id and schema
        #"""
        #mart = model.mart
        #path = os.path.join(model.path(), pail)
        #self.fs.rm_r(mart, path)
        #schema_rdd = peachbox.spark.Instance().sql_context().applySchema(data, model.schema)

        #if model.output_format == peachbox.models.FileFormat.Parquet:
            #scheam_rdd.saveAsParquetFile(self.fs.uri(mart, path))

    #def query(self, model, from_utime, before_utime):
        #pass
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

    #def read_parquet(self, model, from_date, before_date):
        #self.file_system.bucket_name = model.bucket
        #dirs = self.file_system.sub_directories_in_date_range(model.path(), from_date, before_date)
        #sources = ','.join(dirs)
        #schema_rdd = self.spark.sql_context().parquetFile(sources)
        #return schema_rdd

    def stop(self):
        DWH._active_instance = None

    # TODO: Context manager is only used for testing, move it to utils
    def __exit__(self, *err):
        self.stop()

    def __enter__(self):
        return self


