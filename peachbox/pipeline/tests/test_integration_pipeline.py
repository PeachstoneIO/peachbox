#import unittest

#import peachbox.pipeline
#import peachbox.connector.source
#import peachbox.connector.sink
#import peachbox.model
#import peachbox.utils
#import peachbox.gen

#import pyspark.sql
#from pyspark.sql.types import *

#import imp
#import sys


## Create app and  model 
#project_home = peachbox.utils.TestHelper().mkdir_tmp()
#peachbox.gen.Generator.PROJECT_HOME = project_home
##peachbox.gen.app.create_app("MovieReviews", project_home)
##peachbox.gen.model.define_node("Customer", "customer_id", "string")
##peachbox.gen.model.define_node("Review", "review_id", "string")
##peachbox.gen.model.define_edge(0, "Review", "Customer", "Review")
##sys.path.append(project_home)

##from model.review_edge import ReviewEdge

#from peachbox.model import MasterData



#class ImportReviews(peachbox.task.Import):


    ### optional: is called by execute
    #def tear_down(self):
        #pass


#class TestIntegrationPipeline(unittest.TestCase):
    #def test_fail(self):
        #assert False



