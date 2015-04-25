from models.reviews import Reviews
from pyspark import SparkConf, SparkContext
import peachbox
import json
from peachbox.utils.test_helper import TestHelper

def decodeAndFilter(jsonString):
    review = json.loads(jsonString)
    review = models.Review()

    review.values[review.user_id] =  review['user_id']

    review.set("user_id", review['user_id'])
    review.set('product_id', review['product_id'])
    review.set('true_as_of_seconds', review['time'])

    return {
        'user_id': review['user_id'], 
        'product_id': review['product_id'],
        'time': review['time']
    }

# Reads from a file and writes it out to corresponding folder
class ImportReviews(object):
    def __init__(self):
        self.spark = peachbox.spark.Instance()
        self.model = Reviews()

    def run(self):
        sc = self.spark.context()
        lines = sc.textFile(self.datafile)
        reviews = lines.map(decodeAndFilter)
        print "%d reviews found in file %s" % (reviews.count(), self.datafile)
        pairReviews = reviews.map(lambda review: (review['time'] / 3600, review))

        reviewsGroupedByHours = pairReviews.groupByKey()
        pairReviews.persist()

        distinctHours = reviewsGroupedByHours.keys()
        print "distinct hours: %d" % (distinctHours.count())



        for hour in distinctHours.take(10):
            reviewsOfHour = pairReviews.filter(lambda (hr, review): hr == hour).values()
            filename = "output/output_hr%d" % hour
            reviewsOfHour.saveAsTextFile(filename)
        
