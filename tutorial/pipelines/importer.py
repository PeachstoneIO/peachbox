import hashlib

from model.master import ReviewByUserEdge

class ReviewByUser():
    def execute(self, rdd):
        return rdd.map(lambda row: self.fill_edge(row))

    def fill_edge(self, row):
        user_id = row['user_id']
        product_id = row['product_id']
        true_as_of_seconds = row['time']
        review_id = unicode(hashlib.md5(user_id+product_id+str(true_as_of_seconds)).hexdigest())

        return ReviewByUserEdge.spark_row(user_id=user_id, 
            review_id=review_id, true_as_of_seconds=true_as_of_seconds)


