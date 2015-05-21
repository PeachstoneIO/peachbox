import hashlib

from model.master import ReviewByUserEdge, UserProperties
import model.master

class ReviewByUser(object):
    def execute(self, rdd):
        return rdd.map(lambda row: self.fill_edge(row))

    def fill_edge(self, row):
        user_id = row['user_id']
        product_id = row['product_id']
        true_as_of_seconds = row['time']
        review_id = unicode(hashlib.md5(user_id+product_id+str(true_as_of_seconds)).hexdigest())

        return ReviewByUserEdge.spark_row(user_id=user_id, 
            review_id=review_id, true_as_of_seconds=true_as_of_seconds)


class UserProperties(object):
    def execute(self, rdd):
        return rdd.map(lambda row: self.fill_properties(row))

    def fill_properties(self, row):
        user_id = row['user_id']
        true_as_of_seconds = row['time']
        profile_name = row['profile_name']
        score = int(row['score'])
        return model.master.UserProperties.spark_row(user_id=user_id, profile_name=profile_name, 
                score=score, true_as_of_seconds=true_as_of_seconds)



