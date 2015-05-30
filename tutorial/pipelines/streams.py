from model.streams import Reviews as RealTimeReviews

class Reviews(object):

    def execute(self, rdd):
        return rdd.map(lambda row: self.fill_edge(row))

    def fill_edge(self, row):
        user_id = row['user_id']
        product_id = row['product_id']
        true_as_of_seconds = row['time']
        review_id = unicode(hash(user_id+product_id+str(true_as_of_seconds)))
        score = row['score']

        return RealTimeReviews.row(true_as_of_seconds=true_as_of_seconds, review_id=review_id, 
                                                product_id=product_id, score=score)                          

        


