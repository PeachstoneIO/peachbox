import model.master

class UserReviewEdge(object):
    def execute(self, rdd):
        return rdd.map(lambda row: self.fill_edge(row))

    def fill_edge(self, row):
        user_id = row['user_id']
        product_id = row['product_id']
        true_as_of_seconds = row['time']
        review_id = unicode(hash(user_id+product_id+str(true_as_of_seconds)))

        return model.master.UserReviewEdge.spark_row(user_id=user_id, 
            review_id=review_id, true_as_of_seconds=true_as_of_seconds)

class ProductReviewEdge(object):
    def execute(self, rdd):
        return rdd.map(lambda row: self.fill_edge(row))

    def fill_edge(self, row):
        user_id = row['user_id']
        product_id = row['product_id']
        true_as_of_seconds = row['time']
        review_id = unicode(hash(user_id+product_id+str(true_as_of_seconds)))

        return model.master.ProductReviewEdge.spark_row(product_id=product_id, 
            review_id=review_id, true_as_of_seconds=true_as_of_seconds)



class ReviewProperties(object):
    def execute(self, rdd):
        return rdd.map(lambda row: self.fill_properties(row))

    def fill_properties(self, row):
        true_as_of_seconds = row['time']
        user_id = row['user_id']
        product_id = row['product_id']
        review_id = unicode(hash(user_id+product_id+str(true_as_of_seconds)))
        helpful    = int(row['helpfulness'].split('/')[0])
        nothelpful = int(row['helpfulness'].split('/')[1]) - helpful

        score = int(row['score'])
        summary = row['summary']
        text = row['review']

        return model.master.ReviewProperties.spark_row(review_id=review_id, score=score, 
                summary=summary, text=text, helpful=helpful, nothelpful=nothelpful, 
                true_as_of_seconds=true_as_of_seconds)



