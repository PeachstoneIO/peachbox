from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import *

import peachbox.task
import model.batch_views
from model.batch_views import Reviews as BatchViewReviews

class Reviews():

    def execute(self, df):

#        get_gender_udf = UserDefinedFunction(lambda name: self.get_gender(name), StringType())
#
#        gender_column = get_gender_udf(df.profile_name)
#        reviews_gender = df.withColumn('gender', gender_column)
#
        hours_udf = UserDefinedFunction(lambda time: 60*60*int(time/(60*60)), IntegerType())
        hours_column = hours_udf(df.true_as_of_seconds)
        reviews_days = df.withColumn('hour', hours_column)
        reviews = reviews_days.groupBy(['hour', 'review_id', 'product_id', 'score']).agg({"*": "count"})\
                .withColumnRenamed('COUNT(1)', 'review_count')

#        reviews = reviews_days.groupBy(['day', 'gender']).agg({"*": "count"})\
#                .withColumnRenamed('COUNT(1)', 'reviews')
#
        
        reviews = reviews.map(lambda entry: BatchViewReviews.row(true_as_of_seconds=entry.hour, 
                                                      review_count=entry.review_count, 
                                                      review_id=entry.review_id,
                                                      product_id=entry.product_id,
                                                      score=entry.score))

        return reviews


        # return reviews['true_as_of_seconds', 'gender', 'count']

        # df_with_gender_field =  df.map(lambda row: self.fill_row(row))
        # return df_with_gender_field

    #def fill_row(self, row):
    #    user_id = row.user_id
    #    profile_name = row.profile_name
    #    first_name = profile_name.split(' ')[0]
    #    true_as_of_seconds = row.true_as_of_seconds
    #    gender = self.get_gender(first_name)
    #    return (user_id, gender, true_as_of_seconds)

