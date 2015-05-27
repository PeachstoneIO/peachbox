from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import *

import sexmachine.detector as gender

import model.batch_views
import peachbox.task

class ReviewsByGender():
    gender_detector = gender.Detector()

    def execute(self, df):

        get_gender_udf = UserDefinedFunction(lambda name: self.get_gender(name), StringType())

        gender_column = get_gender_udf(df.profile_name)
        reviews_gender = df.withColumn('gender', gender_column)

        days_udf = UserDefinedFunction(lambda time: 60*60*24*int(time/(60*60*24)), IntegerType())
        days_column = days_udf(reviews_gender.true_as_of_seconds)
        reviews_days = reviews_gender.withColumn('day', days_column)

        reviews = reviews_days.groupBy(['day', 'gender']).agg({"*": "count"})\
                .withColumnRenamed('COUNT(1)', 'reviews')

        reviews = reviews.map(lambda row: model.batch_views.ReviewsByGender.row(true_as_of_seconds=row.day, 
                                                                                gender=row.gender, 
                                                                                review_count=row.reviews))

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

    def get_gender(self, name):
        first_name = name.split(' ')[0]
        detected_gender = ReviewsByGender.gender_detector.get_gender(first_name)
        gender = u'nn'
        if 'female' in detected_gender: gender = u'female'
        if 'male'   in detected_gender: gender = u'male'
        return gender
