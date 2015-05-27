#import hashlib
import sexmachine.detector as gender

import model.streams 

class ReviewByGender(object):
    gender_detector = gender.Detector()

    def execute(self, rdd):
        return rdd.map(lambda row: self.fill_edge(row))

    def fill_edge(self, row):
        user_id = row['user_id']
#        product_id = row['product_id']
        true_as_of_seconds = row['time']
#        review_id = unicode(hashlib.md5(user_id+product_id+str(true_as_of_seconds)).hexdigest())
        first_name = row['profile_name'].split(' ')[0]
        gender = self.get_gender(first_name)

        return model.streams.ReviewByGender.row(true_as_of_seconds=true_as_of_seconds, gender=gender)
            #review_id=review_id, true_as_of_seconds=true_as_of_seconds)

    def get_gender(self, name):
        first_name = name.split(' ')[0]
        detected_gender = self.gender_detector.get_gender(first_name)
        gender = u'nn'
        if 'female' in detected_gender: gender = u'female'
        if 'male'   in detected_gender: gender = u'male'
        return gender
        


