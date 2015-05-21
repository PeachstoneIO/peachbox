import sexmachine.detector as gender

class ReviewByGender(object):
    def __init__(self):
        self.gender_detector = gender.Detector(unknown_value=u'nn') 

    def execute(self, df):
        df_with_gender_field =  df.map(lambda row: self.fill_row(row))
        return df_with_gender_field

    def fill_row(self, row):
        user_id = row.user_id
        profile_name = row.profile_name
        first_name = profile_name.split(' ')[0]
        true_as_of_seconds = row.true_as_of_seconds
        gender = self.get_gender(first_name)

        return (user_id, gender, true_as_of_seconds)

    def get_gender(self, name):
        detected_gender = self.gender_detector.get_gender(name)
        gender = u'nn'
        if 'female' in detected_gender:
            gender = u'female'
        elif 'male' in detected_gender:
            gender = u'male'
        return gender
        


