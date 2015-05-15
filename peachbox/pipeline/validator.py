class Validator(object):
    def __init__(self, fields):
        self.fields = fields

    def execute(self, rdd):
        return rdd.filter(lambda row: self.validate(row))

    def validate(self, entry):
        return all (k in entry for k in self.fields)
