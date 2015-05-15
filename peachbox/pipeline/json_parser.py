import ujson as json

class JSONParser(object):
    def execute(self, rdd):
        parsed_rdd = rdd.map(lambda row: self.parse(row))
        valid_rows = parsed_rdd.filter(lambda row: row is not None)
        return valid_rows

    def parse(self, row):
        j = None
        try:
            j = json.loads(row)
        except:
            pass

        return j


