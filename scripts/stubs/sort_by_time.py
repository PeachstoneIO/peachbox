import pyspark
import peachbox.pipeline
import peachbox
import ujson as json

raw = peachbox.Spark.Instance().context().textFile('movies.json')

json_parser = peachbox.pipeline.JSONParser()
validator   = peachbox.pipeline.Validator(['user_id', 'product_id', 'review', 'summary', 'profile_name', 
                                           'helpfulness', 'time', 'score'])

pipe = peachbox.pipeline.Chain([json_parser, validator])

parsed_records = pipe.execute(raw)

sorted_records = parsed_records.map(lambda j: (j['time'], j))\
        .sortByKey().map(lambda j: j[1])\
        .map(lambda row: json.dumps(row))

sorted_records.saveAsTextFile('sorted_movies')


