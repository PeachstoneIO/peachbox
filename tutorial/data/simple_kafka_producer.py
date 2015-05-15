import kafka.client
import kafka.producer
import time

file = open('sorted_reviews_100000.json', 'r')

client   = kafka.client.KafkaClient('localhost:9092')
producer = kafka.producer.SimpleProducer(client)

for line in file:
    try:
        producer.send_messages('t1', line)
    except:
        print 'Something went wrong'
    time.sleep(1)
