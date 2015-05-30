import kafka.client
import kafka.producer
import time

file = open('sorted_reviews_100000.json', 'r')

client   = kafka.client.KafkaClient('localhost:9092')
producer = kafka.producer.SimpleProducer(client)

counter = 0 
timestamp = time.time() 

for line in file:
    try:
        producer.send_messages('movie_reviews', line)
        if (counter % 1000)==0: 
            now = time.time()
            print str(counter)+' events sent in '+str(now-timestamp)+' seconds: '+str(1000.0/(now-timestamp))+' evts/sec'
            timestamp = now
        counter +=1
    except:
        print 'Something went wrong'
    time.sleep(0.5)
