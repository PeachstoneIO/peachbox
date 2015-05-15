# Run inside ipython: %run -i kafka_simple_producer.py

import kafka.client
import kafka.producer
import kafka.common
import os


host_ip = os.environ.get('HOST_IP') or 'localhost'
client   = kafka.client.KafkaClient(host_ip + ':9092')
producer = kafka.producer.SimpleProducer(client)

#producer.send_messages('topic', 'My message')

