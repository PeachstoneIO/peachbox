/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic movie_reviews;
/usr/local/kafka/bin/kafka-server-stop.sh;
/usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties &
