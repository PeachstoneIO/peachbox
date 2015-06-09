/usr/local/cassandra/bin/cassandra &
/usr/local/kafka/bin/zookeeper-server-start.sh /usr/local/kafka/config/zookeeper.properties &
sleep 10 
/usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties &

