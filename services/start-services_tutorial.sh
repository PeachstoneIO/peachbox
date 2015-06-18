#!/bin/bash
/usr/local/cassandra/bin/cassandra &
/usr/local/kafka/bin/zookeeper-server-start.sh $PEACHBOX/services/zookeeper.properties &
sleep 10 
/usr/local/kafka/bin/kafka-server-start.sh $PEACHBOX/services/server.properties &
echo "Cassandra, zookeeper and kafka loaded."
