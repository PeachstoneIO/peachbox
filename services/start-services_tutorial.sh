#!/bin/bash
/usr/local/cassandra/bin/cassandra &
/usr/local/kafka/bin/zookeeper-server-start.sh $PEACHBOX/zookeeper.properties &
sleep 10 
/usr/local/kafka/bin/kafka-server-start.sh $PEACHBOX/server.properties &
echo "Cassandra, zookeeper and kafka loaded."
