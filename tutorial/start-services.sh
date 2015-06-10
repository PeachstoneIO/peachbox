<<<<<<< HEAD
#!/bin/bash
/usr/local/cassandra/bin/cassandra

=======
/usr/local/cassandra/bin/cassandra &
>>>>>>> 1d0a4a3e4fabaa6c59ce9c9d5c0ebc87babd3f30
/usr/local/kafka/bin/zookeeper-server-start.sh /usr/local/kafka/config/zookeeper.properties &
sleep 10 
/usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties &


