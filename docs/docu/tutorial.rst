.. peachbox documentation master file, created by
   sphinx-quickstart on Fri Apr 17 13:18:43 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Peachbox' examples
******************

.. toctree::
   :maxdepth: 3
   :numbered:

..   tutorial


Tutorial: Movie reviews
=======================
Start docker environment from the peachbox main directory::

  $ ./docker/run_docker.sh

This runs a docker session with ``$ docker run -i -p 8888:8888 -v $PWD:/peachbox -t peachstone/dev /bin/bash`` and opens a bash terminal.

Within the docker terminal, start the `cassandra`, `zookeeper` and `kafka` servers::
  
  $ /usr/local/cassandra/bin/cassandra
  $ /usr/local/kafka/bin/zookeeper-server-start.sh $PEACHBOX/services/zookeeper.properties &
  $ /usr/local/kafka/bin/kafka-server-start.sh $PEACHBOX/services/server.properties &

These commands are also available as ``$ source $PEACHBOX/tutorial/start_services.sh``. 

It may be convenient to have multiple shells available within docker. Thus run::

  $ tmux

When typing ``Ctrl+b c``, a new `tab` is opened. Change between tabs with ``Ctrl+b n``.
The bottom line indicates which shell is activated.


We now want to emulate a continous data flow, where two reviews are submitted (to kafka) per second.
The data of the movie reviews is stored in ``data/sorted_reviews_100000.json``. Type::
  
  $ cd data
  $ ./simple_kafka_producer.py

This small script then reads the reviews and passes two reviews to kafka every second.




