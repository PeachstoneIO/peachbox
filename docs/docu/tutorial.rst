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

Peachbox setup
++++++++++++++
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


Emulate data stream
+++++++++++++++++++
We now want to emulate a continous data flow, where two reviews are submitted (to kafka) per second.
First go to the tutorial directory::

  $ cd $PEACHBOX/tutorials/tutorial_movie_reviews/

The data of the movie reviews is stored in ``data/sorted_reviews_100000.json`` and a simple script reads the data file and passes two reviews per second to kafka. Type::
  
  $ cd data
  $ ./simple_kafka_producer.py

The ``simple_kafka_producer.py`` script passes each line of the input file (in ``.json``-format) to kafka. kafka buffers the input data in a durable and fault-tolerant way. 

The input data events are of the form::

  {"user_id":"A37I5QIHD9UMPD","product_id":"6302967538","review":"&quot;The Cruel Sea&quot; gives an excellent account of the real war at sea, the everyday lives of sailors and the situations they were up against both at sea and at home.  The reader feels the fear, the anguish, the camaradarie of the crew.<br \/><p> There is no glamour in war, there is the ordinary man doing his best to win the battle and when it is won, to go home to continue with his life","summary":"The everyday man at war","profile_name":"dmunns@yancey.main.nc.us","helpfulness":"2\/2","time":872035200,"score":5.0}
  {"user_id":"A37I5QIHD9UMPD","product_id":"B00004CILW","review":"&quot;The Cruel Sea&quot; gives an excellent account of the real war at sea, the everyday lives of sailors and the situations they were up against both at sea and at home.  The reader feels the fear, the anguish, the camaradarie of the crew.<br \/><p> There is no glamour in war, there is the ordinary man doing his best to win the battle and when it is won, to go home to continue with his life","summary":"The everyday man at war","profile_name":"dmunns@yancey.main.nc.us","helpfulness":"2\/2","time":872035200,"score":5.0}
  {"user_id":"A37I5QIHD9UMPD","product_id":"B00008V6YR","review":"&quot;The Cruel Sea&quot; gives an excellent account of the real war at sea, the everyday lives of sailors and the situations they were up against both at sea and at home.  The reader feels the fear, the anguish, the camaradarie of the crew.<br \/><p> There is no glamour in war, there is the ordinary man doing his best to win the battle and when it is won, to go home to continue with his life","summary":"The everyday man at war","profile_name":"dmunns@yancey.main.nc.us","helpfulness":"2\/2","time":872035200,"score":5.0}
  {"user_id":"A37I5QIHD9UMPD","product_id":"6302763770","review":"&quot;The Cruel Sea&quot; gives an excellent account of the real war at sea, the everyday lives of sailors and the situations they were up against both at sea and at home.  The reader feels the fear, the anguish, the camaradarie of the crew.<br \/><p> There is no glamour in war, there is the ordinary man doing his best to win the battle and when it is won, to go home to continue with his life","summary":"The everyday man at war","profile_name":"dmunns@yancey.main.nc.us","helpfulness":"2\/2","time":872035200,"score":5.0}
  {"user_id":"A2XBTS97FERY2Q","product_id":"B004J1A72C","review":"This is a wide ranging musical comedy done in the style of post-depression era musicals, including brilliant performances from Steve Martin, Bernadette Peters, and Christopher Walken.  One might take the movie literally as a love story, but upon further consideration we see that it's actually a movie dedicated to the upbeat songs and movies of days long gone by.  I recommend movie highly as it is sometimes boisterous, and other times delicately romantic, but all the while very entertaining.  @see-also &quot;Radio Days&quot; END","summary":"A clever take on an old genre","profile_name":"ron@6dos.com","helpfulness":"6\/7","time":872294400,"score":5.0}

  

Define the master model
+++++++++++++++++++++++
The data stream arrives in a particular stream-schema, where each event/entity has its particular attributes.
Now we want to normalize the stream-schema to avoid reduncancy and allow for extensibility.
Conveniently the entity-relantionship model is applied and the `master model` has to be defined.
This means, that all relevant tables (relationships[edges] and entities[nodes]) have to be defined.

Each `entity`(=node) and `relationship`(=edge) must inhert from ``peachbox.model.MasterDataSet``


Entities (or nodes) containing the attributes (properties)
----------------------------------------------------------
An entity(=node) is represented within peachbox as class which inherits from ``peachbox.model.MasterDataSet`` and consists of:

* A unique name (i.e. the class name: e.g. `ReviewProperties`) 
* A unique `data_unit_index` 
* It holds one or more attributes(=property) 
* The partition key and granularity for horizontal partitioning

Each property/attribute consists of:
* A name, here called `field`: e.g. ``'field':'review_id'``
* A type: ``'type':'StringType'``

An entity could look like:

.. code-block:: python

   class ReviewProperties(peachbox.model.MasterDataSet):                                                                   
       """A particular realization of a node, containing several properties. Here: the review properties """
       data_unit_index = 2                
       partition_key = 'true_as_of_seconds'  
       partition_granularity = 60*60*24*360   
       schema = [{'field':'review_id', 'type':'StringType'},
                 {'field':'helpful', 'type':'IntegerType'},
                 {'field':'nothelpful', 'type':'IntegerType'},
                 {'field':'score', 'type':'IntegerType'},
                 {'field':'summary', 'type':'StringType'}, 
                 {'field':'text', 'type':'StringType'}] 


Relationships (or edges), relating entities (or nodes)
------------------------------------------------------
A relationship (=edge) is represented within peachbox as a class which inherits from ``peachbox.model.MasterDataSet`` and consists of:

* A unique name (i.e. the class name: e.g. `ReviewProperties`)
* A unique `data_unit_index`
* It relates exactly two entities/nodes
* The partitionkey andgranularity forhorizontal partitioning

.. code-block:: python

   class UserReviewEdge(peachbox.model.MasterDataSet):
       """A particular realization of an 'edge'. Here: the user review edge """
       data_unit_index = 0
       partition_key = 'true_as_of_seconds'
       partition_granularity = 60*60*24*360
       schema = [{'field':'user_id', 'type':'StringType'},
                 {'field':'review_id', 'type':'StringType'}]


Peachbox technical implementation
---------------------------------
Each relation (relationship or entity) inherits from the ``MasterDataSet``.
In order to allow horizontal partitioning, every relation has an additional attribute as partition key, similar to something like: ``'field':'true_as_of_seconds':'type':'IntegerType'``.
This involves that the required storage space is increased, the more relations are set up. 

Therefore, there is a tradeof between required disksize and flexibility/speed of the master schema.



Tipps for schema defintions
---------------------------
Following the idea of the fact-based model, the highest granular model offers full extensibility/flexibility and best performance, since any property can be accessed separately and a new property can be related to any existing property.
This would suggest, that each attribute of the stream-schema would be represented as an individual relation to some key-like property.
Since peachbox incorporates horizontal partitioning, which requires an additional unique element as partition key, a large number of these partition keys are stored somewhat redundantly. In addition, for each attribute a full relation has to be stored, which may require disk space due to the redundant relation to the initial key. 

Take as example following stream-schema (we drop the type for simplicity, and imagine that every person has a (unique) name, email address, taxID, amazonID and appleID)::

  Name, emailAddress, TaxID, amazonID, appleID

The highest granular master model would require to choose one attribute as node (actually a key). 
Since all attributes are unique, any may be chosen as a key, for instance `TaxID`, and we add the partition key explicitly::

  true_as_of_seconds, TaxID, Name
  true_as_of_seconds, TaxID, emailAddress
  true_as_of_seconds, TaxID, amazonID
  true_as_of_seconds, TaxID, appleID

or alternatively::

  true_as_of_seconds, Name, emailAddress, TaxID, amazonID, appleID


Apparently, the first implementation allows full extensibility, for instance if one wants to relate in the future buys to the amazonID. 
However, this optional flexibility requires apparently, that `true_as_of_seconds` and `TaxID` is stored repeatedly.
The second implementation requires only half the disk space compared to the former (if all attributes are of the same-sized type), but it does not allow for a flexible extension.


There are further advises available:
https://en.wikipedia.org/wiki/Database_schema#Ideal_requirements_for_schema_integration


Define app
++++++++++


Task
----
The 


Scheduler
---------



