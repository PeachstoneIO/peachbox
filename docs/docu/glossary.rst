.. peachbox documentation master file, created by
   sphinx-quickstart on Fri Apr 17 13:18:43 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.


Peachbox glossary
*****************
A glossary for big data science, distribution, processing and machine learning.
Share your knowledge and add useful references and links.

.. toctree::
   :maxdepth: 3

..   glossary


Programs, tools and apps
========================

Cassandra
+++++++++

Druid
+++++
https://en.wikipedia.org/wiki/Druid_%28open-source_data_store%29


ETL: Extract, transform, load
+++++++++++++++++++++++++++++
In computing, Extract, Transform and Load (ETL) refers to a process in database usage and especially in data warehousing that:

*  Extracts data from homogeneous or heterogeneous data sources
*  Transforms the data for storing it in proper format or structure for querying and analysis purpose
*  Loads it into the final target (database, more specifically, operational data store, data mart, or data warehouse)

See:
https://en.wikipedia.org/wiki/Extract,_transform,_load


git and github
++++++++++++++

json: JavaScript Object Notation
++++++++++++++++++++++++++++++++
JSON or JavaScript Object Notation, is an open standard format that uses human-readable text to transmit data objects consisting of attribute–value pairs. 
It is used primarily to transmit data between a server and web application, as an alternative to XML.
The JSON filename extension is .json.

A .json example looks like::

  {
    "user_id":"A37I5QIHD9UMPD",
    "product_id":"6302967538",
    "review":"The movie was awesome!",
    "summary":"Star wars: The clone wars",
    "profile_name":"dmunns@yancey.main.nc.us",
    "helpfulness":"2\/2",
    "time":872035200,
    "score":5.0
  }


Apache Parquet
++++++++++++++
Apache Parquet is a `columnar storage format` available to any project in the Hadoop ecosystem, regardless of the choice of data processing framework, data model or programming language.

https://parquet.apache.org/

Other file formats are, for instance, TEXTFILE, SEQUENCEFILE, ORC, RCFILE.

Apache Hive
+++++++++++
Apache Hive is a data warehouse infrastructure built on top of Hadoop for providing data summarization, query, and analysis.


Kafka
+++++

Nose
++++
A discovery-based unittest extension for python.

http://pythontesting.net/framework/nose/nose-introduction/

Execute ``$ nosetests`` in ``$PEACHBOX`` directory, or::

  $ nosetest --nologcapture --nocapture

NumPy
+++++

SciPy
+++++


Spark (apache)
++++++++++++++





Abstract concepts
=================

Lambda architecture
+++++++++++++++++++
Lambda architecture is a data-processing architecture designed to handle massive quantities of data.
It consists of three layers: batch processing, speed (or real-time processing), and a serving layer for responding to queries, where all layers ingest from an immutable master data set.
This approach to architecture attempts to balance latency, throughput, and fault-tolerance by using batch processing to provide comprehensive and accurate views of batch data, while simultaneously using real-time stream processing to provide views of online data. 
The two view outputs may be joined before presentation.

Lambda architecture depends on a data model with an append-only, immutable data source that serves as a system of record. 
It is intended for ingesting and processing timestamped events that are appended to existing events rather than overwriting them. State is determined from the natural time-based ordering of the data.

See also: https://en.wikipedia.org/wiki/Lambda_architecture


Database model
++++++++++++++

A database model consists of its
* database scheme, which is typically described in a formal language supported by the database management system (DBMS)
* 


Database schema
+++++++++++++++
Schema is the structure of the database that defines the objects in the database.

In a relational database, the schema defines the tables, fields, relationships, views, indexes, packages, procedures, functions, queues, triggers, types, sequences, materialized views, synonyms, database links, directories, XML schemas, and other elements.

Schemas are generally stored in a data dictionary.


Entity-Relationship-Model
+++++++++++++++++++++++++
The entity–relationship model (ER model) is a data model for describing data in an abstract way to implement/store data into a database.
The main components of ER models are `entities` (things) and `relationships` that can exist among them. 

* A `relationship` captures how entities are related to another.
* Entites and relationships can both have `attributes`.

https://en.wikipedia.org/wiki/Entity–relationship_model

The nomenclature of the `graph schema` is differnetly from the ER model



Graph schema
++++++++++++
The graph schema is an alternative, but very similar, schema to the entity relationship model:

* attribute = property
* entity = node
* relationship = edge




Data dictionary
+++++++++++++++



peachbox data scheme features
+++++++++++++++++++++++++++++
The goal of peachbox data models are:
* fault tolerant
* extensible


Partition key and partition glanularity
+++++++++++++++++++++++++++++++++++++++
In file-based data storage systems, it is impractical to store all data in one file, as well as storing each entity in a single file. 
In order to define a horizontal partitioning a glanularity is useful. The partition key, defines in a relational data model the key column, and the glanularity the modulo of that key, whenever a new file is generated.

The file-base data storage 


Horizontal and vertical partitioning
++++++++++++++++++++++++++++++++++++

Horizontal partitioning involves putting different rows into different tables.
Vertical partitioning involves storing tables with fewer tabels and additional relations.


Resilient Distributed Dataset (RDD)
+++++++++++++++++++++++++++++++++++
An RDD is a collection of data elements partitioned that can be operated on in parallel.






Machine learning
================

(Boosted) decision tree
+++++++++++++++++++++++

Neural net
++++++++++

NLP: Natural language processing
++++++++++++++++++++++++++++++++

(google's) Word2Vec
+++++++++++++++++++




Other things
============

kaggle
++++++
The 'home' of (big) data challenges and a data scientist community: https://www.kaggle.com/


reStructuredText
++++++++++++++++
reStructuredText is an easy-to-read, what-you-see-is-what-you-get plaintext markup syntax and parser system and used for this sphinx documentation.
http://docutils.sourceforge.net/rst.html
