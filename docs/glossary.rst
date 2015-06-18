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

git and github
++++++++++++++


Kafka
+++++

NumPy
+++++

SciPy
+++++


Spark
+++++





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
