.. peachbox documentation master file, created by
   sphinx-quickstart on Fri Apr 17 13:18:43 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to peachbox's documentation!
====================================

Getting Started
===============

Create a new Peachbox app with:

``python -c 'import peachbox.gen; peachbox.gen.app.create_app("MyApp")'``

Define your models in ``setup/models.py``
Define your pipelines in ``pipelines``. See example code in pipeline integration test.



API
===

.. toctree::
   :maxdepth: 3

   peachbox


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

