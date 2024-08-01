Arcana Extension - XNAT
=======================
.. image:: https://github.com/arcanaframework/arcana-xnat/actions/workflows/tests.yml/badge.svg
   :target: https://github.com/arcanaframework/arcana-xnat/actions/workflows/tests.yml
.. image:: https://codecov.io/gh/arcanaframework/arcana-xnat/branch/main/graph/badge.svg?token=UIS0OGPST7
   :target: https://codecov.io/gh/arcanaframework/arcana-xnat
.. image:: https://img.shields.io/pypi/pyversions/arcana-xnat.svg
   :target: https://pypi.python.org/pypi/arcana-xnat/
   :alt: Python versions
.. image:: https://img.shields.io/pypi/v/arcana-xnat.svg
   :target: https://pypi.python.org/pypi/arcana-xnat/
   :alt: Latest Version  
.. image:: https://github.com/ArcanaFramework/arcana/actions/workflows/docs.yml/badge.svg
   :target: https://arcanaframework.github.io/arcana
   :alt: Docs


An extension for the Arcana_ framework that adds the following classes to allow integrated
deployment of workflows and analysis classes with XNAT_ imaging data repositories:

* ``Xnat`` data store for accessing data via REST API (only)
* ``XnatViaCS`` data store for accessing data in the XNAT archive directly when running within the XNAT Container Service
* ``XnatApp`` container image class, for building Docker images that can be installed, ready-to-go, in the XNAT Container Service


Quick Installation
------------------

This extension can be installed for Python 3 using *pip*::

    $ pip3 install arcana-xnat

This will also install the core Arcana_ package and any required dependencies.


License
-------

This work is licensed under a
`Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License <http://creativecommons.org/licenses/by-nc-sa/4.0/>`_

.. image:: https://i.creativecommons.org/l/by-nc-sa/4.0/88x31.png
  :target: http://creativecommons.org/licenses/by-nc-sa/4.0/
  :alt: Creative Commons License: Attribution-NonCommercial-ShareAlike 4.0 International

|



.. _Arcana: http://arcana.readthedocs.io
.. _XNAT: https://xnat.org
