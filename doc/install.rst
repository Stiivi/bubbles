++++++++++++
Installation
++++++++++++

Quick Start
===========

Here are quick installation instructions for the impatient.

.. note::

   Bubbles requires Python 3.3. There are no plans of back-porting the
   framework to Python of lesser version.

Satisfy soft dependencies that cover most of the use cases. For more
information read below.::

    pip install sqlalchemy

Install bubbles::

    pip install bubbles

Try:

.. code-block:: python

    import bubbles

    URL = "https://raw.github.com/Stiivi/cubes/master/examples/hello_world/data.csv"

    p = bubbles.Pipeline()
    p.source(bubbles.data_object("csv_source", URL, infer_fields=True))
    p.aggregate("Category", "Amount (US$, Millions)")
    p.pretty_print()


Requirements
============

The framework currently does not have any hard dependency on other packages.
All dependencies are optional and you need to install the packages only if
certain features are going to be used.

+-------------------------+---------------------------------------------------------+
|Package                  | Feature                                                 |
+=========================+=========================================================+
| sqlalchemy              | Streams from/to SQL databases. Source:                  |
|                         | http://www.sqlalchemy.org                               |
|                         | Recommended version is > 0.7                            |
+=========================+=========================================================+
| openpyxl                | Reads from XLSX files.                                  |
|                         | Source: http://bitbucket.org/ericgazoni/openpyxl        |
|                         | Recommended version is > 2.0                            |
+-------------------------+---------------------------------------------------------+


Customized Installation
=======================

The project sources are stored in the `Github repository`_.

.. _Github repository: https://github.com/Stiivi/bubbles

Download from Github::

    git clone git://github.com/Stiivi/bubbles.git
    
Install::

    cd bubbles
    python setup.py install
