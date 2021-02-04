jsonb format parser
===================

A Python and C implementation to parse the `PostgreSQL jsonb format`__.

.. __: https://github.com/postgres/postgres/blob/master/src/include/utils/jsonb.h


Hacking
-------

Create your favourite virtualenv, or none at all. Then::

    pip install -e .[dev,test]
    pytest


Benchmarking
------------

Just started, with it: run the ``./tools/bench.py`` script. Example ::

    ./tools/bench.py --make-random 500
