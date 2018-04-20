=====
AintQ
=====


.. image:: https://img.shields.io/pypi/v/aintq.svg
        :target: https://pypi.python.org/pypi/aintq

.. image:: https://img.shields.io/travis/fantix/aintq.svg
        :target: https://travis-ci.org/fantix/aintq

.. image:: https://readthedocs.org/projects/aintq/badge/?version=latest
        :target: https://aintq.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status


.. image:: https://pyup.io/repos/github/fantix/aintq/shield.svg
     :target: https://pyup.io/repos/github/fantix/aintq/
     :alt: Updates



AintQ Is Not Task Queue - a Python asyncio task queue on PostgreSQL.


* Free software: BSD license
* Documentation: https://aintq.readthedocs.io.


Features
--------

AintQ is not quite a traditional task queue like Celery_. It runs no workers,
instead it sends HTTP requests to your own web server to execute the tasks. The
purpose of AintQ is to provide a robust broker to queue and trigger task
execution, while being as compact as possible.

* Use PostgreSQL_ as a broker and your own web server as worker, for less moving parts
* AintQ server simply loads tasks from the broker, sends HTTP requests and stores the response
* AintQ server may run standalone or inside your asyncio web server
* Easy to use client API to create tasks and retrieve results
* Support cron-like scheduled repeating tasks
* Support retrying, priority and dependency
* Focus on consistency, stability and robustness
* Provide real-time statistics API

**AintQ is a very new project and under construction. Even though we internally
use similar code in some production environment, AintQ is a complete rewrite
and may probably bite your nose off before it gets stabilized.**

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
.. _Celery: http://www.celeryproject.org/
.. _PostgreSQL: https://www.postgresql.org/
