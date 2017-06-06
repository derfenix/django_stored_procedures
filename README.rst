Settings
--------

There just one setting — ``SP_DIR``. It is the name of the directories inside apps, that contains files with
stored procesures, custom indexes and other stuff. By default it is ``/sp/``.

Procedures files
----------------

Files with database stuff can have any extention and contain any number of procedures and statements.

So stored procedure can be called via helper, its defenition must starts with ``CREATE OR REPLACE FUNCTION <name>``
where ``<name>`` is procedure's name. Case is important.


Upload procedures
-----------------

.. code-block:: shell

    $ ./manage.py upload_sp


Usage
-----

    >>> from django_sp import sp_loader
    >>> sp_loader.some_procedure(arg1, arg2, ret='all')
    [{'column1': 'value1', 'column2': 'value2}, ... ]
    >>> sp_loader.other_procedure(arg1, arg2, ret='one')
    {'column1': 'value1', 'column2': 'value2'}
    >>> sp_loader.list()
    ['some_procedure', 'other_procedure', 'else_one_procedure']