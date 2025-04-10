**************
Config Parser
**************

This tool provides a consistent solution to load multiple configuration files into a single nested dictionary and allows to extend the commonly used dictionary-like formats (``json``, ``yaml``, ``toml``, ``ini``) by adding flags to each key.

Flag
================

Syntax
--------------
- A flag is defined as a key-value pair given by ``<flag_key=flag_value>`` or ``<flag_key>`` if the value is ``None``. 
- Arbitrary number of flags can be added after each ``key``. 
- At least one space is required between the key and the flags.
- The spaces among the flags are optional. 
- If the empty or ``~`` key will be parsed as ``None``.

The following flags are all valid:

.. code-block:: yaml
    
    key: value
    key <flag_key>: value
    key <flag_key=flag_value>: value
    key   <flag_key1=flag_value1><flag_key2>  <flag_key3=flag_value3>  : value
    <flag_key1> <flag_key2=flag_value2>  : value

Parsing
--------------
The flags will be parsed from left to right with the following exceptions:
- The following flags have higher priority: :ref:`config-flag-code` \> :ref:`config-flag-include` \> others from left to right.
- The following flags will not trigger any parser: ``<literal>``, ``<discard>``, ``<dummy>``.

Each parser will take the ``key`` and ``value`` from the previous parser and pass the possibly modified ones to the next, so the order of flags matters.

URL format
--------------
Both the :class:`~heptools.config.ConfigLoader` and built-in flags :ref:`config-flag-include`, ``<file>`` accept the standard URL as path to files, and the IO is completely handled by :func:`fsspec.open`. 

A standard URL is given by the following format:

.. code-block:: yaml

    scheme://netloc/path;parameters?query#fragment

- The ``scheme://netloc/`` can be omitted for local files. 
- The ``parameters`` is always ignored.
- The ``query`` can be used to provide an additional simple configuration.
- The ``fragment`` can be used to access a nested dictionary or an element in a list.
- The `percentage-encoding <https://en.wikipedia.org/wiki/Percent-encoding>`_ ``%XX`` can be used in ``path`` to escape special characters.

The following URLs are valid:

.. code-block:: yaml

    local path: /path/to/file.yml
    XRootD path: root://server.host//path/to/file.yml
    fragment: /path/to/file.yml#key1/key2/0/key3
    query: /path/to/file.yml?key1=value1&key2=value2&key1=value3

The ``fragment`` example above is equivalent to the pseudo code:

.. code-block:: python

    yaml.load(open("/path/to/file.yml"))["key1"]["key2"][int("0")]["key3"]

where the str-to-int conversion will be triggered when encountering a list.

The ``query`` example above will give an additional config dict:

.. code-block:: python

    {
        "key1": ["value1", "value3"],
        "key2": "value2",
    }

where if a key appears multiple times in the query, all values will be collected into a list.
A special key ``json=`` can be used to pass JSON strings but is not recommended. The order of parsing is file, json query, other queries, where the later ones may override the former ones.


.. warning::

    When using with :class:`~heptools.config.ConfigLoader`, the final deserialized object (after all fragments) is required to be a dictionary.


Built-in flags
===============

.. _config-flag-code:

``<code>``
--------------

``<code>`` will replace the value by calling :func:``eval`` on it.

value
^^^^^

- a code string.

example
^^^^^^^

.. code-block:: yaml

    key <code>: '[f"item{i}" for i in range(100)]'

.. _config-flag-include:

``<include>``
--------------

``<include>`` allows to merge dictionaries from other config files into the given level and will be parsed under the current context. For intra file include, ``.`` can be used as path.

flag value
^^^^^^^^^^^

- ``<include>``: the path will be inferred.
- ``<include=absolute>``: force to resolve as the absolute path.
- ``<include=relative>``: force to resolve as the relative path to the config file.

key
^^^^

- the key is required to be empty.
- any flag other than :ref:`config-flag-code` will be ignored.


value
^^^^^^

- a url string
- a list of url strings

example
^^^^^^^

.. code-block:: yaml

    file1.yml
    ----------
    key1:
        key1_1: value1

    file2.yml
    ----------
    key2:
        key2_2: value2

    key3:
        <include>:
            - file1.yml#key1
            - .#key2

Then ``file2.yml#key3`` will give ``{'key1_1': 'value1', 'key2_2': 'value2'}``.




Comparing to ``yaml``
===================
