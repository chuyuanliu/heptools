**************
Config Parser
**************

This tool provides a consistent solution to load multiple configuration files into a single nested dictionary and extend the commonly used dictionary-like formats (``JSON``, ``YAML``, ``TOML``, ``INI``) by adding flags to keys.

Parser
================
# TODO

Flag
================

Syntax
--------------
- A flag is defined as a key-value pair given by ``<flag_key=flag_value>`` or ``<flag_key>`` if the value is ``None``. 
- Arbitrary number of flags can be added after each ``key``. 
- At least one space is required between the key and the flags.
- The spaces among the flags are optional. 
- The empty or ``~`` key will be parsed as ``None``.

Example of valid key and flags:

.. code-block:: yaml

  key: value
  key <flag_key>: value
  key <flag_key=flag_value>: value
  key   <flag_key1=flag_value1><flag_key2>  <flag_key3=flag_value3>  : value
  <flag_key1> <flag_key2=flag_value2>  : value

Parsing
--------------
The flags will be parsed from left to right with the following exceptions:

- The following flags have higher priority than others: :ref:`config-flag-code`, :ref:`config-flag-include`.
- The following flags will not trigger any parser: :ref:`config-flag-literal`, :ref:`config-flag-discard`, :ref:`config-flag-dummy`.

Each parser will take the ``key`` and ``value`` from the previous parser and pass the possibly modified ones to the next, so the order of flags matters.

URL and IO
------------
Both the :class:`~heptools.config.ConfigLoader` and built-in flags :ref:`config-flag-include`, :ref:`config-flag-file` accept the standard URL as file path.

A standard URL is given by the following format:

.. code-block:: yaml

  scheme://netloc/path;parameters?query#fragment

- The ``scheme://netloc/`` can be omitted for local files. 
- The ``parameters`` is always ignored.
- The ``query`` can be used to provide an additional simple configuration.
- The ``fragment`` can be used to access nested dictionaries or lists with ``/`` as delimiter.
- The `percentage-encoding <https://en.wikipedia.org/wiki/Percent-encoding>`_ ``%XX`` can be used in ``path`` to escape special characters.

Example of valid URLs:

.. code-block:: yaml

  local path: /path/to/file.yml
  XRootD path: root://server.host//path/to/file.yml
  fragment: /path/to/file.yml#key1/key2/0/key3
  query: /path/to/file.yml?key1=value1&key2=value2&key1=value3

The ``fragment`` example above is equivalent to the pseudo code:

.. code-block:: python

  yaml.load(open("/path/to/file.yml"))["key1"]["key2"][int("0")]["key3"]

where the str-to-int conversion will only be triggered for list.

The ``query`` example above will give an additional dictionary:

.. code-block:: python

  {
    "key1": ["value1", "value3"],
    "key2": "value2",
  }

where if a key appears multiple times in the query, all values will be collected into a list.
A special key ``json=`` can be used to pass JSON strings. The order of parsing is file, json query and other queries, where the later ones may override the former ones.


File IO is handled by :func:`fsspec.open`. 

- The following extensions are supported: ``.json``, ``.yaml``, ``.yml``, ``.toml``, ``.ini``, ``.pkl``.
- The compression format is inferred from the extension, see :data:`fsspec.utils.compressions`.


.. warning::

  When using with :class:`~heptools.config.ConfigLoader`, the final deserialized object (after all fragments) is required to be a dictionary.


Built-in flags
===============

.. _config-flag-code:

``<code>``
--------------

``<code>`` will replace the value by the result of :func:`eval`.

value
^^^^^

- ``str`` a python expression

example
^^^^^^^

.. code-block:: yaml

  key <code>: '[f"item{i}" for i in range(100)]'

.. _config-flag-include:

``<include>``
--------------

``<include>`` allows to merge dictionaries from other config files into the given level and will be parsed under the current context. To include within the same file, ``.`` can be used as path.

flag
^^^^^^

- ``<include>``: the type of the path will be inferred.
- ``<include=absolute>``: resolve as an absolute path.
- ``<include=relative>``: resolve as an path relative to the current config file.

key
^^^^

- the key is required to be empty.
- any flag other than :ref:`config-flag-code` will be ignored.


value
^^^^^^

- ``str`` a URL to a dictionary
- ``list`` a list of URLs

example
^^^^^^^

.. code-block:: yaml

  # file1.yml
  key1:
    key1_1: value1

  # file2.yml
  key2:
    key2_2: value2

  key3:
    <include>:
      - file1.yml#key1
      - .#key2

Then ``file2.yml#key3`` will give ``{'key1_1': 'value1', 'key2_2': 'value2'}``.

.. _config-flag-literal:

``<literal>``
--------------
# TODO

.. _config-flag-discard:

``<discard>``
--------------
# TODO

.. _config-flag-dummy:

``<dummy>``
------------
# TODO

.. _config-flag-file:

``<file>``
----------
# TODO

.. _config-flag-type:

``<type>``
----------
# TODO

.. _config-flag-attr:

``<attr>``
----------
# TODO

.. _config-flag-var:

``<var>``
---------
# TODO

.. _config-flag-extend:

``<extend>``
------------
# TODO

Customization
===============
# TODO



Comparing to ``YAML``
===================
# TODO
