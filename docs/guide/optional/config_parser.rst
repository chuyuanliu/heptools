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
- Unlimited number of flags can be added after each key. 
- The spaces between key and flags are optional.

Example of valid key and flags:

.. code-block:: yaml

  key: value
  key<flag_key>: value
  key <flag_key=flag_value>: value
  key   <flag_key1=flag_value1><flag_key2>  <flag_key3=flag_value3>  : value
  <flag_key1> <flag_key2=flag_value2>  : value

Parsing
--------------
- The flags are parsed from left to right, with some exceptions.
- Each parser will use the key and value from the previous parser, so the order of flags matters.
- If a flag occurs multiple times, each will be parsed based on the order, with some exceptions.
- Exceptions: :ref:`config-flag-code` and :ref:`config-flag-include` have higher priority than all others and will only be parsed once.
- Exceptions: :ref:`config-flag-discard` and :ref:`config-flag-dummy` will not trigger any parser.

.. _config-url-io:

URL and IO
------------
Both the :class:`~heptools.config.ConfigParser` and built-in flags :ref:`config-flag-include`, :ref:`config-flag-file` shares the same IO mechanism.

The file path is described by a standard URL with the following format:

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

The ``query`` example above will give an additional dictionary ``{"key1": ["value1", "value3"], "key2": "value2"}``, where if a key appears multiple times in the query, all values will be collected into a list.
A special key ``json=`` can be used to pass JSON strings. The order of parsing is file, json query and other queries, where the later ones may override the former ones.


File IO is handled by :func:`fsspec.open` and the deserialization is handled by :class:`~heptools.config.FileLoader`

- The compression format is inferred from the last extension, see :data:`fsspec.utils.compressions`.
- The deserializer is inferred from the first extension.
- The deserialized objects will be catched, and can be cleared by :meth:`~heptools.config.FileLoader.clear_cache`.


.. warning::

  When using with :class:`~heptools.config.ConfigParser`, the final deserialized object (after all fragments) is required to be a dictionary.

Special
---------

``expand`` in :class:`~heptools.config.ConfigParser`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``None`` key
^^^^^^^^^^^^

Besides the standard rules, both ``~`` and empty string in the key will be parsed into ``None``, e.g.

.. code-block:: yaml

  # None
  ~: value
  ~ <flag>: value
  "": value
  <flag>: value
  null: value

  # not None
  null <flag>: value

.. _config-special-list:

Use flag with ``list``
^^^^^^^^^^^^^^^^^^^^^^

In order to apply flags to list elements, when the element is a dictionary and the only key is ``None``, the element will be replaced by its value, e.g.

.. code-block:: yaml

  - key1: value1 
    <flag>: value2
  - <flag>: value3

will be parsed into ``[{"key1": "value1", None: "value2"}, "value3"]``. 

Built-in flags
===============

.. _config-flag-code:

``<code>``
--------------

This flag will replace the value by the result of :func:`eval`. The variables defined with :ref:`config-flag-var` are available as ``locals``.

.. admonition:: value
  :class: guide-config-value

  - ``str``: a python expression

.. admonition:: example
  :class: guide-config-example, dropdown 

  .. code-block:: yaml

    key <code>: '[f"item{i}" for i in range(100)]'

.. _config-flag-include:

``<include>``
--------------

This flag allows to merge dictionaries from other config files into the given level and will be parsed under the current context. To include within the same file, ``.`` can be used as path. See :ref:`config-url-io` for details.

.. admonition:: flag
  :class: guide-config-flag

  - ``<include>``: the type of the path will be inferred.
  - ``<include=absolute>``: resolve as an absolute path.
  - ``<include=relative>``: resolve as an path relative to the current config file.

.. admonition:: key
  :class: guide-config-key

  - the key is required to be empty.
  - any flag other than :ref:`config-flag-code` will be ignored.


.. admonition:: value
  :class: guide-config-value

  - ``str``: a URL to a dictionary
  - ``list``: a list of URLs

.. admonition:: example
  :class: guide-config-example, dropdown

  .. code-block:: yaml

    --- # file1.yml
    key1:
      key1_1: value1

    --- # file2.yml
    key2:
      key2_2: value2

    key3:
      <include>:
        - file1.yml#key1
        - .#key2

  Then ``file2.yml#key3`` will give ``{'key1_1': 'value1', 'key2_2': 'value2'}``.

.. _config-flag-discard:

``<discard>``
--------------

The keys marked as ``<discard>`` will not be added into the current dictionary but will still be parsed. 

.. admonition:: example
  :class: guide-config-example, dropdown

  This is useful when you only want to use the side effects of parsing. e.g. define variables, execute code, etc.

  .. code-block:: yaml

    <discard>:
      var1 <var>: value1
      <type=print>: Hello World
    key1 <ref>: var1

  The example above will print ``Hello World`` and be parsed into ``{'key1': 'value1'}``.

.. _config-flag-dummy:

``<dummy>``
------------

This flag is reserved to never trigger any parser.

.. admonition:: example
  :class: guide-config-example, dropdown

  This is useful when you want to duplicate keys.

  .. code-block:: yaml

    key: 1
    key <extend> <dummy=1>: 2
    key <extend> <dummy=2>: 3
    key <extend> <dummy=3>: 4

  The example above will be parsed into ``{'key': 10}``.


.. _config-flag-file:

``<file>``
----------

This flag allows to insert any deserialized object from a URL. Unlike :ref:`config-flag-include`, this flag will only replace the value by a deep copy of the loaded object, instead of merging it into the current dictionary. See :ref:`config-url-io` for details.

.. admonition:: flag
  :class: guide-config-flag

  - ``<file>``: the type of the path will be inferred.
  - ``<file=absolute>``: resolve as an absolute path.
  - ``<file=relative>``: resolve as an path relative to the current config file.

.. admonition:: value
  :class: guide-config-value

  - ``str``: a URL to any object


.. admonition:: example
  :class: guide-config-example, dropdown

  Given a compressed pickle file ``database.pkl.lz4`` created by

  .. code-block:: python

    with lz4.frame.open("database.pkl.lz4", "wb") as f:
      pickle.dump({"column1": [0] * 1000}, f)

  .. code-block:: yaml

    key1 <file>: database.pkl.lz4#column1

  will be parsed into ``{"key1": [0, 0, ..., 0]}``.

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
