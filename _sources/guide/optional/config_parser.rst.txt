**************
Config Parser
**************

This tool provides a consistent solution to load multiple configuration files into a single nested dictionary and extend the commonly used dictionary-like formats (``JSON``, ``YAML``, ``TOML``) by adding tags to keys.

Parser
================
# TODO

Tag
================

Syntax
--------------

* A tag is defined as a key-value pair given by ``<tag_key=tag_value>`` or ``<tag_key>`` if the tag value is ``None``. 
* Arbitrary number of tags can be attached to a key.
* The spaces between key and tags are optional.

.. admonition:: example
  :class: guide-config-example, dropdown

  The following key and tags are valid:

  .. code-block:: yaml

    key: value
    key<tag_key>: value
    key <tag_key=tag_value>: value
    key   <tag_key1=tag_value1><tag_key2>  <tag_key3=tag_value3>  : value
    <tag_key1> <tag_key2=tag_value2>  : value

.. _config-rule-priority:

Priority
---------

* The tags are parsed from left to right based on the order of appearance. 
* The same tag can be applied multiple times.
* The parsing result is order dependent. See :ref:`config-custom-tag`.
* Some of the built-in tags follow special rules:

  * :ref:`config-tag-code` have the highest priority and will only be parsed once.
  * The following tags will not trigger any parser.

    * :ref:`config-tag-literal`
    * :ref:`config-tag-discard`
    * :ref:`config-tag-dummy`

  * The order of the following tags are ill-defined, as they are not supposed to simply modify the key-value pairs. As a result, they cannot directly be chained with other regular tags, unless through :ref:`config-tag-code`. See how to :ref:`config-tip-include` as an example.

    * :ref:`config-tag-include`
    * :ref:`config-advanced-patch`

.. _config-rule-url:

URL and IO
------------

Both the :class:`~heptools.config.ConfigParser` and built-in tags :ref:`config-tag-include`, :ref:`config-tag-file` shares the same IO mechanism.

The file path is described by a standard URL accepted by :func:`~urllib.parse.urlparse` with the format:

.. code-block::

  [scheme://netloc/]path[;parameters][?query][#fragment]

* ``scheme://netloc/`` can be omitted for local path.
* ``;parameters`` is never used.
* ``?query`` can be used to provide additional key-value pairs. If a key appears multiple times, all values will be collected into a list. Values are interpreted as JSON strings.
* ``#fragment`` is a dot-separated path, allowing to access nested dictionaries or lists.
* The `percentage-encoding <https://en.wikipedia.org/wiki/Percent-encoding>`_ rule (``%XX``) is supported in the ``path`` to escape special characters.

.. admonition:: example
  :class: guide-config-example, dropdown

  The following URLs are all valid:

  .. code-block:: yaml

    local path: /path/to/file.yml
    XRootD path: root://server.host//path/to/file.yml
    fragment: /path/to/file.yml#key1.key2.0.key3
    query: /path/to/file.yml?key1=value1&key2=value2&key1=value3&key3=[1,2,3]&parent.child=value4

  The ``fragment`` example above is equivalent to the pseudo code:

  .. code-block:: python

    yaml.load(open("/path/to/file.yml"))["key1"]["key2"][int("0")]["key3"]

  The ``query`` example above will give an additional dictionary 

  .. code-block:: python

    {
      "key1": ["value1", "value3"],
      "key2": "value2",
      "key3": [1, 2, 3],
      "parent": {"child": "value4"},
    }


File IO is handled by :func:`fsspec.open` and the deserialization is handled by :data:`ConfigParser.io <heptools.config.ConfigParser.io>`, an instance of :class:`~heptools.config.FileLoader`.

* The compression format is inferred from the last extension, see :data:`fsspec.utils.compressions`.
* The deserializer is inferred from the last extension that does not match any compression format.
* The deserialized objects will be catched, and can be cleared by :meth:`ConfigParser.io.clear_cache<heptools.config.FileLoader.clear_cache>`.

Special
---------

.. _config-special-nested:

``nested=True`` in :class:`~heptools.config.ConfigParser`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``nested=True`` (default) option enables a behavior similar to ``TOML``'s section name, where the dot-separated keys will be interpreted as accessing a nested dictionary and the parents will not be overriden. Use :ref:`config-tag-literal` to escape the keys with dot.

.. admonition:: example
  :class: guide-config-example, dropdown

  .. code-block:: yaml

    parent1:
      child1: value1
    parent1 <dummy>: # override the parent
      child2: value2
    parent1.child3: value3 # modify the child without overriding the parent
    parent2.child.grandchild: value4 # create a nested dict

  will be parsed into 

  .. code-block:: python

    {
      "parent1": {
        "child2": "value2",
        "child3": "value3",
      },
      "parent2": {"child": {"grandchild": "value4"}},
    }

``None`` key
^^^^^^^^^^^^

Besides the standard rules, both ``~`` and empty string in the key will be parsed into ``None``.

.. admonition:: example
  :class: guide-config-example, dropdown

  .. code-block:: yaml

    # None
    ~: value
    ~ <tag>: value
    "": value
    <tag>: value
    null: value

    # not None
    null <tag>: value

.. _config-special-list:

Apply to ``list`` elements
^^^^^^^^^^^^^^^^^^^^^^^^^^

When the element is a dictionary and the only key is ``None``, the element will be replaced by its value. Use :ref:`config-tag-literal` to retain the original dictionary.

.. admonition:: example
  :class: guide-config-example, dropdown

  .. code-block:: yaml

    - key1: value1 
      <tag>: value2 # regular None key
    - <tag>: value3 # replace the whole element with its value
    - <tag> <literal>: value4 # escape the None key

  will be parsed into

  .. code-block:: python
  
    [
      {"key1": "value1", None: "value2"},
      "value3",
      {None: "value4"},
    ]


Built-in tags
===============

.. _config-tag-code:

``<code>``
--------------

This tag will replace the value by the result of :func:`eval`. The variables defined with :ref:`config-tag-var` are available as ``locals``.

.. admonition:: value
  :class: guide-config-value

  * ``str``: a python expression

.. admonition:: example
  :class: guide-config-example, dropdown 

  .. code-block:: yaml

    key <code>: '[f"item{i}" for i in range(100)]'

.. _config-tag-include:

``<include>``
--------------

This tag allows to merge dictionaries from other config files into the given level and will be parsed under the current context.

.. admonition:: tag
  :class: guide-config-tag

  * ``<include>``: the type of the path will be inferred.
  * ``<include=absolute>``: resolve as an absolute path.
  * ``<include=relative>``: resolve as an path relative to the current config file.

.. admonition:: value
  :class: guide-config-value

  * ``str``: a URL to a dictionary
  * ``list``: a list of URLs
  * To include within the same file, use ``.`` as path.
  * The rules in :ref:`config-rule-url` apply.

.. admonition:: example
  :class: guide-config-example, dropdown

  .. code-block:: yaml

    --- # file1.yml
    key1:
      key1_1: value1

    --- # file2.yml
    key2:
      key2_1: value1
      key2_2: value2
    key3:
      <include>:
        - file1.yml#key1 # include another file using a relative path
        - .#key2 # include within the same file

  Then ``file2.yml#key3`` will give

  .. code-block:: python

    {
      "key1_1": "value1",
      "key2_1": "value1",
      "key2_2": "value2",
    }

.. _config-tag-literal:

``<literal>``
--------------

The keys marked as ``<literal>`` will not trigger the following rules:

*  :ref:`config-special-nested`
*  :ref:`config-special-list`


.. _config-tag-discard:

``<discard>``
--------------

The keys marked as ``<discard>`` will not be added into the current dictionary but will still be parsed. This is useful when only the side effects of the parsing are needed. e.g. define variables, execute code, etc.

.. admonition:: example
  :class: guide-config-example, dropdown

  .. code-block:: yaml

    <discard>:
      var1 <var>: value1
      <type=print>: Hello World
    key1 <ref>: var1

  The example above will print ``Hello World`` and be parsed into ``{'key1': 'value1'}``.

.. _config-tag-dummy:

``<dummy>``
------------

This tag is reserved to never trigger any parser. This is useful when there are duplicate keys.

.. admonition:: example
  :class: guide-config-example, dropdown

  .. code-block:: yaml

    key: 1
    key <extend> <dummy=1>: 2
    key <extend> <dummy=2>: 3
    key <extend> <dummy=3>: 4

  The example above will be parsed into ``{'key': 10}``.


.. _config-tag-file:

``<file>``
----------

This tag allows to insert any deserialized object from a URL. Unlike :ref:`config-tag-include`, this tag will only replace the value by a deep copy of the loaded object, instead of merging it into the current dictionary. If the object is large and only used once, it is recommended to turn off the cache to avoid the deep copy.

.. admonition:: tag
  :class: guide-config-tag

  * ``<file>``: the type of the path will be inferred.
  * ``<file=absolute>``: resolve as an absolute path.
  * ``<file=relative>``: resolve as an path relative to the current config file.
  * ``<file=nocache>``: turn off the cache.
  * ``<file=nobuffer>``: turn off the buffer.
  * Use ``|`` to separate multiple flags: ``<file=relative|nocache|nobuffer>``

.. admonition:: value
  :class: guide-config-value

  * ``str``: a URL to any object
  * The rules in :ref:`config-rule-url` apply.


.. admonition:: example
  :class: guide-config-example, dropdown

  Given a compressed pickle file ``database.pkl.lz4`` created by

  .. code-block:: python

    with lz4.frame.open("database.pkl.lz4", "wb") as f:
      pickle.dump({"column1": [0] * 1000}, f)

  .. code-block:: yaml

    key1 <file>: database.pkl.lz4#column1
    key2 <file=nocache>: database.pkl.lz4#column1

  will be parsed into ``{"key1": [0, ..., 0], "key2": [0, ..., 0]}``, while the cache is disabled when parsing key2.

.. _config-tag-type:

``<type>``
----------

This tag can be used to import a module/attribute, create an instance of a class, or call a function.

.. admonition:: tag
  :class: guide-config-tag

  * An import path is defined as ``{module}::{attribute}``, which is roughly equivalent to the python statement ``from {module} import {attribute}``.

    * ``{module}::`` can be omitted for :doc:`python:library/functions`.
    * If ``{attribute}`` is not provided, the whole module will be returned.
    * ``{attribute}`` can be a dot separated string to get a similar effect as :ref:`config-tag-attr`.

  * ``<type>``: when the tag value is not provided, the value must be a valid import path ande will be replaced by the imported object.
  * ``<type={module::attribute}>``: when the tag value is provided, the imported object will be called with the value as its arguments.

.. admonition:: value
  :class: guide-config-value

  * ``<type>``:

    * ``str``: a valid import path ``{module}::{attribute}``.

  * ``<type={module::attribute}>``:

    * ``module.attribute(*value)``: if the value is a list, it will be used as positional arguments.
    * ``module.attribute(**value)``: If the value is a dict and only contains string keys, the string keys will be used as keyword arguments.
    * ``module.attribute(*value[None], **value[others])``: If the value is a dict and the ``None`` key is a list, the ``None`` key will be used as positional arguments.
    * ``module.attribute(value[None], **value[others])``: If the value is a dict and the ``None`` key is not a list, the ``None`` key will be used as the first argument.
    * ``module.attribute(value)``: If the value is neither a list nor a dict, it will be used as the first argument.

.. admonition:: example
  :class: guide-config-example, dropdown

  .. code-block:: yaml

    key1 <type>: "json::" # import a module
    key2 <type>: json::loads # import a function
    key3 <type>: json::loads.__qualname__ # import a nested attribute
    key4 <type=range>: [0, 100, 10] # positional arguments
    <discard>:
      <type=logging::basicConfig>:
        level <type>: logging::INFO # tags can be nested
      <type=logging::info>: message  # call a function with one argument
    <discard><type=print>: # create an instance of a built-in class
      ~: # positional arguments
        - message1
        - message2
        - message3
      sep: "\n" # keyword arguments

  will be parsed into

  .. code-block:: python

    import json
    import logging

    {
      "key1": json,
      "key2": json.loads,
      "key3": json.loads.__qualname__,
      "key4": range(0, 100, 10),
    }
    logging.info("message")
    print("message1", "message2", "message3", sep="\n")

``<key-type>``, ``<value-type>``
----------------------------------

.. admonition:: tag
  :class: guide-config-tag

  * ``<key-type>``: similar to :ref:`config-tag-type`, but applied to the key instead.
  * ``<value-type>``: an alias to :ref:`config-tag-type`, just for better readability when used together with ``<key-type>``.


.. admonition:: example
  :class: guide-config-example, dropdown

  .. code-block:: yaml

    list <key-type> <type=float>: 1
    dict <key-type> <value-type=str>: 2 # value-type is simply an alias to type
    100 <key-type=float>: 3
    json::loads.__qualname__ <key-type> <literal>: 4 # use literal to escape the dot
  
  will be parsed into

  .. code-block:: python

    import json

    {
      list: 1.0,
      dict: "2",
      100.0: 3,
      json.loads.__qualname__: 4,
    }


.. _config-tag-attr:

``<attr>``
----------

This tag will replace the value by the its attribute. A tag like ``<attr=attr1.attr2>`` is equivalent to the pseudo code ``value.attr1.attr2``.

.. admonition:: tag
  :class: guide-config-tag

  - ``<attr={attribute}>``: where the attribute can be a dot separated string.

.. _config-tag-extend:

``<extend>``
------------

This tag will try to extend the existing value of the same key by the value, in a way given by the pseudo code:

.. code-block:: python
  
  if key in local:
    return extend(local[key], value)
  else:
    return value

where the ``extend`` function is a binary operation specified by the tag value.

.. admonition:: tag
  :class: guide-config-tag

  * ``<extend>``, ``<extend=add>``: recursively merge dictionaries or apply ``+`` to other types.
  * ``<extend=and>``: apply ``&`` operation.
  * ``<extend=or>``: apply ``|`` operation.
  * ``<extend={operation}>``: see :ref:`config-custom-extend`

.. warning::
  
  The built-in extend methods will not modify the original value in-place.


.. admonition:: example
  :class: guide-config-example, dropdown

  .. code-block:: yaml

    parent1 <var=original>:
      child1: [a, b]
      child2: 1
    parent1 <extend>: # recursively merge dictionaries
      child1: [c]
      child2: 2
    parent2 <ref>: original # the original value is unmodified
  
  will be parsed into

  .. code-block:: python

    {
      "parent1": {
        "child1": ["a", "b", "c"],
        "child2": 3
      },
      "parent2": {
        "child1": ["a", "b"],
        "child2": 1
      }
    }

.. _config-tag-var:

``<var>``, ``<ref>``, ``<copy>``, ``<deepcopy>``
--------------------------------------------------

This tag can be used to create a variable from the value. The variable has a lifecycle spans the entire parser :meth:`~heptools.config.ConfigParser.__call__` and is shared by all files within the same call. The variable can be accessed using ``<ref>``, ``<copy>`` or ``<deepcopy>`` and is also available as ``locals`` in :ref:`config-tag-code`.

.. admonition:: tag
  :class: guide-config-tag

  * The first of the following that is a string will be used as the variable name:

    * ``<var>``: tag value, key
    * ``<ref>``, ``<copy>``, ``<deepcopy>``: tag value, value, key

  * ``<var>``, ``<var={variable}>``: define a new variable. 
  * ``<ref>``, ``<ref={variable}>``: replace the value by a reference to the variable. 
  * ``<copy>``, ``<copy={variable}>``: replace the value by a :func:`~copy.copy` of the variable.
  * ``<deepcopy>``, ``<deepcopy={variable}>``: replace the value by a :func:`~copy.deepcopy` of the variable.

.. admonition:: example
  :class: guide-config-example, dropdown

  .. code-block:: yaml

    --- # file1.yml
    var1 <var>: [value1_1] # use the key as variable name
    key1 <var=var2>: [value2_1, value2_2] # use the tag value as variable name

    --- # file2.yml
    <discard>: # only make use of the variables
      <include>: file1.yml
    key1 <var=var3>: [value3_1, value3_2, value3_3]
    key2 <ref=var1>: # a reference to var1 in file1.yml, use the tag value as variable name
    key3 <copy>: var2 # a copy of var2 in file1.yml, use the value as variable name
    var3 <deepcopy>: # a deepcopy of var3 in the same file, use the key as variable name
    var3 <extend>: [value3_4] # append to the deepcopy

  ``"file2.yml"`` will be parsed into:

  .. code-block:: python

    {
      "key1": ["value3_1", "value3_2", "value3_3"],
      "key2": ["value1_1"],
      "key3": ["value2_1", "value2_2"],
      "var3": ["value3_1", "value3_2", "value3_3", "value3_4"],
    }

Customization
===============
.. _config-custom-tag:

Customized tag parser
----------------------

.. _config-custom-extend:

Customized ``<extend>`` operation
---------------------------------


Tips & Tricks
==============

.. _config-tips-include:

Use ``<include>`` with other tags
----------------------------------

Use keyword in tag values
--------------------------

Advanced
========

.. _config-advanced-patch:

``<patch>``, ``<install>``, ``<uninstall>``
----------------------------------------------
