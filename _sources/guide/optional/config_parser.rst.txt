**************
Config Parser
**************

This tool provides a flexible framework to load and merge configurations from different sources for Python projects. A tag style syntax is introduced to control the loading or merging behavior.

Quick Start
================

Write configs
--------------

A config is any file that can be deserialized into a python dictionary. Common formats including ``.yaml`` (``.yml``), ``.json`` and ``.toml`` are supported out of the box. Other formats may require a :ref:`config-custom-deserializer`. 

:ref:`config-tag` syntax extends the existing serialization languages to support complicated control flows or python specific features, e.g. include directive, variable definition, python object initialization, etc. An example is available in `advanced-yaml-config <https://github.com/chuyuanliu/heptools/tree/master/examples/advanced-yaml-config>`_

Load configs
-------------

An instance of :class:`~heptools.config.ConfigParser` is used to load config files and parse the tags. The default setup is sufficient for most use cases, while :ref:`config-customization` is possible through the arguments of :class:`__init__() <heptools.config.ConfigParser>`. Each :meth:`~heptools.config.ConfigParser.__call__`  will create a new context to maintain local variables and return the parsed configs in a single dictionary.

The parsing is performed in two passes:

* Deserialize the files into dictionaries.
* Apply the tags.

The order of the paths provided to :meth:`~heptools.config.ConfigParser.__call__` and the order of keys and items from the first pass are preserved to the final output. The tags are parsed recursively from innermost to outermost.


.. _config-tag:

Tag
================

Syntax
--------------

* A tag is defined as a key-value pair given by ``<tag_key=tag_value>`` or ``<tag_key>`` if the tag value is ``None``. Newlines are not allowed within a tag.
* Arbitrary number of tags can be attached to a key.
* The spaces and newlines between the key and tags are optional.

.. admonition:: example
  :class: guide-config-example, dropdown

  The following are examples of valid tags:

  .. code-block:: yaml

    key: value
    key<tag_key>: value
    key <tag_key=tag_value>: value
    key   <tag_key1=tag_value1><tag_key2>  <tag_key3=tag_value3>  : value
    <tag_key1> <tag_key2=tag_value2>  : value
    ? key
      <tag_key1> <tag_key2=tag_value2>
      <tag_key3=tag_value3>
    : value

.. _config-rule-precedence:

Precedence 
------------

* The tags are parsed from left to right based on the order of appearance. 
* The same tag can be applied multiple times.
* The parsing result is order dependent. See :ref:`config-custom-tag`.
* Some of the built-in tags follow special rules:

  * :ref:`config-tag-code` have the highest precedence and will only be parsed once.
  * The following tags will not trigger any parser.

    * :ref:`config-tag-literal`
    * :ref:`config-tag-discard`
    * :ref:`config-tag-comment`

  * The order of the following tags are ill-defined, as they are not supposed to simply modify the key-value pairs. As a result, they cannot be directly chained with other regular tags, unless through :ref:`config-tag-code`. See :ref:`config-tips-include`.

    * :ref:`config-tag-include`
    * :ref:`config-tag-patch`

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
* ``#fragment`` is a dot-separated path, allowing to access nested dictionaries or lists. Similar to ``TOML``'s `table <https://toml.io/en/v1.0.0#table>`_, double quotes can be used to escape the dot.
* The `percentage-encoding <https://en.wikipedia.org/wiki/Percent-encoding>`_ rule (``%XX``) is supported in the ``path`` to escape special characters.

.. warning::

  The ``#fragment`` is extracted before any parsing.


.. admonition:: example
  :class: guide-config-example, dropdown

  The following URLs are all valid:

  .. code-block:: yaml

    local path: /path/to/file.yml
    XRootD path: root://server.host//path/to/file.yml
    fragment: /path/to/file.yml#key1.key2 <extend>.0."key3.key4"
    query: /path/to/file.yml?key1=value1&key2=value2&key1=value3&key3=[1,2,3]&parent.child=value4

  The ``fragment`` example above is equivalent to the pseudo code:

  .. code-block:: python

    yaml.load(open("/path/to/file.yml"))["key1"]["key2 <extend>"][int("0")]["key3.key4"]

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
* The deserializer is inferred from the longest registered extension that does not match any compression format.
* The deserialized objects will be catched, and can be cleared by :meth:`ConfigParser.io.clear_cache<heptools.config.FileLoader.clear_cache>`.

Special
---------

.. _config-special-nested:

``nested=True`` in :class:`~heptools.config.ConfigParser`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``nested=True`` (default) option enables a behavior similar to ``TOML``'s `table <https://toml.io/en/v1.0.0#table>`_, where the dot-separated keys will be interpreted as accessing a nested dictionary and the parents will not be overriden. Use double quotes or :ref:`config-tag-literal` to escape the keys with dot.

.. admonition:: example
  :class: guide-config-example, dropdown

  .. code-block:: yaml

    parent1:
      child1: value1
    parent1 <comment>: # override the parent
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

.. _config-tag-comment:

``<comment>``
--------------

This tag is reserved to never trigger any parser. This is useful when you want to leave a comment or add keys with duplicate names.

.. admonition:: example
  :class: guide-config-example, dropdown

  .. code-block:: yaml

    key <comment=this is a counter>: 1
    key <extend> <comment=1>: 2
    key <extend> <comment=2>: 3
    key <extend> <comment=3>: 4

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
    * If ``{attribute}`` is not provided or only contains dots, the whole module will be returned.
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
    key2 <type>: json::. # the same as key1
    key3 <type>: json::loads # import a function
    key4 <type>: json::loads.__qualname__ # import a nested attribute
    key5 <type=range>: [0, 100, 10] # positional arguments
    <discard>:
      <type=logging::basicConfig>:
        level <type>: logging::INFO # import an object
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
      "key2": json,
      "key3": json.loads,
      "key4": json.loads.__qualname__,
      "key5": range(0, 100, 10),
    }
    logging.info("message")
    print("message1", "message2", "message3", sep="\n")


``<key-type>``
----------------

.. admonition:: tag
  :class: guide-config-tag

  * ``<key-type>``: similar to :ref:`config-tag-type`, but applied to the key instead.

.. admonition:: example
  :class: guide-config-example, dropdown

  .. code-block:: yaml

    list <key-type> <type=float>: 1
    100 <key-type=float>: 2
    json::loads.__qualname__ <key-type> <literal>: 3 # use literal to escape the dot
  
  will be parsed into

  .. code-block:: python

    import json

    {
      list: 1.0,
      100.0: 2,
      json.loads.__qualname__: 3,
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


``<var>``
----------

This tag can be used to create a variable from the value. The variable has a lifecycle spans the entire parser :meth:`~heptools.config.ConfigParser.__call__` and is shared by all files within the same call. The variable can be accessed using :ref:`config-tag-ref` and is also available as ``locals`` in :ref:`config-tag-code`.

.. admonition:: tag
  :class: guide-config-tag

  * ``<var>``: use the key as variable name.
  * ``<var={variable}>``: use the tag value as variable name.

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
    key2 <ref>: var1 # a reference to var1 in file1.yml, use the value as variable name
    key3 <ref=copy>: var2 # a copy of var2 in file1.yml, use the value as variable name
    var3 <ref=deepcopy>: # a deepcopy of var3 in the same file, use the key as variable name
    var3 <extend>: [value3_4] # append to the deepcopy

  ``"file2.yml"`` will be parsed into:

  .. code-block:: python

    {
      "key1": ["value3_1", "value3_2", "value3_3"],
      "key2": ["value1_1"],
      "key3": ["value2_1", "value2_2"],
      "var3": ["value3_1", "value3_2", "value3_3", "value3_4"],
    }

.. _config-tag-ref:

``<ref>``
---------

This tag can be used to access the variables defined with :ref:`config-tag-var`.

.. admonition:: tag
  :class: guide-config-tag

  * If the value is a string, it will be used as the variable name. Otherwise, the key will be used.
  * ``<ref>``: replace the value by a reference to the variable. 
  * ``<ref=copy>``: replace the value by a :func:`~copy.copy` of the variable.
  * ``<ref=deepcopy>``: replace the value by a :func:`~copy.deepcopy` of the variable.


Support
========

An `VS Code <https://code.visualstudio.com/>`_ extension is provided for syntax highlight. The extension is enabled for the following files:

* ``YAML``: ``*.cfg.yaml``, ``*.cfg.yml``
* ``JSON``: ``*.cfg.json``

To install the extension, download the ``heptools-config-support-X.X.X.vsix`` from one of the `releases <https://github.com/chuyuanliu/heptools/releases>`_.

Syntax Highlight
-----------------

The tokenization is implemented using `TextMate grammars <https://macromates.com/manual/en/language_grammars>`_, which covers most of the tag rules with the following exceptions:

* no flag conflicts check

.. code-block:: yaml

  <file=absolute|relative>: value # this will be highlighted but fail the parsing

* no multiline key validation

.. code-block:: yaml

  ? key
    <tag> # this will be highlighted but not parsed
    key
  : value

.. _config-customization:

Customization
===============

.. _config-custom-tag:

Tag parser
------------

.. _config-custom-extend:

``<extend>`` operation
------------------------
.. _config-custom-deserializer:

File deserializer
------------------------

Tips & Tricks
==============

.. _config-tips-include:

Dynamic ``<include>``
----------------------

Keyword tag values
-------------------

Advanced
========

The following tags are not recommended for general usage and may lead to unexpected results or significantly increase the maintenance complexity.

.. _config-tag-patch:

``<patch>``
-------------
# TODO patch

.. _config-custom-patch:

Customized ``<patch>`` action
------------------------------

