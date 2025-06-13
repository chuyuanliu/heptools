**************
Config Parser
**************

This tool provides a flexible framework to load and merge configurations from different sources for Python projects. A tag style syntax is introduced to control the loading or merging behavior.

Quick Start
================

Write configs
--------------

A config is any file that can be deserialized into a python dictionary. Common formats including ``.yaml`` (``.yml``), ``.json`` and ``.toml`` are supported out of the box. Other formats may require a :ref:`config-custom-deserializer`. 

:ref:`config-tag` syntax extends the existing serialization languages to support complicated control flows or python specific features, e.g. include directive, variable definition, python object initialization, etc. An example is available in `advanced-yaml-config <https://github.com/chuyuanliu/heptools/tree/master/examples/advanced-yaml-config>`_.

Load configs
-------------

An instance of :class:`~heptools.config.ConfigParser` is used to load config files and parse the tags. The default setup is sufficient for most use cases, while :ref:`config-customization` is possible through the arguments of :class:`__init__() <heptools.config.ConfigParser>`. Each :meth:`~heptools.config.ConfigParser.__call__`  will create a new context to maintain local variables and return the parsed configs in a single dictionary.

The parsing is performed in two passes:

* Deserialize the files into dictionaries.
* Apply the tags.

The order of the paths provided to :meth:`~heptools.config.ConfigParser.__call__` and the order of keys and items from the first pass are preserved to the final output. The tags are parsed recursively from innermost to outermost.

.. admonition:: example
  :class: guide-config-example, dropdown

  .. code-block:: python

    from heptools.config import ConfigParser

    parser = ConfigParser()
    configs = parser("config1.yml", "config2.yml", ...)


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
* The parsing result is order dependent.
* Some of the built-in tags follow special rules:

  * :ref:`config-tag-code` have the highest precedence and will only be parsed once.
  * The following tags will not trigger any parser.

    * :ref:`config-tag-case`
    * :ref:`config-tag-literal`
    * :ref:`config-tag-discard`
    * :ref:`config-tag-comment`

  * The order of the following tags are ill-defined, as they are not supposed to simply modify the key-value pairs. As a result, they cannot be directly chained with other regular tags, unless through :ref:`config-tag-code`.

    * :ref:`config-tag-select`
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


.. _config-builtin-tags:

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

  * ``<include>``: the type of the paths will be inferred.
  * ``<include=absolute>``: resolve as absolute paths.
  * ``<include=relative>``: resolve as paths relative to the current config file.

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
    key5 <type=range>: # create an instance of a built-in class
      [0, 100, 10] # positional arguments
    <discard>:
      <type=logging::basicConfig>:
        level <type>: logging::INFO # import an object
      <type=logging::info>: message  # call a function with one argument
    <discard><type=print>: # call a built-in function
      ~: # positional arguments
        - message1
        - message2
        - message3
      sep: "\n" # keyword arguments

  will be parsed into

  .. code-block:: python

    import json
    import logging

    logging.info("message")
    print("message1", "message2", "message3", sep="\n")

    return {
      "key1": json,
      "key2": json,
      "key3": json.loads,
      "key4": json.loads.__qualname__,
      "key5": range(0, 100, 10),
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

This tag will try to extend the existing key by the new value in a way given by the pseudo code:

.. code-block:: python
  
  if key in local:
    return extend_method(local[key], value)
  else:
    return value

where the ``extend_method()`` is a binary operation specified by the tag value.

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

This tag can be used to create a variable from the value. The variable lifecycle spans the entire parser :meth:`~heptools.config.ConfigParser.__call__` and is shared by all files within the same call. The variable can be accessed using :ref:`config-tag-ref` and is also available as ``locals`` in :ref:`config-tag-code`.

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

.. _config-tag-map:

``<map>``
----------------

This tag converts a list of key-value pairs into a dictionary, which makes it possible to apply the tags that only work with values to the keys.

.. admonition:: value
  :class: guide-config-value

  * ``list``: a list of dictionaries with keys ``key`` and ``val``.

.. admonition:: example
  :class: guide-config-example, dropdown

  .. code-block:: yaml

    parent <map>:
      - key <type=tuple>: [["child", 1]]
        val: value1
      - key <type=tuple>: [["child", 2]]
        val: value2

  will be parsed into 

  .. code-block:: python

    {
      "parent": {
        ("child", 1): "value1",
        ("child", 2): "value2"
      }
    }

.. _config-tag-select:

``<select>``
-------------

This tag implements a conditional statement to select keys from a list of cases and replace itself by the selected keys. Each case is a dictionary where the keys with :ref:`config-tag-case` (case-keys) will be interpreted as booleans and only contribute to the decision, while others (non-case-keys) will be merged into the current dictionary if the final decision is ``True``.

Unlike other tags, only the necessary branches under ``<select>`` will be parsed. When ``<select=all>``, the non-case-keys that failed the selection will not be parsed. When ``<select=first>``, besides the failed non-case-keys, everything after the first selected case will not be parsed. 

.. admonition:: tag
  :class: guide-config-tag

  * ``<select>``, ``<select=first>``: only keep the first selected case.
  * ``<select=all>``: keep all selected cases.

.. admonition:: value
  :class: guide-config-value

  * ``list``: a list of dictionaries with :ref:`config-tag-case` keys.

.. admonition:: example
  :class: guide-config-example, dropdown

  .. code-block:: yaml

    count <var>: 10
    <select>:
      - <case>: true
        <discard> <type=print>: first case
      - <case>: true
        <discard> <type=print>: second case
    selected: [before]
    <select=all>:
      - <case> <code>: count % 2 == 0
        selected <extend>: [A]
        <discard> <type=print>: count is even
      - <case> <code>: count % 2 == 1
        selected <extend>: [B]
        <discard> <type=print>: count is odd # will not print
      - <case> <code>: count > 5
        selected <extend>: [C]
      - <case> <code>: count > 5
        <case=xor>: true # count <= 5
        selected <extend>: [D]
    selected <extend>: [after]

  will be parsed into

  .. code-block:: python

    print("first case")
    print("count is even")

    return {
      "count": 10,
      "selected": ["before", "A", "C", "after"]
    }


.. _config-tag-case:

``<case>``
-------------

This tag can only be used inside :ref:`config-tag-select` to modify the decision. Each case will start with a ``False`` decision and the keys with ``<case>`` will update the decision based on the value and the operation specified by the tag value.

.. admonition:: tag
  :class: guide-config-tag

  * ``<case>``: ``decision = value``
  * ``<case=or>``: ``decision |= value``
  * ``<case=and>``: ``decision &= value``
  * ``<case=xor>``: ``decision ^= value``

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

A tag parser is a function that returns a key-value pair. The signature is given by the protocol :class:`~heptools.config.TagParser` where the arguments are keyword only and can be omitted if unnecessary.  Custom parsers can be registered through the ``tag_parsers`` argument of :class:`~heptools.config.ConfigParser`. :ref:`config-builtin-tags` cannot be overridden.

.. admonition:: example
  :class: guide-config-example, dropdown

  The following example defines two custom tags: one repeats the value by a given number of times and the other controls how the copy is made.

  .. code-block:: python

    import copy

    def repeat_parser(tags: dict[str], tag: str, key: str, value):
        tag = int(tag or 1)
        if mode := tags.get("repeat.mode"):
            match mode:
                case "copy":
                    method = copy.copy
                case "deepcopy":
                    method = copy.deepcopy
                case _:
                    raise ValueError(f"unknown repeat mode {mode}")
            return key, [value] + [method(value) for _ in range(tag - 1)]
        return key, [value] * tag

    parser = ConfigParser(tag_parsers={"repeat": repeat_parser, "repeat.mode": None})

  Then, the following config

  .. code-block:: yaml

    key1 <var=value1><repeat=3>: []
    key2 <var=value2><repeat.mode=deepcopy><repeat=3>: []
    <discard>:
      <code> <comment=key1>: value1.append(1)
      <code> <comment=key2>: value2.append(1)


  will be parsed into

  .. code-block:: python

    {
      "key1": [[1], [1], [1]],
      "key2": [[1], [], []]
    }

.. _config-custom-extend:

``<extend>`` operation
------------------------

Custom ``extend_method()`` for :ref:`config-tag-extend` can be registered through the ``extend_methods`` argument of :class:`~heptools.config.ConfigParser`. The built-in extend methods cannot be overridden.

.. admonition:: example
  :class: guide-config-example, dropdown

  The following example defines a custom operation to concat paths.

  .. code-block:: python

    from pathlib import PurePosixPath

    def extend_paths(old_value: str, new_value: str):
        return PurePosixPath(old_value) / new_value

    parser = ConfigParser(extend_methods={"path": extend_paths})

  Then, the following config

  .. code-block:: yaml

    key: base
    key <extend=path>: file

  will be parsed into

  .. code-block:: python

    {
      "key": PurePosixPath("base") / "file"
    }

.. _config-custom-deserializer:

File deserializer
------------------------

A deserializer is a function that takes a read-only :class:`~io.BytesIO` stream as input and returns a deserialized object. Custom deserializers can be registered using the decorator :meth:`~heptools.config.FileLoader.register` of :data:`ConfigParser.io <heptools.config.ConfigParser.io>`. 


.. admonition:: example
  :class: guide-config-example, dropdown

  The following example implements a deserializer to load ``CSV`` files.

  .. code-block:: python

    @ConfigParser.io.register("csv")
    def csv_loader(stream: BytesIO):
        headers = stream.readline().decode().strip().split(",")
        lineno = 1
        data = [[] for _ in range(len(headers))]
        while row := stream.readline():
            lineno += 1
            row = row.decode().strip()
            if not row:
                continue
            row = row.split(",")
            if len(row) != len(headers):
                raise ValueError(f"line {lineno}: length mismatch.")
            for i, value in enumerate(row):
                data[i].append(value)
        return dict(zip(headers, data))

  Then, the ``.csv`` files in :ref:`config-tag-include` and :ref:`config-tag-file` can be properly loaded.

Advanced
========

The following tags are not recommended for general usage and may lead to unexpected results or significantly increase the maintenance complexity.

.. _config-tag-patch:

``<patch>``
-------------

Patch layers can be attached on top of config files to modify the raw content before :ref:`config-tag-include`. A patch layer consists of a list of patches, each of which is a dictionary with the following structure:

.. code-block:: yaml

  path: "[scheme://netloc/]path[#fragment]" # the file to patch
  actions: # actions to apply
    - action: name # the name of the action
      ... # other keyword arguments provided to the action

where the ``path`` can be either absolute or relative and the ``action`` is one of the following:

.. list-table::
  :widths: 35, 10, 55
  :header-rows: 1

  * - Action
    - Type
    - Arguments
  * - ``mkdir``: create a nested dict.
    - ``dict``
    - - ``target``: a dot-separated path to a dict.
  * - ``update``: update the target dict by the value.
    - ``dict`` 
    - - ``target``: a dot-separated path to a dict.
      - ``value``: a dict.
  * - ``pop``: remove the target key/item from the dict/list.
    - ``dict`` ``list``
    - - ``target``: a dot-separated path to a key/item.
  * - ``set``: set the target key/item to the value.
    - ``dict`` ``list``
    - - ``target``: a dot-separated path to a key/item.
      - ``value``: any object.
  * - ``insert``: insert the value before the target item.
    - ``list``
    - - ``target``: a dot-separated path to an item.
      - ``value``: any object.
  * - ``append``: append the value to the end of the target list.
    - ``list``
    - - ``target``: a dot-separated path to a list.
      - ``value``: any object.
  * - ``extend``: extend the target list by the value.
    - ``list``
    - - ``target``: a dot-separated path to a list.
      - ``value``: a list.

or a custom one registered through the ``patch_actions`` argument of :class:`~heptools.config.ConfigParser`. The built-in actions cannot be overridden.

This tag can be used to register a new patch layer. The layer will be installed right after the registration and in effect across all the configs within the same parser :meth:`~heptools.config.ConfigParser.__call__`. If a key is provided, it will be used as the patch name. A named patch can be installed or uninstalled multiple times. The patches are evaluated lazily after the deserialization but before the tag parsing, so it is supposed to work as a preprocessor with minimal semantic support other than a regular tag.

.. admonition:: tag
  :class: guide-config-tag

  Register and install a patch layer:

  * ``<patch>``:  The type of the paths will be inferred.
  * ``<patch=absolute>``: Resolve as absolute paths.
  * ``<patch=relative>``: Resolve as relative paths.

  Modify the patch layers:

  * ``<patch=install>``: install patch layers.
  * ``<patch=uninstall>``: uninstall patch layers.

.. admonition:: value
  :class: guide-config-value

  * ``<patch>``, ``<patch=absolute>``, ``<patch=relative>``: a patch or a list of patches.
  * ``<patch=install>``, ``<patch=uninstall>``: a patch name or a list of patch names.

.. admonition:: example
  :class: guide-config-example, dropdown

  .. code-block:: yaml

    --- # file1.yml
    key1 <type=os::path.join>:
      - path
      - to
      - file

    --- # file2.yml
    key2 <type=datetime::datetime>:
      year: 2025
      month: 1
      day: 1

    --- # patched.yml
    patch1 <patch>:
      - path: file1.yml
        actions:
          - action: insert
            target: '"key1 <type=os::path.join>".2'
            value: new
      - path: file2.yml
        actions:
          - action: update
            target: "key2 <type=datetime::datetime>"
            value:
              month: 12
              day: 31

    patched:
      <include>:
        - file1.yml
        - file2.yml
    unpatched:
      <patch=uninstall>: patch1
      <include>:
        - file1.yml
        - file2.yml

  The example above will be parsed into

  .. code-block:: python

    {
      "patched": {
        "key1": os.path.join("path", "to", "new", "file"),
        "key2": datetime.datetime(2025, 12, 31),
      },
      "unpatched": {
        "key1": os.path.join("path", "to", "file"),
        "key2": datetime.datetime(2025, 1, 1),
      },
    }