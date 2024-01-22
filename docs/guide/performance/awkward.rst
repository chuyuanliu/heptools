****************
:mod:`awkward`
****************

Memory Usage
============
:class:`ak.Array` (or :class:`ak.Record`) works as a view of the original data with a structure given by :attr:`ak.Array.layout`. Let's start with a simple array:

.. code-block:: python

    >>> size = 100_000
    >>> data = ak.zip({"x":np.ones(size, dtype=np.int64), "y":np.zeros(size, dtype=np.int64)})

and consider the following use cases.

Counting Memory for Multiple Arrays
-----------------------------------
Most of the operations will only wrap the :class:`ak.Array` with another layer instead of making a copy (with some exceptions). For example:

.. code-block:: python

    >>> selected = data[np.random.choice(size, 10)]
    >>> data.nbytes + selected.nbytes
    3200080

But the actual memory usage is about ``1.6MB`` instead, as they are sharing the same content.

Garbage Collection
------------------
Each instance will hold a reference to the original data. If only the following is executed:

.. code-block:: python

    >>> del data
    >>> gc.collect()

the memory will not be released until the ``selected`` is also deleted.

Concatenate
----------------------
:func:`ak.concatenate` can very expensive in some cases. For example:
TODO: add example
