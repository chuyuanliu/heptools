**************************************
Construct :class:`~.root.chain.Friend`
**************************************

Basic Usage
======================
To construct a friend tree from scratch, you can follow the example below.

.. code-block:: python

    import json

    from heptools.root import Chunk, Friend
    from heptools.utils.json import DefaultEncoder

    # initialize a friend tree
    friend = Friend("test_friend")
    # fetch the metadata of the target tree
    target_tree_path="root://host.server//path/to/rootfile.root"
    target_tree_name="Events"
    target = Chunk(source=target_tree_path, name=target_tree_name, fetch=True)
    # construct new branches
    branches = ... # some arrays with the same length as the target tree
    # attach to the target tree
    friend.add(target, branches)
    # dump to root file
    friend.dump() # will write to "root://host.server//path/to/test_friend_{target.uuid}_0_{target.entry_stop}.root" by default
    # save the friend tree to a json file
    with open("friend.json", "w") as f:
        json.dump(friend, f, cls=DefaultEncoder)
    
Then, the data can be retrieved by calling :meth:`~.root.chain.Friend.arrays`.

.. code-block:: python

    # load the friend tree from the json file
    with open("friend.json", "r") as f:
        friend = Friend.from_json(json.load(f))
    # fetch the metadata of the target tree
    target = Chunk(source=path, name=name, fetch=True)
    # retrieve branches
    branches = friend.arrays(target)
    # now you can use the branches as you like

You can also attach an existing tree as a friend.

.. code-block:: python

    # fetch the metadata of the existing tree
    friend_tree_path="root://host.server//path/to/friend.root"
    friend_tree_name="FriendEvents"
    branches = Chunk(path=friend_tree_path, name=friend_tree_name, fetch=True)
    # attach to the friend tree
    friend.add(target, branches)
    # friend.dump() # nothing to dump, so this line can be omitted

With ``coffea<=0.7.22``
========================
The friend tree can be used inside coffea processors. For example,

.. code-block:: python

    import json

    from heptools.root import Chunk, Friend
    from heptools.utils.json import DefaultEncoder

    class FriendTreeMaker(processor.ProcessorABC):
        def process(self, events):
            # initialize a friend tree
            friend = Friend("test_coffea_friend")
            # fetch the metadata of the target chunk
            target = Chunk.from_coffea_events(events)
            # construct new branches
            branches = ... # some arrays with the same length as events
            # attach to the target tree
            friend.add(target, branches)
            # dump to root file
            friend.dump()
            return friend # friend object can be accumulated

    # run the processor
    friend = ... # run the processor in a way you like
    # (optional) merge the friend chunks to improve reading performance
    merged_friend = friend.merge()
    # save the friend tree to a json file
    with open("friend.json", "w") as f:
        json.dump(merged_friend, f, cls=DefaultEncoder)

For a large dataset, you can use dask to merge.

.. code-block:: python

    import dask

    merged_friend, = dask.compute(friend.merge(dask=True))

Then, the data can be retrieved in other processors. For example,

.. code-block:: python

    from heptools.root import Friend
    from heptools.utils.json import DefaultEncoder

    class OtherProcessor(processor.ProcessorABC):
        def __init__(self, friend_path):
            with open(friend_path, "r") as f:
                self.friend = Friend.from_json(json.load(f))

        def process(self, events):
            # fetch the metadata of the target chunk
            target = Chunk.from_coffea_events(events)
            # retrieve branches
            branches = self.friend.arrays(target)

With ``coffea>=2023.12.0``
===========================
# TODO steps: match partition, convert to array, convert back and add up.
# TODO steps: match partition, read from friend