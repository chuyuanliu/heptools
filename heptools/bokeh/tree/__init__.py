from typing import TypedDict

from bokeh.core.properties import Bool, Dict, List, String
from bokeh.models import Widget


class IconCollection(TypedDict):
    root: str
    branch: str
    leaf: str


class TreeView(Widget):
    __implementation__ = "treeview.ts"
    __javascript__ = [
        "https://cdnjs.cloudflare.com/ajax/libs/jquery/1.12.1/jquery.min.js",
        "https://cdnjs.cloudflare.com/ajax/libs/jstree/3.2.1/jstree.min.js",
    ]
    __css__ = [
        "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css",
    ]

    root = String(help="Name of the root node")
    paths = List(String, help="List of paths")
    separator = String(help="Separator for paths")
    expand = Bool(help="Expand all nodes")
    icons = Dict(String, String, help='Icons for "root", "branch" and "leaf"')

    selected_leaves = List(String, help="All selected leaves")
