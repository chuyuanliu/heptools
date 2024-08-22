from bokeh.core.properties import Bool, Dict, List, String
from bokeh.models import Widget


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
    paths = Dict(String, String, help="Paths and types of nodes")
    separator = String(help="Path separator")
    expand = Bool(help="Expand all nodes when loaded")
    icons = Dict(String, String, help="Bootstrap icons for each type")

    selected = List(String, help="Currently selected leaves")
