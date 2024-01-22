'''
An alternative to :mod:`sphinx.ext.linkcode` based on :mod:`autoapi.extension` and :mod:`sphinx.ext.viewcode`.
'''

from __future__ import annotations

import os
from urllib.parse import urljoin

import sphinx
from autoapi.extension import viewcode_follow_imported
from docutils import nodes
from docutils.nodes import Node
from sphinx import addnodes
from sphinx.application import Sphinx
from sphinx.locale import _


def doctree_read(app: Sphinx, doctree: Node) -> None:
    env = app.builder.env
    objs = app.env.autoapi_all_objects

    repourl = getattr(env.config, 'linkrepo_remote_doc_url', None)
    if not repourl:
        raise ValueError('linkrepo_remote_doc_url is not set in conf.py')

    locations: dict[tuple[str, str], tuple[str, int, int]] = {}
    for objnode in list(doctree.findall(addnodes.desc)):
        domain = objnode.get('domain')
        if domain != 'py':
            continue
        urls: set[str] = set()
        for signode in objnode:
            if not isinstance(signode, addnodes.desc_signature):
                continue
            fullname = signode.get('fullname')
            modname = viewcode_follow_imported(
                app, signode.get('module'), fullname)
            if not modname or modname not in objs:
                continue
            importname = (modname, fullname)
            if importname not in locations:
                module = objs[modname]
                for child in module.children:
                    stack = [("", child)]
                    while stack:
                        prefix, obj = stack.pop()
                        objname = prefix + obj.name
                        if "from_line_no" in obj.obj:
                            locations[(modname, objname)] = (
                                module.obj['file_path'],
                                obj.obj["from_line_no"],
                                obj.obj["to_line_no"],
                            )
                        children = getattr(obj, "children", ())
                        stack.extend((objname + ".", gchild)
                                     for gchild in children)
            if importname in locations:
                path, start, end = locations[importname]
                path = os.path.relpath(path, app.srcdir)
                url = f'{urljoin(repourl, path)}#L{start}-L{end}'
                if url not in urls:
                    urls.add(url)
                    inline = nodes.inline(
                        '', _('[source]'), classes=['viewcode-link'])
                    onlynode = addnodes.only(expr='html')
                    onlynode += nodes.reference(
                        '', '', inline, internal=False, refuri=url)
                    signode += onlynode


def setup(app: Sphinx) -> dict[str]:
    app.connect('doctree-read', doctree_read)
    app.add_config_value('linkrepo_remote_doc_url', None, '')
    return {'version': sphinx.__display_version__, 'parallel_read_safe': True}
