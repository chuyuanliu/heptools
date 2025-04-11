import os
import sys

sys.path.insert(0, os.path.abspath(".."))  # noqa: E402

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "HEP tools"
copyright = "2023-2025, Chuyuan Liu"
author = "Chuyuan Liu"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    # allow autodoc style directives
    "sphinx.ext.autodoc",
    "sphinx.ext.autodoc.typehints",
    # allow numpy or google style docstring
    "sphinx.ext.napoleon",
    # allow math
    "sphinx.ext.mathjax",
    # add links to external packages
    "sphinx.ext.intersphinx",
    # allow todo admonition
    "sphinx.ext.todo",
    # allow toggle button
    "sphinx_togglebutton",
    # add links to source code in remote repo
    "sphinx_linkrepo",
    # (not applied) allow recursively loop over all files and automatically generate API docs
    # allow building without imports (otherwise need to setup autodoc_mock_imports manually)
    "autoapi.extension",
]

templates_path = ["_templates"]
exclude_patterns = []

# external packages
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "numpy": ("https://numpy.org/doc/stable/", None),
    "pandas": ("https://pandas.pydata.org/docs/", None),
    "dask": ("https://docs.dask.org/en/latest/", None),
    "dask_awkward": ("https://dask-awkward.readthedocs.io/en/stable/", None),
    "uproot": ("https://uproot.readthedocs.io/en/stable/", None),
    "awkward": ("https://awkward-array.org/doc/stable/", None),
    "coffea": ("https://coffeateam.github.io/coffea/", None),
    "XRootD": ("https://xrootd.slac.stanford.edu/doc/doxygen/5.6.4/python/", None),
    "fsspec": ("https://filesystem-spec.readthedocs.io/en/latest/", None),
}

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "pydata_sphinx_theme"
html_static_path = ["_static"]
html_theme_options = {
    "show_nav_level": 2,
    "use_edit_page_button": True,
    "navigation_with_keys": False,
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/chuyuanliu/heptools",
            "icon": "fa-brands fa-github",
        }  # link to github repo
    ],
}
# link to doc source code in github
html_context = {
    "github_user": "chuyuanliu",
    "github_repo": "heptools",
    "github_version": "master",
    "doc_path": "docs",
}

# custom css
html_css_files = [
    "css/admonition.css",
]

# -- Options for napoleon -------------------------------------------------
napoleon_use_rtype = False

# -- Options for todo -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/extensions/todo.html
todo_include_todos = True

# -- Options for autodoc -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html
autodoc_typehints = "none"
autodoc_member_order = "bysource"

# -- Options for autoapi -------------------------------------------------
# https://sphinx-autoapi.readthedocs.io/en/latest/reference/config.html
autoapi_dirs = ["../heptools"]
# use autodoc style directives
autoapi_generate_api_docs = False
# import submodules without __init__.py file
autoapi_python_use_implicit_namespaces = True

# -- Options for linkrepo -------------------------------------------------
linkrepo_remote_doc_url = "https://github.com/chuyuanliu/heptools/tree/master/docs/"
