

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'HEP tools'
copyright = '2024, Chuyuan Liu'
author = 'Chuyuan Liu'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ['sphinx.ext.autodoc',
              'sphinx.ext.coverage',
              'sphinx.ext.napoleon',
              'sphinx.ext.intersphinx',
              'autoapi.extension']

templates_path = ['_templates']
exclude_patterns = []

intersphinx_mapping = {
    'uproot': ('https://uproot.readthedocs.io/en/latest/', None),
    'awkward': ('https://awkward-array.org/doc/main/', None),
}

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'pydata_sphinx_theme'
html_static_path = ['_static']
html_theme_options = {
    "show_nav_level": 2,
    "use_edit_page_button": True,
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/chuyuanliu/heptools",
            "icon": "fa-brands fa-github",
        }
    ]
}
html_context = {
    "github_user": "chuyuanliu",
    "github_repo": "heptools",
    "github_version": "master",
    "doc_path": "docs",
}
html_sidebars = {
    "**": ["sidebar-nav-bs", "sidebar-ethical-ads"]
}

# -- Options for AutoAPI -------------------------------------------------
# https://sphinx-autoapi.readthedocs.io/en/latest/reference/config.html
autoapi_dirs = ['../heptools']
autoapi_generate_api_docs = False
autoapi_python_use_implicit_namespaces = True
