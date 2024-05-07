# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
# sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(0, os.path.abspath('..'))

try:
  import battetl
except ImportError:
  print("oh no 1")

sys.path.insert(0, os.path.abspath(os.path.join("battetl")))

try:
  import battetl
except ImportError:
  print("oh no 2")

import battetl  # does not fail

sys.path.insert(0, os.path.abspath(os.path.join("..", "..")))

import battetl  # does not fail anymore

# Other paths 
root_doc = 'index'

# -- Project information -----------------------------------------------------

project = 'BattETL'
copyright = '2024, Zander Nevitt, Bing Syuan Wang, Eric Ravet, and Chintan Pathak'
author = 'Zander Nevitt, Bing Syuan Wang, Eric Ravet, and Chintan Pathak'

# The full version, including alpha/beta/rc tags
release = '1.1.14'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx_markdown_tables',
    "myst_parser", 
    "sphinx_wagtail_theme",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'sphinx_wagtail_theme'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

# These paths are either relative to html_static_path
# or fully qualified paths (eg. https://...).
html_css_files = ["custom.css"]

# MyST Parser Configuration
myst_heading_anchors = 4

# These are options specifically for the Wagtail Theme.
html_theme_options = dict(
    project_name = "BattETL",
    logo = "images/BattETL_Logo.png ",
    logo_alt = "BattETL",
    # logo_height = 59,
    logo_url = "https://github.com/BattGenie/BattETL",
    logo_width = 120,
    github_url = "https://github.com/BattGenie/BattETL/docs/", 
    # header_links = "Top 1|http://example.com/one, Top 2|http://example.com/two",
    footer_links = ",".join([
        "About Us|https://battgenie.life",
        "Contact|https://battgenie.life/contact"
    ])

)