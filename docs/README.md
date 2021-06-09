Ruby MongoDB Driver Documentation
=================================

This subdirectory contains the high-level driver documentation, including
tutorials and the reference.

Building the documentation for publishing is done via the
[docs-ruby repo](https://github.com/mongodb/docs-ruby).

To build the documentation locally for review, install `sphinx` and
`sphinx-book-theme`, then execute `make html` in this directory:

    pip install sphinx sphinx-book-theme
    make html

Note that the documentation generated in this manner wouldn't have the
BSON documentation included, nor are intersphinx links currently handled.
