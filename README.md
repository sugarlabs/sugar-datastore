Sugar Datastore
===============

Sugar Datastore provides activities with a way to store data and
metadata, and the journal with querying and full text search.

https://www.sugarlabs.org/

https://wiki.sugarlabs.org/

Installing on Debian or Ubuntu
------------------------------

Automatically done when you install [Sugar
desktop](https://github.com/sugarlabs/sugar).

Installing on Fedora
--------------------

Automatically done when you install [Sugar
desktop](https://github.com/sugarlabs/sugar).

Building
--------

Sugar Artwork follows the [GNU Coding
Standards](https://www.gnu.org/prep/standards/).

Install all dependencies; Python GI API bindings for GLib, Python
bindings for Xapian, Python bindings for D-Bus, and Sugar Toolkit.

Clone the repository, run `autogen.sh`, then `make` and `make
install`.

Storage format history
----------------------

```
0   0.82.x
    Initial format

1   0.84.x
    Refactoring, start using indexes

2   0.86.0, 0.86.1
    Add sorting by title and mtime

3   not-mainstream
    test versioning support

4   0.86.2, 0.88.x
    version bump to force index rebuild that may have been missed during the
    migration to version 2 (SL#1787)

5   not-mainstream
    pre v6 testing

6   0.90
    new metadata fields:
    - creation_time, time of ds entry creation in seconds since the epoch
    - filesize, size of ds entry data file in bytes
```
