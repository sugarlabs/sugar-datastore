Datastore
---------

A simple log like datastore able to connect with multiple
backends. The datastore supports connectionig and disconnecting from
backends on the fly to help the support the limit space/memory
characteristics of the OLPC system and the fact that network services
may become unavailable at times

API
---

For developer information see the doc tests in:
  
 src/olpc/datastore/tests/query.txt


Dependencies 
------------

sqlalchemy -- database connectivity

xapian     -- integrated full text indexing
           svn co svn://svn.xapian.org/xapian/trunk xapian
           currently this requires a checkout

pyxapian   -- Python bindings
           svn co svn://svn.xapian.org/xapian/trunk xapian
           the lemur package must be installed 
           
sqllite    -- metadata database tuned to work in embedded systems 

dbus       -- command and control

Converters 
---------- 
(used to index binaries) 

odt2txt       http://stosberg.net/odt2txt/
pdftotext     from poppler-utils
abiword/write

Future Directions
-----------------

see NOTES.txt

Benjamin Saller
Copyright ObjectRealms, LLC. 2007
