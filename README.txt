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

xapian     -- integrated full text indexing
           svn co svn://svn.xapian.org/xapian/trunk xapian
           currently this requires a checkout

secore     -- pythonic xapian binding -- include in disto but from 
		http://flaxcode.googlecode.com/svn/trunk/libs/secore

dbus       -- command and control

ore.main   -- (optional) A command line application framework/shell
           used in bin/datasore. If you don't want to use this dep 
           for now run bin/datasore-native
           


Converters 
---------- 
(used to index binaries) 

odt2txt       http://stosberg.net/odt2txt/
pdftotext     from poppler-utils
abiword/write


Benjamin Saller
Copyright ObjectRealms, LLC. 2007
