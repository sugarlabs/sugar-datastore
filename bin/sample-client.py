#!/usr/bin/env python
from ore.main import Application
import dbus
import os

def main():
    bus = dbus.SessionBus()
    datastore = bus.get_object("org.laptop.sugar.DataStore",
                               "/org/laptop/sugar/DataStore")

    uid = datastore.create(dict(title="from dbus", author="Benjamin"), os.path.abspath('tests/test.pdf'))
    print "created uid", uid
    
    datastore.complete_indexing()

    print "find", datastore.find(dict(author="Benjamin", title="from"))
    res, count = datastore.find(dict(fulltext="peek"))
    if not res:
        print "unable to index content"
        #return 
    print "bcsaller", [item['uid'] for item in res]

    print "huh?", datastore.find(dict(fulltext="kfdshaksjd"))

    # try the other mimetypes
    datastore.update(uid, dict(title="updated title", mime_type="application/msword"), os.path.abspath('tests/test.doc'))
    print datastore.find(dict(fulltext="inside"))
    datastore.update(uid, dict(title="another updated title", mime_type="application/vnd.oasis.opendocument.text"), os.path.abspath('tests/test.odt'))
    print datastore.find(dict(fulltext="amazed"))

    datastore.complete_indexing()
    datastore.get_properties(uid)

    print "title in fulltext", datastore.find(dict(title="another"))
  
    datastore.delete(uid)
    datastore.complete_indexing()
    
if __name__ == '__main__':
    #a = Application("client", main)
    #a.plugins.append('ore.main.profile_support.ProfileSupport')
    #a()
    main()
