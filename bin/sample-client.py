#!/usr/bin/env python
from ore.main import Application
import dbus
import os
import time

def main():
    bus = dbus.SessionBus()
    datastore = bus.get_object("org.laptop.sugar.DataStore",
                               "/org/laptop/sugar/DataStore")

    uid = datastore.create(dict(title="from dbus", author="Benjamin"), os.path.abspath('tests/test.pdf'))
    print "created uid", uid
    
    
    #for u in datastore.find()[0]:
    #        datastore.delete(u['uid'])
    #return
    # let the async indexer run
    time.sleep(1.2)
    #import pdb;pdb.set_trace()
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
    datastore.get_properties(uid)

    print "title in fulltext", datastore.find(dict(fulltext="another"))
  
    datastore.delete(uid)
    
if __name__ == '__main__':
    #a = Application("client", main)
    #a.plugins.append('ore.main.profile_support.ProfileSupport')
    #a()
    main()
