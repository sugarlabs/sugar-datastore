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
    
    time.sleep(1.2)

    print "find", datastore.find(dict(author="Benjamin", title="from"))
    res, count = datastore.find(dict(fulltext="peek"))
    if not res:
        print "unable to index content"
        return 
    item = res[0]
    print "bcsaller", item['uid']

    print "huh?", datastore.find(dict(fulltext="kfdshaksjd"))

    # try the other mimetypes
    datastore.update(uid, dict(title="updated title"), os.path.abspath('tests/test.doc'))
    datastore.update(uid, dict(title="another updated title"), os.path.abspath('tests/test.odt'))
    datastore.get_properties(uid)
    datastore.delete(uid)
    
if __name__ == '__main__':
    #a = Application("client", main)
    #a.plugins.append('ore.main.profile_support.ProfileSupport')
    #a()
    main()
