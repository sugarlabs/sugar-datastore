#!/usr/bin/env python2.4

import dbus

def main():
    bus = dbus.SessionBus()
    datastore = bus.get_object("org.laptop.sugar.DataStore",
                                   "/org/laptop/sugar/DataStore")

    uid = datastore.create(dict(title="from dbus"), '/etc/passwd')
    print "created uid", uid

    print "all", datastore.all()
    print "bcsaller", datastore.find(dict(fulltext="bcsaller"))
    print "huh?", datastore.find(dict(fulltext="kfdshaksjd"))

    datastore.update(uid, dict(title="updated title"), "/etc/passwd")
    print datastore.get_properties(uid)
    datastore.delete(uid)
    
if __name__ == '__main__':
    main()
