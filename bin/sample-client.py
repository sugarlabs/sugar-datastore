#!/usr/bin/env python2.4

import dbus

def main():
    bus = dbus.SessionBus()
    remote_object = bus.get_object("org.laptop.sugar.DataStore",
                                   "/org/laptop/sugar/DataStore")

    uid = remote_object.create(dict(title="from dbus"), '/etc/passwd')
    print "created uid", uid

    print "all", remote_object.all()
    print "bcsaller", remote_object.find(dict(fulltext="bcsaller"))
    print "huh?", remote_object.find(dict(fulltext="kfdshaksjd"))
    
if __name__ == '__main__':
    main()
