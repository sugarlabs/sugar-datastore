#!/usr/bin/env python2.4

import sys
from traceback import print_exc

import dbus

def main():
    bus = dbus.SessionBus()
    remote_object = bus.get_object("org.laptop.sugar.Datastore",
                                   "/org/laptop/sugar/DataStore.Object")

    uid = remote_object.create(dict(title="from dbus"), '/etc/passwd')
    print "created uid", uid

if __name__ == '__main__':
    main()
