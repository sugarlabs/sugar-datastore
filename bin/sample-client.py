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

##     # ... or create an Interface wrapper for the remote object
##     iface = dbus.Interface(remote_object, "com.example.SampleInterface")

##     hello_reply_tuple = iface.GetTuple()

##     print hello_reply_tuple

##     hello_reply_dict = iface.GetDict()

##     print hello_reply_dict

##     # D-Bus exceptions are mapped to Python exceptions
##     try:
##         iface.RaiseException()
##     except dbus.DBusException, e:
##         print str(e)

    # introspection is automatically supported
    print remote_object.Introspect(dbus_interface="org.freedesktop.DBus.Introspectable")

    #if sys.argv[1:] == ['--exit-service']:
    #    iface.Exit()

if __name__ == '__main__':
    main()
