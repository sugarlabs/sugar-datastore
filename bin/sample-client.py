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
    
    #print "all", datastore.all()
    #for u in datastore.all():
    #    if u != uid:
    #        datastore.delete(u)
    print "find", datastore.find(dict(author="Benjamin", title="from"))
    
    print "bcsaller", datastore.find(dict(fulltext="bcsaller"))
    print "huh?", datastore.find(dict(fulltext="kfdshaksjd"))

    datastore.update(uid, dict(title="updated title"), "/etc/passwd")
    datastore.update(uid, dict(title="another updated title"), "/etc/passwd")
    print datastore.get_properties(uid)
    datastore.delete(uid)
    
if __name__ == '__main__':
    a = Application("client", main)
    a.plugins.append('ore.main.profile_support.ProfileSupport')
    a()
