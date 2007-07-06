#!/usr/bin/env python
import dbus
import os

def main():
    bus = dbus.SessionBus()
    datastore = bus.get_object("org.laptop.sugar.DataStore",
                               "/org/laptop/sugar/DataStore")

    props = {'title:text': 'title',
             'title_set_by_user': '1',
             'buddies': '',
             'keep': '0',
             'icon-color': '#40011d,#79079a',
             'activity': 'org.laptop.WebActivity',
             'mime_type': 'text/plain'}
    
    uid = datastore.create(props, os.path.abspath('tests/web_data.json'))
    print "created uid", uid

    result, count = datastore.find(dict(fulltext='title'))
    assert result[0]['uid'] == uid
    for k, v in result[0].items():
        print "\t", k, v
    print open(datastore.get_filename(uid), 'r').read()
    print "OK"
    
    datastore.delete(uid)
    
if __name__ == '__main__':
    #a = Application("client", main)
    #a.plugins.append('ore.main.profile_support.ProfileSupport')
    #a()
    main()
