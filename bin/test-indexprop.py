#!/usr/bin/env python
import dbus
import os

def main():
    bus = dbus.SessionBus()
    ds = bus.get_object("org.laptop.sugar.DataStore",
                        "/org/laptop/sugar/DataStore")
    datastore = dbus.Interface(ds, dbus_interface='org.laptop.sugar.DataStore')
    
    props = {'title': 'test activity',
             'title_set_by_user': '0',
             'buddies': '',
             'keep': '0',
             'icon-color': '#40011d,#79079a',
             'activity': 'org.laptop.WebActivity',
             'mime_type': ''}

    uid = datastore.create(props, '')
    print "created uid", uid
    datastore.complete_indexing()
    props = {'title': 'test activity title changed',
             'title_set_by_user': '1',
             'buddies': '',
             'keep': '0',
             'icon-color': '#40011d,#79079a',
             'activity': 'org.laptop.WebActivity',
             'mime_type': 'text/plain'}
    
    datastore.update(uid, props, os.path.abspath('tests/web_data.json'))
    print "updated uid", uid
    datastore.complete_indexing()
    
    
    result, count = datastore.find(dict(title='test'))
    print result
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
