#!/usr/bin/python

import dbus
import dbus.glib
import os
import time

import tempfile

def tmpData(data):
    """Put data into a temporary file returning the filename """
    fd, fn = tempfile.mkstemp()
    os.write(fd, data)
    os.close(fd)
    return fn



DS_DBUS_SERVICE = "org.laptop.sugar.DataStore"
DS_DBUS_INTERFACE = "org.laptop.sugar.DataStore"
DS_DBUS_PATH = "/org/laptop/sugar/DataStore"


# clean up any old tests
assert os.system('rm -rf /tmp/store1') == 0

_bus = dbus.SessionBus()
_data_store = dbus.Interface(_bus.get_object(DS_DBUS_SERVICE, DS_DBUS_PATH), DS_DBUS_INTERFACE)

mount_id = _data_store.mount('inplace:/tmp/store1', dict(title='pen drive'))

props = {'activity_id': dbus.String(u'461c7467f9ef6478b205a687579843fc36a98e7a'),
         'title_set_by_user': '0', 
         'ctime': '2007-07-28T11:57:57.909689',
         'title': 'Google News',
         'mtime': '2007-07-28T11:58:22.460331',
         'keep': '0',
         'icon-color': '#00EA11,#00588C',
         'activity': 'org.laptop.WebActivity',
         'mime_type': 'text/plain'}

jsonData = '''{"history":[{"url":"http://www.google.com/","title":"Google"},
              {"url":"http://news.google.com/nwshp?tab=wn","title":"Google News"}]}'''

uid = _data_store.create(props, '')
file_name = os.path.join('/tmp', str(time.time()))
fn = tmpData(jsonData)

_data_store.update(uid, props, fn)




props = _data_store.get_properties(uid)
file_name = _data_store.get_filename(uid)
props['mountpoint'] = mount_id
# suggest a filename
props['filename'] = "history.json"
uid2 = _data_store.create(props, file_name)

# the file name related to the new copy.
fn2 = _data_store.get_filename(uid2)

assert fn2

contents = open(fn2, 'r').read()
assert contents == jsonData

# Now unmount the store, remount it and read the file

_data_store.unmount(mount_id)


mount_id = _data_store.mount('inplace:/tmp/store1', dict(title='pen drive'))

fn2 = _data_store.get_filename(uid2)

assert fn2

contents = open(fn2, 'r').read()
assert contents == jsonData

print "ALL GOOD"

print "Trying with Abidoc"

props = {'mountpoint' : mount_id,
         'title' : 'Abidoc',

         }

uid = _data_store.create(props, '')
# now update it with the document
fn = os.path.abspath("tests/test.odt")
abidoc = open(fn, 'r').read()


props['filename'] =  'test.odt'
_data_store.update(uid, props, fn)

fn2 = _data_store.get_filename(uid)

contents = open(fn2, 'r').read()
assert contents == abidoc

print "Abiword ok"
