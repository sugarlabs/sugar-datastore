# Copyright (C) 2007 One Laptop Per Child
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA

import sys
import os
import unittest
import time
import tempfile
import shutil
from datetime import datetime

import dbus

DS_DBUS_SERVICE = "org.laptop.sugar.DataStore"
DS_DBUS_INTERFACE = "org.laptop.sugar.DataStore"
DS_DBUS_PATH = "/org/laptop/sugar/DataStore"

PROPS_WITHOUT_PREVIEW = {'activity_id': '37fa2f4013b17ae7fc6448f10fe5df53ef92de18',
        'title_set_by_user': '0',
        'title': 'Write Activity',
        'timestamp': int(time.time()),
        'activity': 'org.laptop.AbiWordActivity',
        'share-scope': 'private',
        'keep': 0,
        'icon-color': '#00588C,#00EA11',
        'mtime': datetime.now().isoformat(),
        'preview': '',
        'mime_type': ''}

PROPS_WITH_PREVIEW = {'activity_id': 'e8594bea74faa80539d93ef1a10de3c712bb2eac',
        'title_set_by_user': '0',
        'title': 'Write Activity',
        'share-scope': 'private',
        'timestamp': int(time.time()),
        'activity': 'org.laptop.AbiWordActivity',
        'fulltext': 'mec mac',
        'keep': 0,
        'icon-color': '#00588C,#00EA11',
        'mtime': datetime.now().isoformat(), 
        'preview': dbus.ByteArray('\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\xd8\x00\x00\x00\xa2\x08\x02\x00\x00\x00\xac\xfb\x94\x1d\x00\x00\x00\x03sBIT\x08\x08\x08\xdb\xe1O\xe0\x00\x00\x03\x1dIDATx\x9c\xed\xd6\xbfJci\x00\xc6\xe1\xc4\xb5Qb\xb0Qa\xfc\xb3\x0cV\xda\xc8,\xa4\xf0^,\xbc;\xef@\x04kA\xb3(\x16b\x97 3("\xc9\x14\'\x13\xc5\x9c\x9cq\x8bmg\xdcj\x93\x17\xf3<\xedw\x8a\xf7\x83\x1f\xe7\x9c\xfa\xd1\xd1\xd1\xc6\xc6F\r\xa6\xe4\xf1\xf1\xf1\xfe\xfe~~{{\xbb\xd5jM{\x0c\xb3\xeb\xe1\xe1\xa1\xddn\xcf\xbf\xf3D\xaf\xd7\xbb\xbd\xbdm4\x1aooo\xbb\xbb\xbb\x97\x97\x97;;;\xa3\xd1\xa8\xd3\xe9\xb4Z\xad\xaa\xaa...\x96\x96\x96\xca\xb2\xdc\xdf\xdf???_[[[^^\xbe\xb9\xb9\xd9\xdb\xdbk6\x9b\x13\xbb\t\x1f\xc0{!\xd6\xeb\xf5n\xb7\xfb\xf4\xf4\xb4\xb0\xb0p}}\xbd\xbe\xbe~rrR\x14\xc5\xc1\xc1AY\x96\x8dF\xe3\xea\xea\xaa\xaa\xaa\xc5\xc5\xc5V\xab\xd5\xe9tNOOWVV\x0e\x0f\x0f\x87\xc3\xe1\xc4.\xc0\xc70\xf7\xce\xd9`0(\xcb\xf2\xc7`\xf0\xbd\xdf\xffsk\xebgU\xfd\xf5\xe5K\xb3\xd9\xfc\xbb\xdd\xfecn\xee\xf9\xf9\xf9\xf5\xf5\xb5(\x8a\xe1pxww\xd7\xef\xf7\x8b\xa2X]]=;;\x9b\xd8z>\x8cz\xbb\xdd\xfe\xed?\xe2\xb7o\xb5\xaf_\x7f}\xf4\xe9S\xed\xf3\xe7\xffo\x16\xb3\xe3\xbf\xff\x11k\x9b\x9b\xb5\xcd\xcdI\xeda\xa6\xbd\xf7i\x86\x89\x11"\x11\x84H\x04!\x12A\x88D\x10"\x11\x84H\x04!\x12A\x88D\x10"\x11\x84H\x04!\x12A\x88D\x10"\x11\x84H\x04!\x12A\x88D\x10"\x11\x84H\x04!\x12A\x88D\x10"\x11\x84H\x04!\x12A\x88D\x10"\x11\x84H\x04!\x12A\x88D\x10"\x11\x84H\x04!\x12A\x88D\x10"\x11\x84H\x04!\x12A\x88D\x10"\x11\x84H\x04!\x12A\x88D\x10"\x11\x84H\x04!\x12A\x88D\x10"\x11\x84H\x04!\x12A\x88D\x10"\x11\x84H\x04!\x12A\x88D\x10"\x11\x84H\x04!\x12A\x88D\x10"\x11\x84H\x04!\x12A\x88D\x10"\x11\x84H\x04!\x12A\x88D\x10"\x11\x84H\x04!\x12A\x88D\x10"\x11\x84H\x04!\x12A\x88D\x10"\x11\x84H\x04!\x12A\x88D\x10"\x11\x84H\x04!\x12A\x88D\x10"\x11\x84H\x04!\x12A\x88D\x10"\x11\x84H\x04!\x12A\x88D\x10"\x11\x84H\x04!\x12A\x88D\x10"\x11\x84H\x04!2e\xe3\xf1x<\x1e\xcfO{\x06\xb3\xee\xf8\xf8\xb8\xd7\xeby#2e///UU\t\x91\x08B$\x82\x10\x89 D"\x08\x91\x08B$\x82\x10\x89 D"\x08\x91\x08B$\x82\x10\x89 D"\x08\x91\x08B$\x82\x10\x89 D"\x08\x91\x08B$\x82\x10\x89 D"\x08\x91\x08B$\x82\x10\x89 D"\x08\x91\x08B$\x82\x10\x89 D"\x08\x91\x08B$\x82\x10\x89 D"\x08\x91\x08B$\x82\x10\x89 D"\x08\x91\x08B$\x82\x10\x89 D"\x08\x91\x08B$\x82\x10\x89 D"\x08\x91\x08B$\x82\x10\x89 D"\x08\x91\x08B$\x82\x10\x89 D"\x08\x91\x08B$\x82\x10\x89 D"\xccw\xbb\xdd\xd1h4\xed\x19\xcc\xae\x7f\xf3\xfb\x07q8\x9emk8\x97\xda\x00\x00\x00\x00IEND\xaeB`\x82'),
        'mime_type': 'application/vnd.oasis.opendocument.text'}

def prepare_file():
    file_path = os.path.join(os.getcwd(), 'tests/funkyabi.odt')
    f, tmp_path = tempfile.mkstemp()
    os.close(f)
    shutil.copyfile(file_path, tmp_path)
    return tmp_path

bus = dbus.SessionBus()
proxy = bus.get_object(DS_DBUS_SERVICE, DS_DBUS_PATH)
data_store = dbus.Interface(proxy, DS_DBUS_INTERFACE)

uids = []

n = 100

total = 0
print 'Creating %r entries' % n
for i in range(n):
    file_path = prepare_file()
    t = time.time()
    uids.append(data_store.create(PROPS_WITHOUT_PREVIEW, file_path, True))
    total += time.time() - t
print 'Created %r entries in %.2f ms. avg' % (n, float(total * 1000) / n)

total = 0
print 'Updating %r entries' % len(uids)
for uid in uids:
    file_path = prepare_file()
    t = time.time()
    data_store.update(uid, PROPS_WITH_PREVIEW, file_path, True)
    total += time.time() - t
print 'Updated %r entries in %.2f ms. avg' % (n, float(total * 1000) / len(uids))

total = 0
print 'Retrieving %r full entries' % len(uids)
for uid in uids:
    t = time.time()
    props = data_store.get_properties(uid)
    total += time.time() - t
print 'Retrieved %r full entries in %.2f ms. avg' % (n, float(total * 1000) / len(uids))

total = 0
query = {'order_by': ['-mtime'], 'limit': 80}
properties = ['uid', 'title', 'mtime', 'timestamp', 'keep', 'buddies',
              'icon-color', 'mime_type', 'progress', 'activity', 'mountpoint',
              'activity_id']
results, count = data_store.find(query, properties)
print 'Searching %r times on a journal with %d entries' % (n, count)
for i in range(n):
    t = time.time()
    results, count = data_store.find(query, properties)
    total += time.time() - t
print 'Searched %r full entries in %.2f ms. avg' % (n, float(total * 1000) / n)

