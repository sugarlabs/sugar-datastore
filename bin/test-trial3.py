import dbus
import os
import shutil
import popen2
import tempfile

DEFAULT_STORE = "/tmp/store1"

def _create_temp(content):
    pt, pp = tempfile.mkstemp(suffix=".txt")
    os.write(pt, content)
    os.close(pt)
    return pp


def _create_temp_odt(content):
    # This just isn't working on my system
    # first write the contents to a temp file, then convert it
    pt, pp = tempfile.mkstemp(suffix=".txt")
    os.write(pt, content)
    os.close(pt)
    
    f, temp_path = tempfile.mkstemp(suffix='.odt')
    del f
    # my abiword didn't support fd://0
    cmd = 'abiword --to=odt --to-name=%s %s' % (temp_path, pp)
    print cmd
    child_stdout, child_stdin, child_stderr = popen2.popen3(cmd)
    child_stdin.write(content)
    child_stdin.close()
    child_stdout.close()
    child_stderr.close()
    os.unlink(pp)

    return temp_path

def start():
    if os.path.exists(DEFAULT_STORE):
        os.system("rm -rf %s" % DEFAULT_STORE)

    bus = dbus.SessionBus()
    bobj = bus.get_object("org.laptop.sugar.DataStore",
                           "/org/laptop/sugar/DataStore")

    
    ds = dbus.Interface(bobj, dbus_interface='org.laptop.sugar.DataStore')

    mp = ds.mount("hg:%s" % DEFAULT_STORE,
                  dict(title="Primary Storage"))
    return ds, mp

def stop(ds):
    ds.stop()
    


ds, mp = start()
# Browse starts download
print "Download Simulation"
props = {'title': 'Downloading test.pdf from \nhttp://example.org/test.pdf.',
         'mime_type': 'application/pdf',
         'progress': '0',
         'mountpoint' : mp,
         }
uid, vid = ds.checkin(props, '')
print 'Created download: %s %s' % (uid, vid)
assert uid
assert vid == '1'
ds.complete_indexing()

# Browse notifies the DS about the progress
props['uid'] = uid
for i in range(1, 5):
    props['progress'] = str(i * 20)
    props['vid'] = vid
    uid, vid = ds.checkin(props, '')
    print 'Updated download: %s %s %s' % (uid, vid, props['progress'])
    assert uid
    ds.complete_indexing()

# Browse finishes the download
# Now assume we have a file called tests/test.pdf (which there is if
# this is run from the project root)
# Checkin Gives the file to the datastore, it no longer owns it and
# will be removed when the datastore is done with it
source_path = "tests/test.pdf"
dest_path = "/tmp/test.pdf"
# we don't want it to kill our test file so we copy it to /tmp
shutil.copy(source_path, dest_path)

props['title'] = 'File test.pdf downloaded from\nhttp://example.org/test.pdf.'
props['progress'] = '100'
uid, vid = ds.checkin(props, dest_path)
ds.complete_indexing()
print 'Updated download with file: %s %s %s' % (uid, vid, props['progress'])
assert uid
#assert vid == '1'


# Check the DS has removed the file.
assert not os.path.exists(dest_path)

# Journal displays the download
objects, count = ds.find({'title': 'downloaded', 'order_by' : ['-vid']})
props = objects[0]
print 'Query returned: %s' % props['uid'], count
assert props['vid'] == vid, """%s != %s""" % (props['vid'], vid) # the last rev

# Read resumes the entry
props, file_path = ds.checkout(uid, '', '', '', '')
print 'Entry checked out: %s %s\n%s' % (uid, file_path, props)
assert props
assert props['vid'] == vid
assert file_path

# Read saves position and zoom level
props['position'] = '15'
props['zoom_level'] = '150'
props['activity'] = 'org.laptop.sugar.Read'


uid, nvid = ds.checkin(props, file_path)
print 'Updated Read state: %s %s' % (uid, nvid)
assert uid
assert nvid != "1"
assert nvid > vid, "%s < %s" %(nvid, vid)
ds.complete_indexing()


print "DONE"
#stop(ds)

# test_writing
#ds, mp = start()
print "Writing test"
# Create empty entry
props = {'title': 'Write activity', 'mountpoint' : mp}
uid, vid = ds.checkin(props, '')
print 'Created entry: %s %s' % (uid, vid)
assert uid
assert vid == '1'

ds.complete_indexing()

# First checkout
props, file_path = ds.checkout(uid, '', '', '', '')
print 'Entry checked out: %s %s' % (uid, file_path)
assert props
assert props['vid'] == '1'
assert file_path == ''


# Write first contents
file_path = _create_temp('blah blah 1')

props['mountpoint'] = mp
props['mime_type'] = 'text/plain'
uid, vid = ds.checkin(props, file_path)
ds.complete_indexing()
print 'First checkin: %s %s %s' % (uid, vid, file_path)
assert uid
#assert vid == '1'
assert not os.path.exists(file_path)
        
# Second checkout
props, file_path = ds.checkout(uid, '', '', '', '')
print 'Entry checked out: %s %s' % (props['uid'], file_path)
assert props
#assert props['vid'] == '1'
assert file_path

# Write second contents
file_path = _create_temp('blah blah 1\nbleh bleh 2')
props['mime_type'] = 'text/plain'
props['mountpoint'] = mp
uid, vid = ds.checkin(props, file_path)
ds.complete_indexing()
print 'Second checkin: %s %s %s' % (uid, vid, file_path)
assert uid
#assert vid == '2'
assert not os.path.exists(file_path)

print "DONE"
#stop(ds)


# PREVIEW test
#ds, mp = start()
print "preview test"
props = {'title': 'Write activity',
         'preview': dbus.ByteArray('\123\456\789\000\123'),
         'mountpoint' : mp}
uid, vid = ds.checkin(props, '')
print 'Created entry: %s %s' % (uid, vid)
assert uid
assert vid == '1'

props, file_path = ds.checkout(uid, '', '', '', '')
print props

print "DONE"
#stop(ds)
