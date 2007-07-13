#!/usr/bin/env python
import dbus
import os

def main():
    bus = dbus.SessionBus()
    datastore = bus.get_object("org.laptop.sugar.DataStore",
                               "/org/laptop/sugar/DataStore")

    uid = datastore.create(dict(title="from dbus", author="Benjamin"), os.path.abspath('tests/test.pdf'))
    print "created uid", uid, "with binary content"
    
    datastore.complete_indexing()

    res, count = datastore.find(dict(fulltext="peek"))
    assert count == 1, "failed to index content"
    assert res[0]['uid'] == uid, "returned incorrect results"
    print "found inside binary file :: PDF"
    
    assert datastore.find(dict(fulltext="kfdshaksjd"))[1] == 0
    print "successfully ignored bad searches"
    
    # try the other mimetypes
    datastore.update(uid, dict(title="updated title",
                               mime_type="application/msword"),
                     os.path.abspath('tests/test.doc'))

    datastore.complete_indexing()

    assert datastore.find(dict(fulltext="inside"))[0][0]['uid'] == uid
    print "found in binary file :: WORD"
    
    datastore.update(uid, dict(title="another updated title",
                               mime_type="application/vnd.oasis.opendocument.text"),
                     os.path.abspath('tests/test.odt'))
    datastore.complete_indexing()
    
    assert datastore.find(dict(fulltext="amazed"))[0][0]['uid'] == uid
    print "found in binary file :: ODT"

    datastore.get_properties(uid)

    assert datastore.find(dict(title="another"))[0][0]['uid'] == uid
    print "found title using dict params", 

    assert datastore.find("another")[0][0]['uid'] == uid
    print "found title in search of all fields (as text)"


    assert datastore.find('title:"another"')[0][0]['uid'] == uid
    print "field in query  field:'value' "

    datastore.delete(uid)
    datastore.complete_indexing()

    print "deleted", uid
    try: datastore.get_properties(uid)
    except: pass
    else:
        print "Found deleted value... oops"
        raise  KeyError(uid)
    
    print "ALL GOOD"
    
if __name__ == '__main__':
    #from ore.main import Application
    #a = Application("client", main)
    #a.plugins.append('ore.main.profile_support.ProfileSupport')
    #a()
    main()
