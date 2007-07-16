import unittest
from testutils import tmpData, waitforindex

from olpc.datastore import DataStore
from olpc.datastore import model, backingstore
import datetime
import os


DEFAULT_STORE = '/tmp/test_ds'

class Test(unittest.TestCase):
    def setUp(self): os.system('rm -rf %s' % DEFAULT_STORE)
    def tearDown(self): os.system('rm -rf %s' % DEFAULT_STORE)
        
    def test_dateproperty(self):
        n = datetime.datetime.now()
        # we have to kill the microseconds as
        # time.strptime which we must use in 2.4 doesn't parse it
        n = n.replace(microsecond=0)
        p = model.Property('ctime', n.isoformat(), 'date')
        assert p.key == "ctime"
        # XXX: the 'date()' is a work around for a missing secore
        # feature right now
        assert p.value == n.date().isoformat()

        
    def test_binaryproperty(self):
        ds = DataStore()
        ds.registerBackend(backingstore.FileBackingStore)

        #add a custom field to the model 
        dm = model.defaultModel.copy().addField('thumbnail', 'binary')
                                         
        
        ds.mount(DEFAULT_STORE, {'indexmanager.model' : dm})
        n = datetime.datetime.now()

        data = open('test.jpg', 'r').read()
        # binary data with \0's in it can cause dbus errors here
        fn = tmpData("with image\0\0 prop")
        # XXX: We should be able to remove:binary now
        uid = ds.create({'title' : "Document 1", 'thumbnail:binary' :
        data, 'ctime:date' : n.isoformat()}, fn)
        
        waitforindex(ds)

        c = ds.get(uid)
        assert c.get_property('thumbnail') == data
        # I don't care about the microsecond issue now, the typelib
        # patch later can fix that
        assert c.get_property('ctime')[:19] ==  n.isoformat()[:19]
        
        ds.stop()


        
def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite

if __name__ == "__main__":
    unittest.main()
                    
