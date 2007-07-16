import unittest
from testutils import tmpData

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
        assert p.value == n.isoformat()
        p.value = p.value
        assert p.value == n.isoformat()
        
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
        # The key types are looked up in the model now
        uid = ds.create({'title' : "Document 1", 'thumbnail' : data, 'ctime' : n.isoformat()}, fn)
        
        ds.complete_indexing()

        c = ds.get(uid)
        assert c.get_property('thumbnail') == data
        # I don't care about the microsecond issue now, the typelib
        # patch later can fix that
        assert c.get_property('ctime')[:19] ==  n.isoformat()[:19]
        
        ds.stop()


    def test_intproperty(self):
        p = model.Property('keep', 1, 'int')
        assert p.value == '1'
        
        p.value = 0
        assert p.value == '0'

        p.value = '1'
        assert p.value == '1'
        
        p.value = '0'
        assert p.value == '0'
        
        
        ds = DataStore()
        ds.registerBackend(backingstore.FileBackingStore)
        
        ds.mount(DEFAULT_STORE)

        uid = ds.create({'title' : "Document 1", 'keep' : 1},)
        ds.complete_indexing()
        c = ds.get(uid)
        assert c.get_property('keep') == 1

        ds.update(uid, {'title' : "Document 1", 'keep' : 0})
        ds.complete_indexing()
        c = ds.get(uid)
        assert c.get_property('keep') == 0
        

        ds.update(uid, {'title' : "Document 1", 'keep' : '1'})
        ds.complete_indexing()
        c = ds.get(uid)
        assert c.get_property('keep') == 1

        ds.update(uid, {'title' : "Document 1", 'keep' : '0'})
        ds.complete_indexing()
        c = ds.get(uid)
        assert c.get_property('keep') == 0

def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite

if __name__ == "__main__":
    unittest.main()
                    
