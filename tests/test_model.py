import unittest
from testutils import tmpData

from olpc.datastore import DataStore
from olpc.datastore import model, backingstore
import datetime
import os

class Test(unittest.TestCase):
    def test_dateproperty(self):
        n = datetime.datetime.now()
        # we have to kill the microseconds as
        # time.strptime which we must use in 2.4 doesn't parse it
        n = n.replace(microsecond=0)
        p = model.DateProperty('ctime', n)
        assert p.key == "ctime"
        assert p.value.isoformat() == n.isoformat()

    def test_binaryproperty(self):
        ds = DataStore()
        ds.registerBackend(backingstore.FileBackingStore)
        ds.mount('/tmp/test_ds')
        
        data = open('test.jpg', 'r').read()
        # binary data with \0's in it can cause dbus errors here
        uid = ds.create({'title' : "Document 1", 'thumbnail:binary' : data},
                        tmpData("with image\0\0 prop"))
        c = ds.get(uid)
        assert c.get_property('thumbnail') == data
        ds.stop()

        os.system('rm -rf /tmp/test_ds')
        
def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite

if __name__ == "__main__":
    unittest.main()
                    
