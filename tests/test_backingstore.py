import unittest
from testutils import tmpData

from olpc.datastore import backingstore
import os

DEFAULT_STORE = '/tmp/_bs_test'

class Test(unittest.TestCase):
    def setUp(self):
        if os.path.exists(DEFAULT_STORE):
            os.system("rm -rf %s" % DEFAULT_STORE)

    def tearDown(self):
        if os.path.exists(DEFAULT_STORE):
            os.system("rm -rf %s" % DEFAULT_STORE)
            
    def test_fsstore(self):
        bs = backingstore.FileBackingStore(DEFAULT_STORE)
        bs.initialize_and_load()
        bs.create_descriptor()
        desc = bs.descriptor()
        assert 'id' in desc
        assert 'uri' in desc
        assert 'title' in desc
        assert desc['title'] is not None

        d = """This is a test"""
        d2 = "Different"
        
        uid = bs.create(dict(title="A"), tmpData(d))

        bs.complete_indexing()
        
        obj = bs.get(uid)

        assert obj.get_property('title') == "A"
        got = obj.file.read()
        assert got == d

        bs.update(uid, dict(title="B"), tmpData(d2))

        bs.complete_indexing()
        
        obj = bs.get(uid)
        assert obj.get_property('title') == "B"
        got = obj.file.read()
        assert got == d2

        bs.delete(uid)
        bs.complete_indexing()
        self.failUnlessRaises(KeyError, bs.get, uid)
        
def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite

if __name__ == "__main__":
    unittest.main()
                    
