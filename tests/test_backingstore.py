import unittest
from StringIO import StringIO

from olpc.datastore import backingstore
import os

DEFAULT_STORE = '/tmp/_bs_test'

class Test(unittest.TestCase):
    def tearDown(self):
        if os.path.exists(DEFAULT_STORE):
            os.system("rm -rf %s" % DEFAULT_STORE)
            
    def test_fsstore(self):
        bs = backingstore.FileBackingStore(DEFAULT_STORE)
        bs.initialize_and_load()
        bs.create_descriptor()
        desc = bs.descriptor()
        assert 'id' in desc
        assert 'title' in desc
        assert 'uri' in desc


        d = """This is a test"""
        d2 = "Different"
        
        c = bs.create(dict(title="A"), StringIO(d))
        obj = bs.get(c.id)
        assert obj.get_property('title') == "A"
        got = obj.file.read()
        assert got == d

        bs.update(c.id, dict(title="B"), StringIO(d2))
        obj = bs.get(c.id)
        assert obj.get_property('title') == "B"
        got = obj.file.read()
        assert got == d2

        bs.delete(c.id)
        self.failUnlessRaises(KeyError, bs.get, c.id)
        
def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite

if __name__ == "__main__":
    unittest.main()
                    
