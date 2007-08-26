import unittest
from testutils import blit, tmpData

from olpc.datastore import hg_backingstore
import os

DEFAULT_STORE = '/tmp/hgtest'
DATA_1 = '/tmp/data1'
DATA_2 = '/tmp/data2'

class Test(unittest.TestCase):
    def setUp(self):
        os.system("rm -rf %s" % DEFAULT_STORE)
        os.system("rm -rf %s" % DATA_1)
        os.system("rm -rf %s" % DATA_2)

        os.makedirs(DATA_1)
        os.makedirs(DATA_2)
        
    def tearDown(self):
        os.system("rm -rf %s" % DEFAULT_STORE)
        os.system("rm -rf %s" % DATA_1)
        os.system("rm -rf %s" % DATA_2)
        
    def test_hgrepo(self):
        repo = hg_backingstore.FileRepo(DEFAULT_STORE)

        # create a test file in DATA_1
        TEXT_1= "this is a test"
        fn1 = blit(TEXT_1, os.path.join(DATA_1, "s1"))
        # and the version we will use later in DATA_2
        # we do this to test tracking from different source dirs
        TEXT_2 = "another test"
        fn2 = blit(TEXT_2, os.path.join(DATA_2, "s2"))

        # this should add foo to the repo with TEXT_1 data
        repo.put("foo", fn1, text="initial")

        # now commit fn2 to the same path, will create another
        # revision
        # of the existing data
        repo.put("foo", fn2, text="updated")

        # now verify access to both revisions and their data
        # we check things out into DATA_1
        co1 = os.path.join(DATA_1, "co1")
        co2 = os.path.join(DATA_1, "co2")

        repo.dump("foo", 0, co1, '', '')
        repo.dump("foo", 1, co2, '', '')

        assert open(co1, 'r').read() == TEXT_1
        assert open(co2, 'r').read() == TEXT_2
        
    def test_hgbs(self):
        bs = hg_backingstore.HgBackingStore("hg:%s" % DEFAULT_STORE)
        bs.initialize_and_load()
        bs.create_descriptor()
        desc = bs.descriptor()
        assert 'id' in desc
        assert 'uri' in desc
        assert 'title' in desc
        assert desc['title'] is not None

        d = """This is a test"""
        d2 = "Different"

        uid, rev = bs.checkin(dict(title="A", filename="a.txt"), tmpData(d))

        bs.complete_indexing()

        props, fn = bs.checkout(uid)

        assert props.get('title') == "A"
        got = open(fn, 'r').read()
        assert got == d

        props['title'] = "B"
        uid, rev = bs.checkin(props, tmpData(d2))

        bs.complete_indexing()
        
        props, fn = bs.checkout(uid)
        assert props.get('title') == "B"
        got = open(fn, 'r').read()
        assert got == d2

        # go back and check out the first version
        props, fn = bs.checkout(uid, 1)
        assert props.get('title') == "A"
        got = open(fn, 'r').read()
        assert got == d
        
        bs.delete(uid, props['vid'])
        bs.complete_indexing()

        # There is no more revision 2
        self.failUnlessRaises(KeyError, bs.get, uid, 1)

##         props, fn = bs.checkout(uid)

##         import pdb;pdb.set_trace()
##         assert props.get('title') == "A"
##         got = open(fn, 'r').read()
##         assert got == d
        
        
def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite

if __name__ == "__main__":
    unittest.main()
                    
