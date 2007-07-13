from olpc.datastore.xapianindex import IndexManager
import os
from datetime import datetime

import time
import unittest
import gnomevfs

DEFAULT_STORE = '/tmp/_xi_test'


def index_file(iconn, filepath):
    """Index a file."""

    mimetype = gnomevfs.get_mime_type(filepath)
    main, subtype = mimetype.split('/',1)

    stat = os.stat(filepath)
    ctime = datetime.fromtimestamp(stat.st_ctime)
    mtime = datetime.fromtimestamp(stat.st_mtime)
    
    if main in ['image']: filepath = None
    if subtype in ['x-trash', 'x-python-bytecode']: filepath = None



    props = {'mimetype' : mimetype, 'mtime:date' : mtime,
             'ctime:date' : ctime,}

    if filepath:
        fn = os.path.split(filepath)[1]
        props['filename'] = fn 
    
    iconn.index(props, filepath)

    return 1

def index_path(iconn, docpath):
    """Index a path."""
    count = 0
    for dirpath, dirnames, filenames in os.walk(docpath):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            index_file(iconn, filepath)
            count += 1
    return count

class Test(unittest.TestCase):
    def setUp(self):
        if os.path.exists(DEFAULT_STORE):
            os.system("rm -rf %s" % DEFAULT_STORE)

    def tearDown(self):
        if os.path.exists(DEFAULT_STORE):
            os.system("rm -rf %s" % DEFAULT_STORE)

    def test_index(self):
        # import a bunch of documents into the store
        im = IndexManager()
        im.connect(DEFAULT_STORE)

        # test basic index performance
        start = time.time()
        count = index_path(im, os.getcwd())
        end = time.time()
        delta = end - start

        #print "%s in %s %s/sec" % (count, delta, count/delta)

        # wait for indexing to finish
        im.complete_indexing()

        # test basic search performance
        results = list(im.search('peek')[0])

        # this indicates that we found text inside binary content that
        # we expected 
        assert 'test.pdf' in set(r.get_property('filename') for r in results)

        assert im.search('mimetype:application/pdf filename:test.pdf peek')[1] == 1
        
        
def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite

if __name__ == "__main__":
    unittest.main()
                    
